"""
St. Luke's CKD Identification & Risk Flagging - clinician worklist app.

Surfaces the full solution: for each flagged patient it shows lab trend, the lab-derived KDIGO
stage, the ML-suggested stage + confidence, the documented stage, nephrology evidence (structured +
AI note-derived), and key meds. A clinician reviews and approves the suggested stage, which is
queued for write-back to Epic (human-in-the-loop) - the integration pattern leadership asked about.

Auth: uses the app's service principal via databricks-sdk StatementExecution (no manual tokens).
"""
import os, pandas as pd, streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.getenv("CATALOG", "slhs_test1")
SCHEMA = os.getenv("SCHEMA", "ckd_prod")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "9946580e2a7f4dd1")
FQ = f"{CATALOG}.{SCHEMA}"
w = WorkspaceClient()

st.set_page_config(page_title="CKD Risk Flagging", page_icon="🩺", layout="wide")

def q(sql: str) -> pd.DataFrame:
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql,
                                                catalog=CATALOG, schema=SCHEMA, wait_timeout="50s")
    if r.status.state != StatementState.SUCCEEDED:
        st.error(f"Query failed: {r.status.error.message if r.status and r.status.error else r.status.state}")
        return pd.DataFrame()
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    data = r.result.data_array if r.result and r.result.data_array else []
    return pd.DataFrame(data, columns=cols)

def exec_write(sql: str):
    w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql,
                                            catalog=CATALOG, schema=SCHEMA, wait_timeout="50s")

st.title("🩺 CKD Identification & Risk Flagging")
st.caption("St. Luke's Health System · turns labs, notes, and an ML model into a prioritized clinician worklist with a governed path back to Epic.")

# KPI row
k = q("""SELECT
  COUNT(DISTINCT patient_id) patients,
  SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END) ckd,
  SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END) undoc,
  SUM(CASE WHEN possibly_mis_staged THEN 1 ELSE 0 END) mis,
  SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END) hirisk,
  SUM(CASE WHEN advanced_not_on_protective_meds THEN 1 ELSE 0 END) nomeds
  FROM patient_ckd_analytics""")
if not k.empty:
    c = st.columns(6)
    for col, label, key in zip(c, ["Patients","Have CKD","Undocumented","Mis-staged","High risk (no nephro)","Adv, no protective meds"],
                               ["patients","ckd","undoc","mis","hirisk","nomeds"]):
        col.metric(label, f"{int(k.iloc[0][key]):,}")

tab_work, tab_queue = st.tabs(["Worklist", "Epic write-back queue"])

with tab_work:
    cohort = st.selectbox("Cohort", ["Undocumented CKD","Possibly mis-staged","High risk (no nephrology)",
                                     "Advanced, not on protective meds","Rapid eGFR decline","All CKD patients"])
    where = {"Undocumented CKD":"undocumented_ckd","Possibly mis-staged":"possibly_mis_staged",
             "High risk (no nephrology)":"high_risk_no_nephrology","Advanced, not on protective meds":"advanced_not_on_protective_meds",
             "Rapid eGFR decline":"rapid_egfr_decline","All CKD patients":"is_ckd"}[cohort]
    wl = q(f"""SELECT patient_id, age, sex, lab_kdigo_stage, documented_ckd,
                 suggested_stage, model_confidence, mean_egfr, egfr_trend_recent_minus_prior,
                 sees_nephrology, note_says_nephrology, key_meds
              FROM patient_ckd_analytics WHERE {where} ORDER BY mean_egfr ASC LIMIT 500""")
    st.write(f"**{len(wl)} patients** in cohort: {cohort}")
    st.dataframe(wl, use_container_width=True, hide_index=True)

    if not wl.empty:
        pid = st.selectbox("Open patient", wl["patient_id"].tolist())
        d = wl[wl["patient_id"] == pid].iloc[0]
        left, right = st.columns(2)
        with left:
            st.subheader(f"Patient {pid}")
            st.write(f"**Age/Sex:** {d['age']} / {d['sex']}  ·  **Key meds:** {d['key_meds'] or 'none'}")
            st.write(f"**Lab-derived KDIGO stage:** {d['lab_kdigo_stage']}  ·  **Mean eGFR:** {d['mean_egfr']}")
            st.write(f"**Documented stage:** {d['documented_ckd']}")
            st.write(f"**Model-suggested stage:** {d['suggested_stage']}  (confidence {d['model_confidence']})")
            st.write(f"**Nephrology - structured:** {d['sees_nephrology']}  ·  **from notes (AI):** {d['note_says_nephrology']}")
            # eGFR trend
            trend = q(f"SELECT seq, value FROM bronze_lab_events WHERE patient_id='{pid}' AND analyte='egfr' AND encounter_type='outpatient' ORDER BY seq DESC")
            if not trend.empty:
                trend["value"] = pd.to_numeric(trend["value"])
                st.line_chart(trend.set_index("seq")["value"], height=180)
        with right:
            st.subheader("Review & write back to Epic")
            st.info("Approving records the clinician-confirmed stage in the governed write-back queue "
                    "(the Epic integration point). No PHI leaves Unity Catalog.")
            confirmed = st.selectbox("Confirmed CKD stage", ["G1","G2","G3a","G3b","G4","G5",str(d["suggested_stage"])], index=6)
            refer = st.checkbox("Also flag for nephrology referral", value=bool(d["lab_kdigo_stage"] in ("G4","G5")))
            if st.button("✅ Approve & queue for Epic", type="primary"):
                exec_write(f"""INSERT INTO epic_writeback_queue
                    (patient_id, confirmed_stage, refer_nephrology, reviewed_by, reviewed_at, status)
                    VALUES ('{pid}', '{confirmed}', {str(bool(refer)).lower()}, current_user(), current_timestamp(), 'queued')""")
                st.success(f"Queued stage {confirmed} for patient {pid} for Epic write-back.")

with tab_queue:
    st.subheader("Pending Epic write-backs")
    st.caption("In production this queue is Lakebase (OLTP), drained by an Epic HL7/FHIR / app-orchard integration.")
    qd = q("SELECT patient_id, confirmed_stage, refer_nephrology, reviewed_by, reviewed_at, status FROM epic_writeback_queue ORDER BY reviewed_at DESC LIMIT 200")
    st.dataframe(qd, use_container_width=True, hide_index=True)
