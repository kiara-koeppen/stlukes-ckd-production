"""
St. Luke's CKD Identification & Risk Flagging - clinician worklist app.

Flow: population (KPIs + Ask Genie) -> cohort (worklist) -> patient (review, with in-context
Ask Genie) -> act (approve the confirmed stage -> Epic write-back queue, human-in-the-loop).

The ML model suggests each patient's stage; the App is where a clinician reviews and acts; the
embedded Genie agent answers ad-hoc questions on the SAME governed CKD data.

Auth: uses the app's service principal via databricks-sdk (StatementExecution for SQL, Genie
Conversation API for the Ask-Genie panel).
"""
import os, pandas as pd, streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.getenv("CATALOG", "slhs_test1")
SCHEMA = os.getenv("SCHEMA", "ckd_prod")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "9946580e2a7f4dd1")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f1b09e2ba113eda818aac6bf646a95")
HOST = os.getenv("DATABRICKS_HOST", "https://adb-447340683886633.13.azuredatabricks.net")
GENIE_URL = f"{HOST}/genie/rooms/{GENIE_SPACE_ID}"
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

def ask_genie(question: str):
    """Call the CKD Genie agent (Conversation API). Returns (answer_text, sql, dataframe)."""
    msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, question)
    text, sql, df = [], None, None
    for att in (msg.attachments or []):
        if getattr(att, "text", None) and att.text.content: text.append(att.text.content)
        if getattr(att, "query", None) and att.query.query: sql = att.query.query
    if sql:
        try:
            qr = w.genie.get_message_query_result(GENIE_SPACE_ID, msg.conversation_id, msg.id)
            sr = qr.statement_response
            if sr and sr.result and sr.result.data_array:
                cols = [c.name for c in sr.manifest.schema.columns]
                df = pd.DataFrame(sr.result.data_array, columns=cols)
        except Exception:
            pass
    return (" ".join(text) or "(Genie returned no text answer.)"), sql, df

@st.dialog("🩺 Ask about these CKD patients", width="large")
def ask_dialog(prefill: str = ""):
    st.caption("Plain-English questions on the **same governed CKD data** — answered by the CKD Genie agent. "
               "The numbers match this worklist because both use the same metric view.")
    for s in ["How many undocumented CKD patients are there?",
              "Which providers have the most care-gap patients?",
              "How many high-risk patients are not on kidney-protective meds?"]:
        if st.button(s, key="samp_"+s):
            st.session_state["genie_q"] = s
    question = st.text_input("Your question", value=st.session_state.get("genie_q", prefill),
                             placeholder="e.g., How many high-risk patients aren't seeing nephrology?")
    if st.button("Ask Genie", type="primary") and question:
        with st.spinner("Asking Genie…"):
            try:
                answer, sql, df = ask_genie(question)
                st.markdown(f"**{answer}**")
                if df is not None and not df.empty:
                    st.dataframe(df, hide_index=True, use_container_width=True)
                if sql:
                    with st.expander("SQL Genie ran"):
                        st.code(sql, language="sql")
            except Exception as e:
                st.warning(f"Couldn't reach the Genie agent from here ({type(e).__name__}). "
                           f"Open the full agent: {GENIE_URL}")

st.title("🩺 CKD Identification & Risk Flagging")
st.caption("St. Luke's Health System · labs, notes, and an ML model → a prioritized clinician worklist, "
           "with a governed path back to Epic. Ask questions in plain English, review the model's suggestions, act.")

# --- Population: KPIs + Ask Genie ---
top = st.columns([5,1])
k = q("""SELECT COUNT(DISTINCT patient_id) patients, SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END) ckd,
  SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END) undoc, SUM(CASE WHEN possibly_mis_staged THEN 1 ELSE 0 END) mis,
  SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END) hirisk, SUM(CASE WHEN advanced_not_on_protective_meds THEN 1 ELSE 0 END) nomeds
  FROM patient_ckd_analytics""")
with top[0]:
    if not k.empty:
        c = st.columns(6)
        for col,label,key in zip(c,["Patients","Have CKD","Undocumented","Mis-staged","High risk (no nephro)","Adv, no meds"],
                                 ["patients","ckd","undoc","mis","hirisk","nomeds"]):
            col.metric(label, f"{int(k.iloc[0][key]):,}")
with top[1]:
    st.write("");
    if st.button("🔎 Ask Genie", use_container_width=True):
        st.session_state["genie_q"] = ""
        ask_dialog()

tab_work, tab_queue = st.tabs(["Worklist", "Epic write-back queue"])

with tab_work:
    cohort = st.selectbox("Cohort (model-flagged)", ["Undocumented CKD","Possibly mis-staged","High risk (no nephrology)",
                          "Advanced, not on protective meds","Rapid eGFR decline","All CKD patients"])
    where = {"Undocumented CKD":"undocumented_ckd","Possibly mis-staged":"possibly_mis_staged",
             "High risk (no nephrology)":"high_risk_no_nephrology","Advanced, not on protective meds":"advanced_not_on_protective_meds",
             "Rapid eGFR decline":"rapid_egfr_decline","All CKD patients":"is_ckd"}[cohort]
    wl = q(f"""SELECT patient_id, age, sex, lab_kdigo_stage, documented_ckd, suggested_stage, model_confidence,
                 mean_egfr, egfr_trend_recent_minus_prior, sees_nephrology, note_says_nephrology, key_meds
              FROM patient_ckd_analytics WHERE {where} ORDER BY mean_egfr ASC LIMIT 500""")
    st.write(f"**{len(wl)} patients** · {cohort} — each row carries the model's **suggested stage + confidence**.")
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
            st.write(f"**Nephrology — structured:** {d['sees_nephrology']}  ·  **from notes (AI):** {d['note_says_nephrology']}")
            trend = q(f"SELECT seq, value FROM bronze_lab_events WHERE patient_id='{pid}' AND analyte='egfr' AND encounter_type='outpatient' ORDER BY seq DESC")
            if not trend.empty:
                trend["value"] = pd.to_numeric(trend["value"]); st.line_chart(trend.set_index("seq")["value"], height=180)
            if st.button("🔎 Ask Genie about this cohort"):
                ask_dialog(prefill=f"For {cohort.lower()} patients, ")
        with right:
            st.subheader("Review & write back to Epic")
            st.info("Approving records the clinician-confirmed stage in the governed write-back queue "
                    "(the Epic integration point). Human-in-the-loop — nothing auto-writes to Epic.")
            confirmed = st.selectbox("Confirmed CKD stage", ["G1","G2","G3a","G3b","G4","G5",str(d["suggested_stage"])], index=6)
            refer = st.checkbox("Also flag for nephrology referral", value=bool(d["lab_kdigo_stage"] in ("G4","G5")))
            if st.button("✅ Approve & queue for Epic", type="primary"):
                exec_write(f"""INSERT INTO epic_writeback_queue (patient_id, confirmed_stage, refer_nephrology, reviewed_by, reviewed_at, status)
                    VALUES ('{pid}', '{confirmed}', {str(bool(refer)).lower()}, current_user(), current_timestamp(), 'queued')""")
                st.success(f"Queued stage {confirmed} for patient {pid} for Epic write-back.")

with tab_queue:
    st.subheader("Pending Epic write-backs")
    st.caption("In production this queue is Lakebase (OLTP), drained by an Epic HL7/FHIR / app-orchard integration.")
    st.dataframe(q("SELECT patient_id, confirmed_stage, refer_nephrology, reviewed_by, reviewed_at, status FROM epic_writeback_queue ORDER BY reviewed_at DESC LIMIT 200"),
                 use_container_width=True, hide_index=True)
