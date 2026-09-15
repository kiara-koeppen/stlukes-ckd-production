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

def genie_bubble():
    """A persistent floating 'Ask Genie' chat bubble pinned to the bottom-right corner."""
    st.markdown("""<style>
    div[data-testid="stPopover"] { position: fixed !important; bottom: 26px; right: 26px; z-index: 1000; }
    div[data-testid="stPopover"] button { border-radius: 26px; background: #FF3621; color: #fff;
        border: none; font-weight: 600; padding: 10px 20px; box-shadow: 0 6px 18px rgba(0,0,0,.25); }
    </style>""", unsafe_allow_html=True)
    if "genie_chat" not in st.session_state:
        st.session_state.genie_chat = []
    # Example questions the tiles/worklist do NOT already answer. Each maps to a trusted query so
    # clicking returns instantly in the demo; free-typed questions go to the live Genie agent.
    SUGGESTIONS = [
        {"q": "Which providers have the most undocumented CKD patients?",
         "sql": "SELECT assigned_provider_name AS provider, count(*) AS undocumented_ckd FROM patient_ckd_analytics WHERE undocumented_ckd = true GROUP BY assigned_provider_name ORDER BY undocumented_ckd DESC LIMIT 10"},
        {"q": "Of the high-risk patients, how many are also not on kidney-protective meds?",
         "sql": "SELECT count(*) AS high_risk_not_on_protective_meds FROM patient_ckd_analytics WHERE high_risk_no_nephrology = true AND on_kidney_protective_meds = false"},
        {"q": "Break down undocumented CKD patients by KDIGO stage.",
         "sql": "SELECT lab_kdigo_stage, count(*) AS patients FROM patient_ckd_analytics WHERE undocumented_ckd = true GROUP BY lab_kdigo_stage ORDER BY lab_kdigo_stage"},
        {"q": "What is the average model confidence by CKD stage?",
         "sql": "SELECT lab_kdigo_stage, round(avg(model_confidence),3) AS avg_confidence FROM patient_ckd_analytics WHERE is_ckd = true GROUP BY lab_kdigo_stage ORDER BY lab_kdigo_stage"},
    ]
    with st.popover("💬  Ask Genie", use_container_width=False):
        h1, h2 = st.columns([3, 1])
        h1.markdown("**Ask about these CKD patients**")
        new_chat = h2.button("🔄 New chat", key="genie_new", use_container_width=True)
        st.caption("Plain-English Q&A on the same governed data — answers match the worklist.")
        history = st.container(height=260)          # rendered on top; filled after we process input
        st.caption("Example questions (instant · not shown by the tiles) — or type your own below:")
        pending = None
        for i, s in enumerate(SUGGESTIONS):
            if st.button(s["q"], key=f"genie_sugg_{i}", use_container_width=True):
                pending = s
        with st.form("genie_form", clear_on_submit=True):
            c1, c2 = st.columns([4, 1])
            typed = c1.text_input("q", label_visibility="collapsed", placeholder="…or type any question")
            if c2.form_submit_button("Ask", use_container_width=True) and typed:
                pending = {"q": typed, "sql": None}
        if new_chat:
            st.session_state.genie_chat = []
            pending = None
        if pending:
            st.session_state.genie_chat.append({"role": "user", "content": pending["q"]})
            with st.spinner("Working…"):
                try:
                    if pending.get("sql"):        # example question -> trusted query, instant
                        df = q(pending["sql"])
                        st.session_state.genie_chat.append({"role": "assistant",
                            "content": "Here's what the trusted query returns:", "sql": pending["sql"], "df": df})
                    else:                          # free-form -> live Genie agent
                        answer, sql, df = ask_genie(pending["q"])
                        st.session_state.genie_chat.append({"role": "assistant", "content": answer, "sql": sql, "df": df})
                except Exception as e:
                    st.session_state.genie_chat.append({"role": "assistant",
                        "content": f"Couldn't get an answer ({type(e).__name__}). Open the full Genie agent: {GENIE_URL}"})
        if not st.session_state.genie_chat:
            history.caption("Pick a suggested question or type your own to start.")
        for turn in st.session_state.genie_chat:                # render into the top container (no rerun -> stays open)
            with history.chat_message(turn["role"]):
                st.markdown(turn["content"])
                if turn.get("df") is not None and not turn["df"].empty:
                    st.dataframe(turn["df"], hide_index=True, use_container_width=True)
                if turn.get("sql"):
                    with st.expander("SQL"):
                        st.code(turn["sql"], language="sql")

st.title("🩺 CKD Identification & Risk Flagging")
st.caption("St. Luke's Health System · labs, notes, and an ML model → a prioritized clinician worklist, "
           "with a governed path back to Epic. Ask questions in plain English, review the model's suggestions, act.")

# --- Population: KPI tiles (Ask Genie lives in the floating corner bubble) ---
k = q("""SELECT COUNT(DISTINCT patient_id) patients, SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END) ckd,
  SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END) undoc, SUM(CASE WHEN possibly_mis_staged THEN 1 ELSE 0 END) mis,
  SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END) hirisk, SUM(CASE WHEN advanced_not_on_protective_meds THEN 1 ELSE 0 END) nomeds
  FROM patient_ckd_analytics""")
if not k.empty:
    c = st.columns(6)
    for col,label,key in zip(c,["Patients","Have CKD","Undocumented","Mis-staged","High risk (no nephro)","Adv, no meds"],
                             ["patients","ckd","undoc","mis","hirisk","nomeds"]):
        col.metric(label, f"{int(k.iloc[0][key]):,}")

genie_bubble()   # floating "Ask Genie" chat bubble, bottom-right, always available

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
            st.caption("Have a question these panels don't answer? Use the 💬 Ask Genie bubble (bottom-right).")
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
