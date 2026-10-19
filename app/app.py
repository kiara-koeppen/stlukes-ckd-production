"""
St. Luke's CKD Identification & Risk Flagging - clinician worklist app.

Flow: population (KPIs + Ask Genie) -> cohort (worklist) -> patient (review, with in-context
Ask Genie) -> act (approve the confirmed stage -> Epic write-back queue, human-in-the-loop).

The ML model suggests each patient's stage; the App is where a clinician reviews and acts; the
embedded Genie agent answers ad-hoc questions on the SAME governed CKD data.

Data paths:
  * Lakebase Postgres (operational): the worklist + KPI tiles read the synced `patient_worklist_synced`
    table, and clinician approvals are written to the app-owned OLTP table `app.epic_writeback_queue`.
  * SQL warehouse (analytical): patient detail, notes, lab trends, live ai_query on a pasted note.
  * Model Serving: the fusion (labs + notes) CKD-risk model. Genie Conversation API: Ask Genie.
Auth: the app's service principal (databricks-sdk); Lakebase via an OAuth database credential.
"""
import os, time, pandas as pd, streamlit as st
import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.getenv("CATALOG", "slhs_test1")
SCHEMA = os.getenv("SCHEMA", "ckd_febar")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "9946580e2a7f4dd1")
GENIE_TITLE = os.getenv("GENIE_SPACE_TITLE", "CKD Identification & Risk Flagging (FE Bar)")
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "projects/ckd-febar/branches/production/endpoints/primary")
HOST = os.getenv("DATABRICKS_HOST", "https://adb-447340683886633.13.azuredatabricks.net")
if not HOST.startswith("http"):                 # Apps runtime sets DATABRICKS_HOST without a scheme
    HOST = "https://" + HOST

w = WorkspaceClient()
st.set_page_config(page_title="CKD Risk Flagging", page_icon="🩺", layout="wide")

@st.cache_resource
def genie_space_id() -> str:
    """The Genie agent is created by the bundle job (task 06); find it by title."""
    spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
    return next((s["space_id"] for s in spaces if s.get("title") == GENIE_TITLE), "")

GENIE_SPACE_ID = genie_space_id()
GENIE_URL = f"{HOST.rstrip('/')}/genie/rooms/{GENIE_SPACE_ID}"

# --- Lakebase (Postgres) -------------------------------------------------------------------------
# PGHOST / PGDATABASE / PGUSER / PGPORT / PGSSLMODE are injected by the app's `postgres` resource.
# The password is a short-lived OAuth database credential, refreshed before it expires.
@st.cache_resource
def _pg_token_holder():
    return {"token": None, "at": 0.0}

def pg_connect():
    h = _pg_token_holder()
    if not h["token"] or time.time() - h["at"] > 40 * 60:
        h["token"] = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
        h["at"] = time.time()
    return psycopg.connect(host=os.environ["PGHOST"], dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
                           user=os.environ["PGUSER"], password=h["token"], port=os.environ.get("PGPORT", "5432"),
                           sslmode=os.environ.get("PGSSLMODE", "require"))

def pgq(sql: str, params=None) -> pd.DataFrame:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])

def pg_exec(sql: str, params=None):
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)

@st.cache_resource
def ensure_writeback_queue():
    """The OLTP queue table is created by the bundle job (task 05); fail loudly if it is missing."""
    pgq("SELECT 1 FROM app.epic_writeback_queue LIMIT 1")
    return True

def queue_for_epic(patient_id: str, stage: str, refer: bool, source: str):
    ensure_writeback_queue()
    reviewer = st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-Preferred-Username") or "unknown"
    pg_exec("""INSERT INTO app.epic_writeback_queue (patient_id, confirmed_stage, refer_nephrology, source, reviewed_by)
               VALUES (%s, %s, %s, %s, %s)""", (patient_id, stage, bool(refer), source, reviewer))

WL = f'"{SCHEMA}".patient_worklist_synced'   # Lakebase synced copy of the gold worklist
WL_COLS = """patient_id, age, sex, lab_kdigo_stage, documented_ckd, notes_ckd_risk, ckd_risk_score,
             suggested_stage, model_confidence, mean_egfr, sees_nephrology, key_meds, caught_by_notes"""

def q(sql: str) -> pd.DataFrame:
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql,
                                                catalog=CATALOG, schema=SCHEMA, wait_timeout="50s")
    if r.status.state != StatementState.SUCCEEDED:
        st.error(f"Query failed: {r.status.error.message if r.status and r.status.error else r.status.state}")
        return pd.DataFrame()
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    data = r.result.data_array if r.result and r.result.data_array else []
    return pd.DataFrame(data, columns=cols)


RISK_MODEL_ENDPOINT = os.getenv("CKD_RISK_ENDPOINT", "ckd-risk-model-febar")
RISK_ORD = {"none": 0, "low": 1, "moderate": 2, "high": 3}
# The editable "CKD-from-notes definition" — the prompt the AI uses to judge a note. Reviewers can tune it.
NOTE_RISK_PROMPT_DEFAULT = (
    "You are a nephrology-aware clinician reviewing an outpatient note that may NOT mention kidney disease by name. "
    "Judge ONLY from indirect evidence: proteinuria or foamy urine, ankle or periorbital edema, rising or elevated "
    "creatinine, poorly controlled hypertension, poorly controlled diabetes, nocturia, fatigue, nausea or poor appetite. "
    "Assess this patient's risk of having undiagnosed chronic kidney disease. Respond with ONLY minified JSON: "
    '{"ckd_risk":"none|low|moderate|high","indicators":["short phrases you found"],"rationale":"one sentence"}.')

def run_note_ai(prompt: str, note_text: str) -> dict:
    """Run the (editable) CKD-from-notes prompt over a note via ai_query, live on the warehouse."""
    import json as _json, re as _re
    p, n = prompt.replace("'", "''"), (note_text or "").replace("'", "''")
    df = q(f"SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', concat('{p}', ' Note: ', '{n}')) AS resp")
    if df.empty:
        return {}
    m = _re.search(r"\{.*\}", df.iloc[0]["resp"] or "", _re.S)
    try:
        return _json.loads(m.group(0)) if m else {}
    except Exception:
        return {}

def extract_note_text(f) -> str:
    """Read an uploaded .txt or .pdf note to text."""
    if f is None:
        return ""
    if f.name.lower().endswith(".pdf"):
        try:
            from pypdf import PdfReader
            return "\n".join((pg.extract_text() or "") for pg in PdfReader(f).pages).strip()
        except Exception as e:
            return f"(Could not read PDF: {type(e).__name__})"
    try:
        return f.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""

def predict_ckd_risk(features: dict):
    """Query the fusion (labs + notes) risk-model serving endpoint. Returns the prediction or None."""
    try:
        r = w.serving_endpoints.query(RISK_MODEL_ENDPOINT, dataframe_records=[features])
        return r.predictions[0]
    except Exception:
        return None

# Friendly labels for Genie's live message-status stages (mirrors the native agent's "thinking").
GENIE_STAGE = {
    "SUBMITTED": "Sending your question to Genie…",
    "FILTERING_CONTEXT": "Understanding the question…",
    "FETCHING_METADATA": "Finding the right tables…",
    "ASKING_AI": "Reasoning and writing SQL…",
    "PENDING_WAREHOUSE": "Starting the SQL warehouse…",
    "EXECUTING_QUERY": "Running the query on governed data…",
    "COMPLETED": "Composing the answer…",
}

def ask_genie(question: str, on_status=None):
    """Ask the CKD Genie agent, polling the message so the caller can surface Genie's live
    stages (its 'thought process'). Returns (answer_text, sql, dataframe, conv_id, msg_id)."""
    wait = w.genie.start_conversation(GENIE_SPACE_ID, question)
    conv_id, msg_id = wait.conversation_id, wait.message_id
    msg, seen = None, None
    for _ in range(150):                       # ~3 min ceiling
        msg = w.genie.get_message(GENIE_SPACE_ID, conv_id, msg_id)
        code = msg.status.value if msg.status else ""
        if on_status and code != seen:
            on_status(GENIE_STAGE.get(code, code)); seen = code
        if code in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            break
        time.sleep(1.2)
    text, sql, df = [], None, None
    for att in (msg.attachments or []) if msg else []:
        if getattr(att, "text", None) and att.text.content: text.append(att.text.content)
        if getattr(att, "query", None) and att.query.query: sql = att.query.query
    if sql:
        try:
            qr = w.genie.get_message_query_result(GENIE_SPACE_ID, conv_id, msg_id)
            sr = qr.statement_response
            if sr and sr.result and sr.result.data_array:
                cols = [c.name for c in sr.manifest.schema.columns]
                df = pd.DataFrame(sr.result.data_array, columns=cols)
        except Exception:
            pass
    return (" ".join(text) or "(Genie returned no text answer.)"), sql, df, conv_id, msg_id

def genie_bubble():
    """A persistent floating 'Ask Genie' chat bubble pinned to the bottom-right corner."""
    st.markdown("""<style>
    div[data-testid="stPopover"] { position: fixed !important; bottom: 26px; right: 26px; z-index: 1000; }
    div[data-testid="stPopover"] button { border-radius: 26px; background: #FF3621; color: #fff;
        border: none; font-weight: 600; padding: 10px 20px; box-shadow: 0 6px 18px rgba(0,0,0,.25); }
    </style>""", unsafe_allow_html=True)
    if "genie_chat" not in st.session_state:
        st.session_state.genie_chat = []
    # Clickable example questions the tiles/worklist do NOT already answer. Clicking one sends it to
    # the live Genie agent (same path as a free-typed question) so it returns a full natural-language answer.
    SUGGESTIONS = [
        "Which providers have the most undocumented CKD patients?",
        "Of the high-risk patients, how many are also not on kidney-protective meds?",
        "Break down undocumented CKD patients by KDIGO stage.",
        "What is the average model confidence by CKD stage?",
    ]
    def feedback_controls(turn):
        # 👍/👎 on an answer — sent to the Genie space (same as the native agent's rating).
        cid, mid = turn.get("conv_id"), turn.get("msg_id")
        if not (cid and mid):
            return
        try:                                    # older SDKs lack the feedback API — hide, don't crash
            from databricks.sdk.service.dashboards import GenieFeedbackRating
        except Exception:
            return
        if turn.get("feedback"):
            st.caption("👍 Feedback sent to Genie." if turn["feedback"] == "POSITIVE" else "👎 Feedback sent to Genie.")
            return
        b1, b2, _ = st.columns([1, 1, 8])
        clicked = "POSITIVE" if b1.button("👍", key=f"fb_up_{mid}", help="Helpful — send to Genie") else \
                  "NEGATIVE" if b2.button("👎", key=f"fb_down_{mid}", help="Not helpful — send to Genie") else None
        if clicked:
            try:
                w.genie.send_message_feedback(GENIE_SPACE_ID, cid, mid,
                    GenieFeedbackRating.POSITIVE if clicked == "POSITIVE" else GenieFeedbackRating.NEGATIVE)
            except Exception:
                pass
            turn["feedback"] = clicked
            st.caption("✓ Sent to Genie — thanks!")

    def render_turn(container, turn):
        with container.chat_message(turn["role"]):
            st.markdown(turn["content"])
            if turn.get("df") is not None and not turn["df"].empty:
                st.dataframe(turn["df"], hide_index=True, use_container_width=True)
            if turn.get("sql"):
                with st.expander("🔎 The SQL Genie wrote"):
                    st.code(turn["sql"], language="sql")
            if turn["role"] == "assistant":
                feedback_controls(turn)

    with st.popover("💬  Ask Genie", use_container_width=False):
        h1, h2, h3 = st.columns([2, 1.5, 1])
        h1.markdown("**Ask about these CKD patients**")
        h2.link_button("↗ Open in Genie", GENIE_URL, use_container_width=True)  # the full native agent
        new_chat = h3.button("🔄 New chat", key="genie_new", use_container_width=True)
        st.caption("Plain-English Q&A on the same governed data — answers match the worklist.")
        history = st.container(height=320)          # the chat thread; thinking + answers land here

        pending = None
        # Example questions, collapsed into an expander to save space (open when the chat is empty).
        with st.expander("💡 Example questions (ask the live Genie agent)",
                         expanded=not st.session_state.genie_chat):
            for i, s in enumerate(SUGGESTIONS):
                if st.button(s, key=f"genie_sugg_{i}", use_container_width=True):
                    pending = s
        with st.form("genie_form", clear_on_submit=True):
            c1, c2 = st.columns([4, 1])
            typed = c1.text_input("q", label_visibility="collapsed", placeholder="…or type any question")
            if c2.form_submit_button("Ask", use_container_width=True) and typed:
                pending = typed
        if new_chat:
            st.session_state.genie_chat = []
            pending = None

        # Render the existing thread first.
        if not st.session_state.genie_chat and not pending:
            history.caption("Pick a suggested question or type your own to start.")
        for turn in st.session_state.genie_chat:
            render_turn(history, turn)

        # Process a new question INSIDE the thread: the user bubble, then the assistant bubble whose
        # live "thinking" status sits directly above where the answer lands.
        if pending:
            with history.chat_message("user"):
                st.markdown(pending)
            a_turn = {"role": "assistant"}
            with history.chat_message("assistant"):
                with st.status("Asking Genie…", expanded=True) as status:
                    try:                                   # example buttons + typed both hit the live agent
                        answer, sql, df, cid, mid = ask_genie(pending, on_status=lambda label: status.update(label=f"💭 {label}"))
                        status.update(label="Genie answered ✓", state="complete", expanded=False)
                    except Exception as e:
                        status.update(label="Genie hit an error", state="error")
                        answer, sql, df, cid, mid = (f"Couldn't get an answer ({type(e).__name__}). "
                                           f"Open the full Genie agent: {GENIE_URL}"), None, None, None, None
                st.markdown(answer)
                if df is not None and not df.empty:
                    st.dataframe(df, hide_index=True, use_container_width=True)
                if sql:
                    with st.expander("🔎 The SQL Genie wrote"):
                        st.code(sql, language="sql")
                a_turn.update({"content": answer, "sql": sql, "df": df, "conv_id": cid, "msg_id": mid})
                feedback_controls(a_turn)
            st.session_state.genie_chat.append({"role": "user", "content": pending})
            st.session_state.genie_chat.append(a_turn)

st.title("🩺 CKD Identification & Risk Flagging")
st.caption("St. Luke's Health System · labs, notes, and an ML model → a prioritized clinician worklist, "
           "with a governed path back to Epic. Ask questions in plain English, review the model's suggestions, act.")

# --- Population: KPI tiles (Ask Genie lives in the floating corner bubble) ---
k = pgq(f"""SELECT COUNT(DISTINCT patient_id) patients, SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END) ckd,
  SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END) undoc,
  SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END) hirisk,
  SUM(CASE WHEN notes_risk_flag THEN 1 ELSE 0 END) notesrisk,
  SUM(CASE WHEN caught_by_notes THEN 1 ELSE 0 END) caught
  FROM {WL}""")
if not k.empty:
    c = st.columns(6)
    for col,label,key,helptext in zip(c,
        ["Patients","Have CKD","Undocumented","High risk (no nephro)","Notes flag CKD","Caught by notes"],
        ["patients","ckd","undoc","hirisk","notesrisk","caught"],
        ["","by labs","CKD by labs, not in Epic","advanced, not seeing nephrology",
         "AI read the notes and flagged CKD risk","normal labs — only the notes caught it"]):
        col.metric(label, f"{int(k.iloc[0][key]):,}", help=helptext or None)

genie_bubble()   # floating "Ask Genie" chat bubble, bottom-right, always available

tab_work, tab_analyzer, tab_queue = st.tabs(["Worklist", "🔬 Notes → CKD analyzer", "Epic write-back queue"])

with tab_work:
    cohort = st.selectbox("Cohort (model-flagged)", ["Caught by notes (occult CKD)","Undocumented CKD","Notes flagged at-risk",
                          "High risk (no nephrology)","Possibly mis-staged","Advanced, not on protective meds",
                          "Rapid eGFR decline","All CKD patients"])
    where = {"Caught by notes (occult CKD)":"caught_by_notes","Undocumented CKD":"undocumented_ckd",
             "Notes flagged at-risk":"notes_risk_flag","High risk (no nephrology)":"high_risk_no_nephrology",
             "Possibly mis-staged":"possibly_mis_staged","Advanced, not on protective meds":"advanced_not_on_protective_meds",
             "Rapid eGFR decline":"rapid_egfr_decline","All CKD patients":"is_ckd"}[cohort]
    if cohort == "Caught by notes (occult CKD)":
        st.info("**These patients have normal-looking kidney labs** (e.g. eGFR ≥90, no albuminuria on record) — a labs-only "
                "approach misses them. The AI read their clinical notes, found indirect signs, and the model flagged them. "
                "This is CKD **hidden in the notes**.")
    # `where` is one of the fixed boolean column names above (never user text)
    t0 = time.time()
    wl = pgq(f"SELECT {WL_COLS} FROM {WL} WHERE {where} ORDER BY ckd_risk_score DESC LIMIT 500")
    st.caption(f"Served from Lakebase Postgres in {(time.time() - t0) * 1000:.0f} ms")
    st.write(f"**{len(wl)} patients** · {cohort} — each row shows the **AI notes-risk** and the **fusion CKD-risk score** (labs + notes).")
    st.dataframe(wl, use_container_width=True, hide_index=True)

    if not wl.empty:
        ids = wl["patient_id"].tolist()
        csel, cpaste = st.columns([2, 1])
        sel = csel.selectbox("Open patient", ids)          # searchable dropdown of the current cohort
        pasted = cpaste.text_input("…or paste a patient ID", placeholder="any patient ID",
                                   help="Jumps to any patient, even one outside this cohort").strip()
        pid = pasted or sel
        row = wl[wl["patient_id"] == pid]
        if row.empty and pasted:                           # pasted ID not in this cohort — look it up directly
            row = pgq(f"SELECT {WL_COLS} FROM {WL} WHERE patient_id = %s", (pasted,))
        if row.empty:
            st.warning(f"No patient found with ID '{pid}'. Check the ID, or pick one from the list.")
        else:
            d = row.iloc[0]; pid = d["patient_id"]
            left, right = st.columns(2)
            with left:
                st.subheader(f"Patient {pid}")
                st.write(f"**Age/Sex:** {d['age']} / {d['sex']}  ·  **Key meds:** {d['key_meds'] or 'none'}")
                st.write(f"**Lab-derived KDIGO stage:** {d['lab_kdigo_stage']}  ·  **Mean eGFR:** {d['mean_egfr']}")
                st.write(f"**Documented stage:** {d['documented_ckd']}")
                st.write(f"**Model-suggested stage:** {d['suggested_stage']}  (confidence {d['model_confidence']})")
                st.write(f"**AI notes CKD-risk:** {str(d['notes_ckd_risk']).upper()}  ·  **Fusion CKD-risk score (labs+notes):** {d['ckd_risk_score']}")
                if str(d.get('caught_by_notes')).lower() == 'true':
                    st.warning("🔬 **Caught by the notes** — normal labs; the AI's reading of the notes is what surfaced this CKD risk.")
                # The physician note is the evidence the reviewer needs to read before confirming — show it.
                note = q(f"SELECT note_text FROM bronze_clinical_notes WHERE patient_id='{pid}'")
                ind  = q(f"SELECT notes_ckd_risk, notes_indicators FROM patient_ckd_analytics WHERE patient_id='{pid}'")
                with st.expander("📄 Physician note — read what the AI read",
                                 expanded=str(d.get('caught_by_notes')).lower() == 'true'):
                    st.markdown(f"> {note.iloc[0]['note_text']}" if not note.empty and note.iloc[0]['note_text']
                                else "_No note on file for this patient._")
                    if not ind.empty:
                        st.caption(f"**AI read this as** {str(ind.iloc[0]['notes_ckd_risk']).upper()} CKD risk  ·  "
                                   f"**indicators it flagged:** {ind.iloc[0]['notes_indicators']}")
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
                    queue_for_epic(pid, confirmed, refer, "worklist")
                    st.success(f"Queued stage {confirmed} for patient {pid} for Epic write-back.")

with tab_analyzer:
    st.subheader("Notes → CKD analyzer")
    st.caption("Reviewer tool: pull a patient's labs, drop in a clinical note, and let the AI read the note for indirect "
               "signs of CKD — then the model fuses that with the labs. Tune the definition the AI uses, live.")
    la, lb = st.columns(2)
    with la:
        apid = st.text_input("Patient ID — pull their labs", placeholder="e.g. ACO-…").strip()
        labrow = None
        if apid:
            safe = apid.replace("'", "''")
            labrow = q(f"""SELECT patient_id, age, sex, mean_egfr, latest_egfr, prior_egfr, egfr_trend_recent_minus_prior,
                             mean_creatinine, albuminuria_cat, lab_kdigo_stage, documented_ckd
                          FROM patient_ckd_analytics WHERE patient_id='{safe}'""")
            if labrow.empty:
                st.warning("No patient with that ID."); labrow = None
            else:
                Lr = labrow.iloc[0]
                m1, m2, m3 = st.columns(3)
                m1.metric("Mean eGFR", Lr["mean_egfr"]); m2.metric("Creatinine", Lr["mean_creatinine"])
                m3.metric("Albuminuria", Lr["albuminuria_cat"])
                st.caption(f"Lab-derived stage **{Lr['lab_kdigo_stage']}** · documented **{Lr['documented_ckd']}**")
                tr = q(f"SELECT seq, value FROM bronze_lab_events WHERE patient_id='{safe}' AND analyte='egfr' AND encounter_type='outpatient' ORDER BY seq DESC")
                if not tr.empty:
                    tr["value"] = pd.to_numeric(tr["value"]); st.line_chart(tr.set_index("seq")["value"], height=150)
    with lb:
        up = st.file_uploader("Upload a clinical note (.txt or .pdf)", type=["txt", "pdf"])
        note_text = extract_note_text(up) if up is not None else ""
        note_text = st.text_area("…or paste the note text", value=note_text, height=170,
                                 placeholder="Paste an outpatient note — the AI infers CKD risk from indirect signs.")
    with st.expander("⚙️ CKD-from-notes definition — the prompt the AI uses (edit to tune what counts as suspicious)"):
        prompt = st.text_area("Definition", value=st.session_state.get("note_prompt", NOTE_RISK_PROMPT_DEFAULT), height=180)
        st.session_state["note_prompt"] = prompt
    if st.button("🔬 Analyze note + labs", type="primary"):
        if not note_text.strip():
            st.warning("Paste or upload a note first.")
        else:
            with st.spinner("AI reading the note…"):
                res = run_note_ai(st.session_state.get("note_prompt", NOTE_RISK_PROMPT_DEFAULT), note_text)
            risk = str(res.get("ckd_risk", "unknown")).lower()
            st.markdown(f"### AI notes read: **{risk.upper()}** CKD risk")
            if res.get("rationale"): st.write(f"_{res['rationale']}_")
            if res.get("indicators"): st.write("**Indicators found:** " + ", ".join(res["indicators"]))
            if labrow is not None:
                Lr = labrow.iloc[0]
                feat = {"age": float(Lr["age"]), "mean_egfr": float(Lr["mean_egfr"]),
                        "latest_egfr": float(Lr["latest_egfr"]), "prior_egfr": float(Lr["prior_egfr"]),
                        "egfr_trend_recent_minus_prior": float(Lr["egfr_trend_recent_minus_prior"]),
                        "mean_creatinine": float(Lr["mean_creatinine"]), "notes_ckd_risk_ord": RISK_ORD.get(risk, 0)}
                pred = predict_ckd_risk(feat)
                if pred is None:
                    st.info("Fusion model endpoint is warming up (scale-to-zero) — try again in ~30s, or rely on the AI read + labs above.")
                else:
                    flagged = str(pred).strip() in ("1", "1.0", "true", "True")
                    st.markdown(f"### Fusion model (labs + this note): **{'⚠️ CKD RISK' if flagged else 'not flagged'}**")
                    st.caption(f"Feature sent to the model — eGFR {feat['mean_egfr']}, creatinine {feat['mean_creatinine']}, "
                               f"notes-risk **{risk}**. The notes signal is what lets the model catch CKD when the labs look normal.")
                    if flagged:
                        refer2 = st.checkbox("Also flag for nephrology referral", key="an_refer")
                        if st.button("✅ Approve & queue this patient for Epic", key="an_approve"):
                            queue_for_epic(apid, "flagged-from-notes", refer2, "notes-analyzer")
                            st.success(f"Queued {apid} for Epic write-back (flagged via notes + labs).")
            else:
                st.info("Enter a patient ID above to also run the fusion model (labs + this note).")

with tab_queue:
    st.subheader("Pending Epic write-backs")
    st.caption("Stored in Lakebase Postgres (`app.epic_writeback_queue`). Next step in production: an Epic "
               "HL7/FHIR integration drains queued rows and writes the confirmed stage back to the chart.")
    ensure_writeback_queue()
    st.dataframe(pgq("""SELECT id, patient_id, confirmed_stage, refer_nephrology, source, reviewed_by, reviewed_at, status
                        FROM app.epic_writeback_queue ORDER BY reviewed_at DESC LIMIT 200"""),
                 use_container_width=True, hide_index=True)
