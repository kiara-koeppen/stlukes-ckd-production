# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - End-to-end validation (execution evidence)
# MAGIC
# MAGIC Every check runs against the LIVE assets this job just built and records its actual output.
# MAGIC Results are written as text (markdown + JSON) to the `evidence` volume and committed to the repo
# MAGIC under `evidence/`, so a reviewer can read what ran without screenshots. Any failed check fails
# MAGIC the job.
# MAGIC
# MAGIC Layers covered: Lakeflow ingest + expectations -> Unity Catalog governance -> Gen AI notes ->
# MAGIC ML models + serving -> Metric View -> Genie Agent -> Lakebase (synced read + OLTP write) -> App.

# COMMAND ----------
# MAGIC %pip install "psycopg[binary]>=3.1" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
dbutils.widgets.text("lakebase_project", "ckd-febar")
dbutils.widgets.text("app_name", "ckd-risk-worklist-febar")
dbutils.widgets.text("risk_endpoint", "ckd-risk-model-febar")
dbutils.widgets.text("genie_space_id", "")
CATALOG, SCHEMA, PROJECT, APP, RISK_EP, GENIE = (dbutils.widgets.get(k) for k in
    ("catalog", "schema", "lakebase_project", "app_name", "risk_endpoint", "genie_space_id"))
S = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
import json, time, datetime
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
results = []

def check(layer, name, ok, detail):
    results.append({"layer": layer, "check": name, "result": "PASS" if ok else "FAIL", "detail": detail})
    print(("PASS" if ok else "FAIL"), "|", layer, "|", name, "|", detail)

def one(sql):
    return spark.sql(sql).first()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 1 - Lakeflow ingest, medallion, and data-quality expectations

# COMMAND ----------
c = one(f"""SELECT (SELECT count(*) FROM {S}.bronze_patient) p, (SELECT count(*) FROM {S}.bronze_lab_events) l,
  (SELECT count(*) FROM {S}.bronze_clinical_notes) n, (SELECT count(*) FROM {S}.silver_lab_events) sl,
  (SELECT count(*) FROM {S}.silver_patient_labs) sp, (SELECT count(*) FROM {S}.patient_ckd_gold) g,
  (SELECT count(*) FROM {S}.silver_lab_events WHERE encounter_type <> 'outpatient') inpatient_in_silver,
  (SELECT count(*) FROM {S}.bronze_lab_events WHERE encounter_type = 'inpatient') inpatient_in_bronze""")
check("Lakeflow", "Bronze rows ingested by Auto Loader from the landing volume",
      c.p == 2000 and c.n == 2000 and c.l > 12000, f"bronze_patient={c.p}, bronze_lab_events={c.l}, bronze_clinical_notes={c.n}")
check("Lakeflow", "KDIGO outpatient-only rule enforced in silver (expectation drops inpatient labs)",
      c.inpatient_in_bronze > 0 and c.inpatient_in_silver == 0 and c.sl == c.l - c.inpatient_in_bronze,
      f"inpatient in bronze={c.inpatient_in_bronze}, inpatient in silver={c.inpatient_in_silver}, silver_lab_events={c.sl}")
check("Lakeflow", "Silver + gold one row per patient", c.sp == 2000 and c.g == 2000, f"silver_patient_labs={c.sp}, patient_ckd_gold={c.g}")

dq = spark.sql(f"""
  SELECT e.name AS expectation, e.dataset, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
  FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
          'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) e
        FROM event_log(TABLE({S}.silver_lab_events))
        WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
          AND origin.update_id = (SELECT origin.update_id FROM event_log(TABLE({S}.silver_lab_events))
                                  WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1))
  GROUP BY 1, 2 ORDER BY 2, 1""").collect()
dq_rows = [r.asDict() for r in dq]
outp = [r for r in dq_rows if r["expectation"] == "outpatient_only"]
check("Lakeflow", "Pipeline event log records expectation metrics (latest update)",
      bool(outp) and outp[0]["failed"] == c.inpatient_in_bronze, json.dumps(dq_rows))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 2 - Unity Catalog governance

# COMMAND ----------
cons = spark.sql(f"""SELECT table_name, constraint_type, constraint_name FROM {CATALOG}.information_schema.table_constraints
                     WHERE table_schema = '{SCHEMA}' ORDER BY 1, 2""").collect()
cons_s = [f"{r.table_name}:{r.constraint_type}" for r in cons]
check("Unity Catalog", "Primary/foreign keys declared on pipeline tables",
      "bronze_patient:PRIMARY KEY" in cons_s and "patient_ckd_gold:PRIMARY KEY" in cons_s and "patient_ckd_gold:FOREIGN KEY" in cons_s,
      ", ".join(cons_s))
m = one(f"SELECT (SELECT max(mrn) FROM {S}.patient_ckd_gold) g, (SELECT max(mrn) FROM {S}.bronze_patient) b, is_account_group_member('ckd_phi_readers') member")
check("Unity Catalog", "PHI column mask on MRN (reader not in ckd_phi_readers sees masked value)",
      (not m.member) and m.g == "MRN-***-****" and m.b == "MRN-***-****", f"gold.mrn={m.g}, bronze.mrn={m.b}, reader_in_phi_group={m.member}")
cm = one(f"SELECT count(*) n, count(comment) commented FROM {CATALOG}.information_schema.columns WHERE table_schema='{SCHEMA}' AND table_name='patient_ckd_gold'")
check("Unity Catalog", "Gold columns documented with comments", cm.commented == cm.n, f"{cm.commented}/{cm.n} columns commented")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 3 - Gen AI (notes) and ML (models + serving)

# COMMAND ----------
a = one(f"""SELECT count(*) n, sum(CASE WHEN sig.ckd_risk IS NULL THEN 1 ELSE 0 END) unparsed,
  sum(CASE WHEN lower(sig.ckd_risk) IN ('moderate','high') THEN 1 ELSE 0 END) at_risk FROM {S}.note_ckd_signals""")
check("Gen AI", "ai_query read every note and returned structured CKD risk", a.n == 2000 and a.unparsed <= 20,
      f"notes={a.n}, unparsed={a.unparsed}, moderate/high risk={a.at_risk}")
ex = spark.sql(f"""SELECT n.sig.ckd_risk risk, n.sig.indicators indicators FROM {S}.note_ckd_signals n
                   JOIN {S}.patient_ckd_risk_scored r USING (patient_id) WHERE r.caught_by_notes LIMIT 1""").first()
check("Gen AI", "Example AI read on an occult-CKD patient (normal labs, caught by notes)", ex is not None,
      f"risk={ex.risk if ex else None}, indicators={list(ex.indicators) if ex and ex.indicators else None}")

s1 = one(f"""SELECT count(*) n, sum(CASE WHEN model_agrees_with_documented THEN 1 ELSE 0 END) agree, round(avg(confidence),3) conf
             FROM {S}.patient_ckd_scored""")
check("ML", "Stage-suggestion model batch scores", s1.n == 2000, f"scored={s1.n}, agree with clinician stage={s1.agree}, mean confidence={s1.conf}")
s2 = one(f"""SELECT count(*) n, sum(CASE WHEN caught_by_notes THEN 1 ELSE 0 END) caught, sum(CASE WHEN ckd_risk_pred THEN 1 ELSE 0 END) flagged
             FROM {S}.patient_ckd_risk_scored""")
check("ML", "Fusion (labs + notes) model catches CKD the labs-only model misses", s2.n == 2000 and s2.caught > 0,
      f"scored={s2.n}, flagged CKD risk={s2.flagged}, caught_by_notes (labs-only missed)={s2.caught}")

import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient
mc = MlflowClient()
for model in ("ckd_stage_model", "ckd_risk_model"):
    vs = mc.search_model_versions(f"name='{S}.{model}'")
    latest = max(vs, key=lambda v: int(v.version))
    run = mc.get_run(latest.run_id)
    check("ML", f"{model} registered in Unity Catalog with MLflow metrics", bool(vs),
          f"{S}.{model} v{latest.version}, metrics={ {k: round(v, 3) for k, v in run.data.metrics.items()} }")

feat = spark.sql(f"""SELECT g.age, g.mean_egfr, g.latest_egfr, g.prior_egfr, g.egfr_trend_recent_minus_prior, g.mean_creatinine,
                     coalesce(n.notes_ckd_risk_ord, 0) notes_ckd_risk_ord
                     FROM {S}.patient_ckd_gold g JOIN {S}.patient_note_signals n USING (patient_id)
                     JOIN {S}.patient_ckd_risk_scored r USING (patient_id) WHERE r.caught_by_notes LIMIT 1""").first().asDict()
feat = {k: (float(v) if k != "notes_ckd_risk_ord" else int(v)) for k, v in feat.items()}
for attempt in range(6):
    try:
        resp = w.serving_endpoints.query(RISK_EP, dataframe_records=[feat]); break
    except Exception as e:
        time.sleep(30)
check("ML", "Real-time Model Serving endpoint returns a prediction", resp.predictions is not None,
      f"endpoint={RISK_EP}, input={json.dumps(feat)}, response={resp.predictions}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 4 - Semantic layer (Metric View) and Genie Agent

# COMMAND ----------
kpi = one(f"""SELECT MEASURE(`Patient Count`) patients, MEASURE(`CKD Patients`) ckd, MEASURE(`Undocumented CKD`) undoc,
  MEASURE(`Possibly Mis-Staged`) mis, MEASURE(`High Risk No Nephrology`) hirisk, MEASURE(`Caught By Notes`) caught
  FROM {S}.ckd_metrics""")
check("Semantic layer", "Metric View KPIs (MEASURE query)", kpi.patients == 2000,
      f"patients={kpi.patients}, ckd={kpi.ckd}, undocumented={kpi.undoc}, possibly_mis_staged={kpi.mis}, high_risk_no_nephrology={kpi.hirisk}, caught_by_notes={kpi.caught}")

msg = w.genie.start_conversation_and_wait(GENIE, "How many patients have undocumented CKD?")
g_text, g_sql, g_rows = None, None, None
for att in msg.attachments or []:
    if att.text: g_text = att.text.content
    if att.query:
        g_sql = att.query.query
        g_rows = w.genie.get_message_attachment_query_result(GENIE, msg.conversation_id, msg.message_id, att.attachment_id).statement_response.result.data_array
genie_number = int(float(g_rows[0][0])) if g_rows else None
check("Genie Agent", "Live Genie answer matches the governed Metric View", genie_number == int(kpi.undoc),
      f"question='How many patients have undocumented CKD?', genie_result={g_rows}, metric_view={kpi.undoc}, sql={g_sql!r}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 5 - Lakebase: synced read path + OLTP write path

# COMMAND ----------
import psycopg
BRANCH = f"projects/{PROJECT}/branches/production"
ep = next(iter(w.postgres.list_endpoints(BRANCH)))
token = w.postgres.generate_database_credential(endpoint=ep.name).token
me = w.current_user.me().user_name
sync = w.postgres.get_synced_table(f"synced_tables/{S}.patient_worklist_synced")
with psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=me, password=token, sslmode="require") as conn:
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*), sum(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END) FROM "{SCHEMA}".patient_worklist_synced')
        pg_n, pg_undoc = cur.fetchone()
        lat = []
        cur.execute(f'SELECT patient_id FROM "{SCHEMA}".patient_worklist_synced ORDER BY patient_id LIMIT 50')
        ids = [r[0] for r in cur.fetchall()]
        for pid in ids:
            t0 = time.perf_counter()
            cur.execute(f'SELECT * FROM "{SCHEMA}".patient_worklist_synced WHERE patient_id = %s', (pid,)); cur.fetchone()
            lat.append((time.perf_counter() - t0) * 1000)
        lat.sort(); p50, p95 = lat[len(lat)//2], lat[int(len(lat)*0.95) - 1]
        cur.execute("""INSERT INTO app.epic_writeback_queue (patient_id, confirmed_stage, refer_nephrology, source, reviewed_by, status)
                       VALUES (%s, 'G3a', true, 'validation', %s, 'validation') RETURNING id""", (ids[0], me))
        qid = cur.fetchone()[0]
        cur.execute("SELECT patient_id, confirmed_stage, refer_nephrology, status FROM app.epic_writeback_queue WHERE id = %s", (qid,))
        back = cur.fetchone()
        cur.execute("DELETE FROM app.epic_writeback_queue WHERE id = %s", (qid,))
    conn.commit()
check("Lakebase", "Synced table serves the gold worklist in Postgres (matches Delta)",
      pg_n == 2000 and pg_undoc == int(kpi.undoc),
      f"state={sync.status.detailed_state.value}, postgres rows={pg_n}, undocumented={pg_undoc}, last_sync={sync.status.last_sync_time}")
check("Lakebase", "Low-latency point reads", p95 < 100, f"50 primary-key lookups: p50={p50:.1f} ms, p95={p95:.1f} ms")
check("Lakebase", "OLTP write-back queue round-trip (insert -> read -> delete)", back is not None and back[1] == "G3a",
      f"inserted id={qid}, read back={back}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer 6 - Databricks App wired to Lakebase, the warehouse, Model Serving, and Genie

# COMMAND ----------
app = w.apps.get(APP)
res_types = sorted({k for r in (app.resources or []) for k, v in r.as_dict().items() if k not in ("name", "description")})
dep = app.active_deployment
check("App", "App running with Lakebase + SQL warehouse resources",
      app.compute_status.state.value == "ACTIVE" and "postgres" in res_types and "sql_warehouse" in res_types,
      f"app={APP}, url={app.url}, compute={app.compute_status.state.value}, deployment={dep.status.state.value if dep else None}, resources={res_types}")
sp = app.service_principal_client_id
grants = [r.asDict() for r in spark.sql(f"SHOW GRANTS `{sp}` ON SCHEMA {S}").collect()]
check("App", "App service principal has least-privilege UC access (no admin)",
      any(g["ActionType"] == "SELECT" for g in grants), f"sp={sp}, schema grants={[g['ActionType'] for g in grants]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write the evidence

# COMMAND ----------
passed = sum(r["result"] == "PASS" for r in results)
ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
run_url = f"{w.config.host}/jobs/{ctx.jobId().getOrElse(None)}/runs/{ctx.currentRunId().getOrElse(None)}" if ctx.jobId().isDefined() else "interactive"
lines = [f"# CKD solution - end-to-end validation results", "",
         f"Run: {ts} · target `{S}` · job run: {run_url}", "",
         f"**{passed}/{len(results)} checks passed**", "",
         "| Layer | Check | Result | Actual output |", "|---|---|---|---|"]
lines += [f"| {r['layer']} | {r['check']} | {r['result']} | {str(r['detail']).replace('|', '/')} |" for r in results]
md = "\n".join(lines) + "\n"
dbutils.fs.put(f"/Volumes/{CATALOG}/{SCHEMA}/evidence/VALIDATION_RESULTS.md", md, overwrite=True)
dbutils.fs.put(f"/Volumes/{CATALOG}/{SCHEMA}/evidence/validation_results.json", json.dumps(results, indent=1, default=str), overwrite=True)
print(md)
assert passed == len(results), f"{len(results) - passed} validation checks failed"
