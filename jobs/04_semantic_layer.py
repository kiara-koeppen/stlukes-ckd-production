# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Semantic layer: unified analytics view, governed Metric View, serving-ready worklist
# MAGIC
# MAGIC * `patient_ckd_analytics` joins gold flags + the AI notes signal + both ML models' scores, one row
# MAGIC   per patient.
# MAGIC * `ckd_metrics` is a Unity Catalog **Metric View**, so Genie, dashboards, and SQL share one
# MAGIC   definition of every KPI (CKD, undocumented, mis-staged, high risk, caught by notes).
# MAGIC * `patient_worklist` is a Delta table (PK + change data feed) that **Lakebase** syncs into Postgres
# MAGIC   for the App's low-latency worklist reads (task 05).

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
S = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unified per-patient analytics view

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {S}.patient_ckd_analytics
COMMENT 'Unified per-patient analytics: KDIGO lab stage + documented comparison + risk flags + AI notes CKD-risk + ML stage suggestion + fusion (labs+notes) CKD-risk incl. caught_by_notes.' AS
SELECT g.*,
       s.suggested_stage, s.confidence AS model_confidence,
       n.notes_ckd_risk, n.notes_ckd_risk_ord, n.notes_risk_flag, n.notes_indicators,
       r.ckd_risk_score, r.ckd_risk_pred, r.ckd_risk_labs_only, r.caught_by_notes,
       (s.suggested_stage IS NOT NULL AND s.suggested_stage <> coalesce(g.documented_ckd, 'None')) AS model_vs_documented_differs
FROM {S}.patient_ckd_gold g
LEFT JOIN {S}.patient_ckd_scored s USING (patient_id)
LEFT JOIN {S}.patient_note_signals n USING (patient_id)
LEFT JOIN {S}.patient_ckd_risk_scored r USING (patient_id)""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Governed Metric View

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {S}.ckd_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: {S}.patient_ckd_analytics
comment: Governed CKD KPIs for the identification & risk-flagging solution (labs + AI notes fusion).
dimensions:
  - name: Lab KDIGO Stage
    expr: lab_kdigo_stage
  - name: Suggested Stage
    expr: suggested_stage
  - name: Notes CKD Risk
    expr: notes_ckd_risk
  - name: Sex
    expr: sex
  - name: Provider
    expr: assigned_provider_name
  - name: Albuminuria
    expr: albuminuria_cat
measures:
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: CKD Patients
    expr: SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END)
  - name: Undocumented CKD
    expr: SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END)
  - name: Possibly Mis-Staged
    expr: SUM(CASE WHEN possibly_mis_staged THEN 1 ELSE 0 END)
  - name: High Risk No Nephrology
    expr: SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END)
  - name: Advanced Not On Protective Meds
    expr: SUM(CASE WHEN advanced_not_on_protective_meds THEN 1 ELSE 0 END)
  - name: Rapid eGFR Decline
    expr: SUM(CASE WHEN rapid_egfr_decline THEN 1 ELSE 0 END)
  - name: Notes Flagged At-Risk
    expr: SUM(CASE WHEN notes_risk_flag THEN 1 ELSE 0 END)
  - name: Caught By Notes
    expr: SUM(CASE WHEN caught_by_notes THEN 1 ELSE 0 END)
  - name: Avg CKD Risk Score
    expr: AVG(ckd_risk_score)
  - name: Avg Model Confidence
    expr: AVG(model_confidence)
$$""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Serving-ready worklist (source for the Lakebase synced table)
# MAGIC Only the columns the App's worklist and KPI tiles need. Primary key + change data feed let the
# MAGIC Lakebase sync run in TRIGGERED mode (incremental refresh after each job run).

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.patient_worklist (
  patient_id STRING NOT NULL, age INT, sex STRING, lab_kdigo_stage STRING, documented_ckd STRING,
  suggested_stage STRING, model_confidence DOUBLE, notes_ckd_risk STRING, ckd_risk_score DOUBLE,
  mean_egfr DOUBLE, sees_nephrology BOOLEAN, key_meds STRING,
  is_ckd BOOLEAN, undocumented_ckd BOOLEAN, possibly_mis_staged BOOLEAN, high_risk_no_nephrology BOOLEAN,
  advanced_not_on_protective_meds BOOLEAN, rapid_egfr_decline BOOLEAN, notes_risk_flag BOOLEAN, caught_by_notes BOOLEAN,
  CONSTRAINT patient_worklist_pk PRIMARY KEY (patient_id)
)
COMMENT 'Clinician worklist: one row per patient with flags + model scores. Synced to Lakebase Postgres for low-latency App reads.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
spark.sql(f"""
INSERT OVERWRITE {S}.patient_worklist
SELECT patient_id, age, sex, lab_kdigo_stage, documented_ckd, suggested_stage, CAST(model_confidence AS DOUBLE),
       notes_ckd_risk, CAST(ckd_risk_score AS DOUBLE), mean_egfr, sees_nephrology, key_meds,
       is_ckd, undocumented_ckd, possibly_mis_staged, high_risk_no_nephrology,
       advanced_not_on_protective_meds, rapid_egfr_decline,
       coalesce(notes_risk_flag, false), coalesce(caught_by_notes, false)
FROM {S}.patient_ckd_analytics""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execution evidence: the headline KPIs through the Metric View

# COMMAND ----------
display(spark.sql(f"""SELECT MEASURE(`Patient Count`) AS patients, MEASURE(`CKD Patients`) AS ckd,
  MEASURE(`Undocumented CKD`) AS undocumented, MEASURE(`Possibly Mis-Staged`) AS mis_staged,
  MEASURE(`High Risk No Nephrology`) AS high_risk_no_nephro, MEASURE(`Caught By Notes`) AS caught_by_notes
FROM {S}.ckd_metrics"""))
