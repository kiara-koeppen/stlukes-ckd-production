# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Gen AI: infer CKD risk from clinical notes (`ai_query`)
# MAGIC
# MAGIC **What:** an LLM reads every outpatient note in `bronze_clinical_notes` and infers a CKD-risk level
# MAGIC (none / low / moderate / high) plus the indirect indicators it found. The notes never say "CKD",
# MAGIC so the model has to reason from breadcrumbs (foamy urine, edema, rising creatinine, uncontrolled
# MAGIC HTN/DM, nocturia).
# MAGIC
# MAGIC **Why:** at eGFR >= 60, CKD needs a damage marker (proteinuria) that often lives only in the notes.
# MAGIC The patient-level rollup becomes a feature for the fusion (labs + notes) risk model in task 03.
# MAGIC
# MAGIC In production, front the model with **Unity AI Gateway** (rate limits, cost tracking, PII guardrails).

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Foundation model endpoint")
CATALOG, SCHEMA, LLM = (dbutils.widgets.get(k) for k in ("catalog", "schema", "llm_endpoint"))
S = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Per-note inferred CKD risk
# MAGIC The prompt asks for minified JSON only; `regexp_extract` + `from_json` turn the response into a typed
# MAGIC struct, which is more robust than trusting the model to emit clean JSON every time.

# COMMAND ----------
PROMPT = ("You are a nephrology-aware clinician reviewing an outpatient progress note that may NOT mention kidney "
          "disease by name. Judge ONLY from indirect evidence: proteinuria or foamy urine, ankle or periorbital edema, "
          "rising or elevated creatinine, poorly controlled hypertension, poorly controlled diabetes, nocturia, fatigue, "
          "nausea or poor appetite. Assess this patient''s risk of having undiagnosed chronic kidney disease. Respond with "
          "ONLY minified JSON and nothing else: "
          '{"ckd_risk":"none|low|moderate|high","indicators":["short phrases you found"],"rationale":"one sentence"}. Note: ')
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.note_ckd_signals
COMMENT 'AI-inferred CKD risk per note (LLM reasons from indirect breadcrumbs; the note never states CKD).' AS
SELECT patient_id, note_id, note_date,
  from_json(regexp_extract(resp, '\\\\{{.*\\\\}}', 0),
            'STRUCT<ckd_risk:STRING, indicators:ARRAY<STRING>, rationale:STRING>') AS sig
FROM (
  SELECT patient_id, note_id, note_date,
         ai_query('{LLM}', concat('{PROMPT}', note_text)) AS resp
  FROM {S}.bronze_clinical_notes
)""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Patient-level rollup
# MAGIC Ordinal max risk, a boolean at-risk flag (moderate/high), and the indicators, one row per patient.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.patient_note_signals
COMMENT 'Patient-level AI note CKD-risk: ordinal max risk + at-risk flag (moderate/high) + indicators. Feeds the fusion model.' AS
WITH o AS (
  SELECT patient_id, sig,
         CASE lower(sig.ckd_risk) WHEN 'high' THEN 3 WHEN 'moderate' THEN 2 WHEN 'low' THEN 1 ELSE 0 END AS r
  FROM {S}.note_ckd_signals
)
SELECT patient_id,
  max(r) AS notes_ckd_risk_ord,
  element_at(array('none','low','moderate','high'), 1 + max(r)) AS notes_ckd_risk,
  (max(r) >= 2) AS notes_risk_flag,
  flatten(collect_list(coalesce(sig.indicators, array()))) AS notes_indicators,
  count(*) AS n_notes
FROM o GROUP BY patient_id""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execution evidence
# MAGIC Parse rate and the risk distribution, so a reviewer can see the AI step actually ran.

# COMMAND ----------
display(spark.sql(f"""SELECT count(*) AS notes, sum(CASE WHEN sig.ckd_risk IS NULL THEN 1 ELSE 0 END) AS unparsed,
  sum(CASE WHEN lower(sig.ckd_risk) IN ('moderate','high') THEN 1 ELSE 0 END) AS at_risk FROM {S}.note_ckd_signals"""))
display(spark.sql(f"SELECT notes_ckd_risk, count(*) AS patients FROM {S}.patient_note_signals GROUP BY 1 ORDER BY 1"))
