# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate the synthetic raw "Epic extract" into a landing volume
# MAGIC
# MAGIC **What this does:** builds three raw files (patients, lab events, clinical notes) shaped like an
# MAGIC Epic Clarity extract and drops them as CSV into a Unity Catalog **landing volume**. The Lakeflow
# MAGIC pipeline (`pipeline/ckd_lakeflow_pipeline.sql`) then ingests them with Auto Loader.
# MAGIC
# MAGIC **Why a generator:** there is no Epic in this environment and no real PHI may be used. The source is
# MAGIC the synthetic patient registry built by `00_generate_patients.py` (modeled on St. Luke's de-identified
# MAGIC sample shape). Everything below is deterministic, so every run produces the same story:
# MAGIC * long-form lab events (3 outpatient eGFR + creatinine per patient) plus ~10% spurious **inpatient**
# MAGIC   eGFRs that the pipeline must exclude (KDIGO ascertainment uses outpatient labs only)
# MAGIC * an **occult CKD cohort** (normal structured labs, true CKD evidenced only in the notes)
# MAGIC * outpatient notes with **indirect CKD breadcrumbs** that never say "CKD/kidney/renal"
# MAGIC
# MAGIC In production this notebook is replaced by **Lakeflow Connect** (SQL Server connector off Clarity)
# MAGIC or HL7/FHIR feeds landing into the same volume / bronze tables.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters
# MAGIC All locations are widgets so the bundle can retarget catalog/schema per environment.

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SOURCE = f"{CATALOG}.{SCHEMA}.source_patient_registry"   # written by 00_generate_patients.py
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/raw_landing"
print("target:", f"{CATALOG}.{SCHEMA}", "| source:", SOURCE, "| landing:", LANDING)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema, landing volume, and the PHI mask function
# MAGIC The mask function must exist before the pipeline runs, because the bronze/gold table definitions
# MAGIC attach it to the `mrn` column.

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'St. Luke''s CKD identification & risk flagging - FE Bar end-to-end build (Lakeflow -> UC -> ML/GenAI -> Lakebase -> Genie -> App).'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_landing COMMENT 'Landing zone for the raw synthetic Epic-shaped extract (CSV). Ingested by the Lakeflow pipeline via Auto Loader.'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.evidence COMMENT 'Text execution evidence written by the validation task.'")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.mask_mrn(m STRING)
  RETURNS STRING
  COMMENT 'Column mask for the synthetic MRN (PHI stand-in): only members of ckd_phi_readers see the value.'
  RETURN CASE WHEN is_account_group_member('ckd_phi_readers') THEN m ELSE 'MRN-***-****' END""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab events (long form, as Epic emits them)
# MAGIC Unpivot the 3 outpatient eGFR/creatinine draws, then add a spurious inpatient eGFR (AKI during a
# MAGIC stay) for every 10th patient. The pipeline's `outpatient_only` expectation drops those rows.

# COMMAND ----------
labs = spark.sql(f"""
WITH unpivoted AS (
  SELECT patient_id, 'egfr' AS analyte, 1 AS seq, CAST(gfr_1 AS DOUBLE) AS value FROM {SOURCE}
  UNION ALL SELECT patient_id,'egfr',2,CAST(gfr_2 AS DOUBLE) FROM {SOURCE}
  UNION ALL SELECT patient_id,'egfr',3,CAST(gfr_3 AS DOUBLE) FROM {SOURCE}
  UNION ALL SELECT patient_id,'creatinine',1,CAST(creatinine_1 AS DOUBLE) FROM {SOURCE}
  UNION ALL SELECT patient_id,'creatinine',2,CAST(creatinine_2 AS DOUBLE) FROM {SOURCE}
  UNION ALL SELECT patient_id,'creatinine',3,CAST(creatinine_3 AS DOUBLE) FROM {SOURCE}
)
SELECT patient_id, analyte, seq, value, 'outpatient' AS encounter_type,
       add_months(current_date(), -1 * seq) AS measured_date
FROM unpivoted
UNION ALL
SELECT patient_id, 'egfr', 0, 12.0, 'inpatient', add_months(current_date(), -2)
FROM {SOURCE} WHERE patient_num % 10 = 0
""")
labs.createOrReplaceTempView("raw_labs")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Patients, including the occult CKD cohort
# MAGIC Occult cohort = no CKD per the registry, lab-derived KDIGO G1 (mean outpatient eGFR >= 90), and
# MAGIC every 5th patient. Their clinician-ascertained truth is flipped to undocumented early CKD while the
# MAGIC structured labs stay "normal", so only the notes reveal them. A synthetic MRN is added as the PHI
# MAGIC field that Unity Catalog masks.

# COMMAND ----------
patients = spark.sql(f"""
WITH lab_stage AS (
  SELECT patient_id, avg(value) AS mean_egfr
  FROM raw_labs WHERE analyte = 'egfr' AND encounter_type = 'outpatient' GROUP BY patient_id
),
occult AS (
  SELECT r.patient_id FROM {SOURCE} r JOIN lab_stage l USING (patient_id)
  WHERE r.has_ckd = false AND l.mean_egfr >= 90 AND (r.patient_num % 5) = 0
)
SELECT r.patient_id, r.patient_num,
       concat('MRN', lpad(cast(r.patient_num AS string), 8, '0')) AS mrn,
       r.sex, r.dob, r.age, r.assigned_provider_name, r.chronic_conditions, r.chronic_condition_count,
       r.ckd_in_problem_list,
       CASE WHEN o.patient_id IS NOT NULL THEN 'None' ELSE r.documented_ckd END AS documented_ckd,
       r.microalbumin_value, r.microalbumin_category, r.microalbumin_date,
       (r.has_ckd OR o.patient_id IS NOT NULL) AS has_ckd,
       CASE WHEN o.patient_id IS NOT NULL THEN '1' ELSE r.actual_ckd_stage END AS actual_ckd_stage,
       r.sees_nephrology, r.key_meds
FROM {SOURCE} r LEFT JOIN occult o USING (patient_id)
""")
patients.createOrReplaceTempView("raw_patients")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Clinical notes with indirect CKD breadcrumbs
# MAGIC Tiers create realistic signal and noise: strong breadcrumbs for borderline CKD (eGFR >= 60, where
# MAGIC labs can't decide), sparser for clear CKD, and mild non-specific symptoms for ~18% of non-CKD
# MAGIC patients. Notes never say CKD/kidney/renal/nephrology, so the AI step must infer risk.

# COMMAND ----------
notes = spark.sql("""
WITH g AS (
  SELECT p.patient_id, p.patient_num, p.age, p.sex, p.has_ckd,
         (upper(coalesce(p.chronic_conditions,'')) LIKE '%DIABET%')   AS dm,
         (upper(coalesce(p.chronic_conditions,'')) LIKE '%HYPERTENS%') AS htn,
         (l.mean_egfr >= 60) AS borderline,
         (p.patient_num*7 + 13) % 100 AS rnd
  FROM raw_patients p
  JOIN (SELECT patient_id, avg(value) AS mean_egfr FROM raw_labs
        WHERE analyte='egfr' AND encounter_type='outpatient' GROUP BY patient_id) l USING (patient_id)
),
tiered AS (
  SELECT *,
    CASE
      WHEN has_ckd AND borderline THEN (CASE WHEN rnd < 90 THEN 'strong' ELSE 'neutral' END)
      WHEN has_ckd AND NOT borderline THEN (CASE WHEN rnd < 62 THEN 'strong' WHEN rnd < 80 THEN 'mild' ELSE 'neutral' END)
      ELSE (CASE WHEN rnd < 82 THEN 'neutral' ELSE 'mild' END)
    END AS tier
  FROM g
),
parts AS (
  SELECT patient_id, patient_num, tier,
    element_at(array(
      concat(cast(age as string),'-year-old ', case when sex='F' then 'woman' else 'man' end,' here for routine follow-up. '),
      concat('Established patient, ', cast(age as string),' y/o, seen for ongoing management. '),
      'Patient returns for scheduled follow-up. ',
      concat(cast(age as string),' y/o ', case when sex='F' then 'female' else 'male' end,', interval visit. ')), 1 + (patient_num % 4)) AS stem,
    CASE tier
      WHEN 'strong' THEN element_at(array(
        'Reports increasing fatigue and swelling in both ankles over recent months. ',
        'Mentions his urine has looked foamy lately with puffiness around the eyes in the mornings. ',
        'Notes waking several times a night to urinate along with persistent tiredness. ',
        'Complains of poor appetite, mild nausea, and lower-leg swelling. '), 1 + (patient_num % 4))
      WHEN 'mild' THEN element_at(array(
        'Reports feeling a bit more tired than usual lately. ',
        'Notes occasional mild ankle swelling after long days on his feet. ',
        'Mentions some fatigue, attributes it to work stress. '), 1 + (patient_num % 3))
      ELSE element_at(array(
        'No new complaints; feeling well overall. ','Here for seasonal allergy symptoms, otherwise well. ',
        'Doing well, no acute concerns today. ','Follow-up for hyperlipidemia; tolerating therapy well. '), 1 + (patient_num % 4)) END AS symptom,
    CASE tier
      WHEN 'strong' THEN concat(
        element_at(array('Serum creatinine has drifted up from prior values; ','Repeat panel shows a modest rise in creatinine over the past year; ',
          'Urinalysis today shows trace-to-moderate protein; ','Dipstick positive for protein on two recent visits; '), 1 + (patient_num % 4)),
        case when htn then 'blood pressure remains difficult to control despite multiple agents. ' else 'will monitor closely. ' end,
        case when dm then 'Glycemic control remains poor (A1c elevated), counseled accordingly. ' else '' end)
      WHEN 'mild' THEN 'Vitals and routine labs largely stable today; will keep an eye on trends. '
      ELSE 'Vitals stable and labs within normal limits today. ' END AS objective,
    CASE tier
      WHEN 'strong' THEN 'Plan: repeat labs including a urine protein quantification, recheck in 6-8 weeks, reinforce BP and dietary measures. '
      WHEN 'mild' THEN 'Plan: continue current care, recheck at next routine visit. '
      ELSE 'Plan: continue current care, routine health maintenance. ' END AS plan
  FROM tiered
)
SELECT patient_id, concat('NOTE-', patient_id) AS note_id, add_months(current_date(), -1) AS note_date,
  element_at(array('Dr. Alvarez','Dr. Chen','Dr. Okafor','Dr. Patel','Dr. Romano','Dr. Nguyen'), 1 + (patient_num % 6)) AS author,
  'Outpatient progress note' AS note_type, concat(stem, symptom, objective, plan) AS note_text
FROM parts
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Land the files
# MAGIC One folder per feed, overwritten each run (the pipeline's Auto Loader checkpoint picks up new files;
# MAGIC run the pipeline with a full refresh after regenerating).

# COMMAND ----------
for name, df in [("patients", patients), ("lab_events", labs), ("clinical_notes", notes)]:
    (df.coalesce(1).write.mode("overwrite").option("header", True).option("quoteAll", True)
       .csv(f"{LANDING}/{name}"))
    print(f"landed {name}: {df.count()} rows -> {LANDING}/{name}")
print("occult cohort size:", spark.sql("SELECT count(*) FROM raw_patients WHERE actual_ckd_stage='1' AND documented_ckd='None' AND has_ckd").first()[0])
