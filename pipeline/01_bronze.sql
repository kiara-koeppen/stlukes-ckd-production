-- CKD Production - BRONZE
-- Raw, Epic-shaped landing data. In production these are fed by Lakeflow Connect (SQL Server /
-- Epic Clarity); here they are seeded from the synthetic registry to mirror the real data shape.
-- Labs arrive as long-form events (that is how Epic emits them), tagged outpatient vs inpatient so
-- the silver layer can exclude inpatient labs per the KDIGO ascertainment method.

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.bronze_patient
COMMENT 'Bronze: one row per patient - demographics, problem-list/documented CKD, clinician-ascertained truth, nephrology status, key meds.' AS
SELECT patient_id, patient_num, sex, dob, age, assigned_provider_name,
       chronic_conditions, chronic_condition_count, ckd_in_problem_list, documented_ckd,
       microalbumin_value, microalbumin_category, microalbumin_date,
       has_ckd, actual_ckd_stage, sees_nephrology, key_meds
FROM slhs_test1.clinical.ckd_patient_registry;

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.bronze_lab_events
COMMENT 'Bronze: long-form lab events (eGFR, creatinine) tagged outpatient/inpatient with a measured date, mirroring an Epic lab feed.' AS
-- 3 most-recent OUTPATIENT eGFR + creatinine per patient (from the registry), as long events
WITH unpivoted AS (
  SELECT patient_id, 'egfr' AS analyte, 1 AS seq, CAST(gfr_1 AS DOUBLE) AS value FROM slhs_test1.clinical.ckd_patient_registry
  UNION ALL SELECT patient_id,'egfr',2,CAST(gfr_2 AS DOUBLE) FROM slhs_test1.clinical.ckd_patient_registry
  UNION ALL SELECT patient_id,'egfr',3,CAST(gfr_3 AS DOUBLE) FROM slhs_test1.clinical.ckd_patient_registry
  UNION ALL SELECT patient_id,'creatinine',1,CAST(creatinine_1 AS DOUBLE) FROM slhs_test1.clinical.ckd_patient_registry
  UNION ALL SELECT patient_id,'creatinine',2,CAST(creatinine_2 AS DOUBLE) FROM slhs_test1.clinical.ckd_patient_registry
  UNION ALL SELECT patient_id,'creatinine',3,CAST(creatinine_3 AS DOUBLE) FROM slhs_test1.clinical.ckd_patient_registry
)
SELECT patient_id, analyte, seq, value,
       'outpatient' AS encounter_type,
       add_months(current_date(), -1 * seq) AS measured_date
FROM unpivoted
UNION ALL
-- ~10% of patients also have a spurious INPATIENT eGFR (e.g. AKI during a hospital stay) that MUST
-- be excluded from staging; demonstrates the outpatient-only rule.
SELECT patient_id, 'egfr' AS analyte, 0 AS seq, 12.0 AS value,
       'inpatient' AS encounter_type, add_months(current_date(), -2) AS measured_date
FROM slhs_test1.clinical.ckd_patient_registry
WHERE patient_num % 10 = 0;

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.bronze_clinical_notes
COMMENT 'Bronze: free-text clinical notes (used by the AI note-extraction step for nephrology/CKD mentions).' AS
SELECT patient_id, note_id, note_date, author, note_type, note_text
FROM slhs_test1.clinical.clinical_notes;
