-- CKD Production - GOLD patient-level feature + risk-flag table
-- Derives KDIGO stage from labs (validated against clinician staging), compares to documented
-- stage, and flags care gaps / high risk. Catalog is a parameter (default slhs_test1).
-- In the SDP pipeline this becomes a MATERIALIZED VIEW; here it is a CTAS for direct verification.
--
-- KDIGO G-stage from mean outpatient eGFR (3 most-recent outpatient values, inpatient excluded
-- upstream in silver): G1>=90, G2 60-89, G3a 45-59, G3b 30-44, G4 15-29, G5 <15. eGFR>=60 without
-- albuminuria is not CKD (has_ckd captures the KDIGO markers). gfr_1 = most recent.

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.patient_ckd_gold
COMMENT 'Gold: one row per patient with lab-derived KDIGO stage, documented-stage comparison, trend features, and risk/care-gap flags for the CKD identification solution.'
AS
WITH base AS (
  SELECT
    patient_id, patient_num, sex, age, assigned_provider_name,
    documented_ckd, actual_ckd_stage, has_ckd, sees_nephrology,
    microalbumin_category, microalbumin_value, key_meds,
    creatinine_1, creatinine_2, creatinine_3, gfr_1, gfr_2, gfr_3,
    round((gfr_1 + gfr_2 + gfr_3) / 3.0, 1) AS mean_egfr,
    round((creatinine_1 + creatinine_2 + creatinine_3) / 3.0, 2) AS mean_creatinine,
    (gfr_1 - gfr_3) AS egfr_trend_recent_minus_prior   -- negative = declining kidney function
  FROM slhs_test1.clinical.ckd_patient_registry
),
staged AS (
  SELECT *,
    CASE
      WHEN mean_egfr >= 90 THEN 'G1' WHEN mean_egfr >= 60 THEN 'G2'
      WHEN mean_egfr >= 45 THEN 'G3a' WHEN mean_egfr >= 30 THEN 'G3b'
      WHEN mean_egfr >= 15 THEN 'G4' ELSE 'G5'
    END AS lab_kdigo_stage,
    CASE WHEN microalbumin_category IN ('A2','A3') THEN microalbumin_category ELSE coalesce(microalbumin_category,'A1') END AS albuminuria_cat,
    (key_meds IS NOT NULL AND trim(key_meds) <> '') AS on_kidney_protective_meds  -- SGLT2i / GLP-1
  FROM base
)
SELECT
  s.*,
  -- normalized documented stage (e.g. 'CKD 3a' -> 'G3a', 'None'/'No' -> NULL) for comparison
  CASE
    WHEN upper(documented_ckd) IN ('NO','NONE') OR documented_ckd IS NULL THEN NULL
    ELSE 'G' || regexp_replace(upper(regexp_extract(documented_ckd, '([0-9][a-bA-B]?)', 1)), ' ', '')
  END AS documented_stage_norm,
  -- CARE-GAP + RISK FLAGS (the clinical payoff)
  has_ckd AS is_ckd,
  (has_ckd AND (upper(coalesce(documented_ckd,'NONE')) IN ('NO','NONE'))) AS undocumented_ckd,
  (has_ckd AND upper(coalesce(documented_ckd,'NONE')) NOT IN ('NO','NONE')
     AND 'G' || regexp_replace(upper(regexp_extract(documented_ckd,'([0-9][a-bA-B]?)',1)),' ','') <> lab_kdigo_stage) AS possibly_mis_staged,
  (lab_kdigo_stage IN ('G4','G5') AND NOT sees_nephrology) AS high_risk_no_nephrology,
  (has_ckd AND lab_kdigo_stage IN ('G3b','G4','G5') AND NOT s.on_kidney_protective_meds) AS advanced_not_on_protective_meds,
  (egfr_trend_recent_minus_prior <= -5) AS rapid_egfr_decline
FROM staged s;
