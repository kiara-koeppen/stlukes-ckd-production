-- CKD Production - GOLD patient-level feature + risk-flag table (flows from silver + bronze)
-- Derives KDIGO stage from the mean OUTPATIENT eGFR (silver), compares to the documented stage,
-- and flags care gaps / high risk. KDIGO G-stage: G1>=90, G2 60-89, G3a 45-59, G3b 30-44,
-- G4 15-29, G5 <15 (eGFR>=60 without albuminuria is not CKD; has_ckd carries the KDIGO markers).

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.patient_ckd_gold
COMMENT 'Gold: one row per patient with lab-derived KDIGO stage, documented-vs-lab comparison, trend features, and risk/care-gap flags for CKD identification.' AS
WITH j AS (
  SELECT p.patient_id, p.patient_num, p.sex, p.age, p.assigned_provider_name,
         p.documented_ckd, p.actual_ckd_stage, p.has_ckd, p.sees_nephrology,
         p.microalbumin_category, p.microalbumin_value, p.key_meds,
         l.mean_egfr, l.mean_creatinine, l.latest_egfr, l.prior_egfr, l.egfr_trend_recent_minus_prior
  FROM slhs_test1.ckd_prod.bronze_patient p
  JOIN slhs_test1.ckd_prod.silver_patient_labs l USING (patient_id)
),
staged AS (
  SELECT *,
    CASE WHEN mean_egfr >= 90 THEN 'G1' WHEN mean_egfr >= 60 THEN 'G2'
         WHEN mean_egfr >= 45 THEN 'G3a' WHEN mean_egfr >= 30 THEN 'G3b'
         WHEN mean_egfr >= 15 THEN 'G4' ELSE 'G5' END AS lab_kdigo_stage,
    CASE WHEN microalbumin_category IN ('A2','A3') THEN microalbumin_category ELSE coalesce(microalbumin_category,'A1') END AS albuminuria_cat,
    (key_meds IS NOT NULL AND trim(key_meds) <> '') AS on_kidney_protective_meds
  FROM j
)
SELECT s.*,
  CASE WHEN upper(coalesce(documented_ckd,'NONE')) IN ('NO','NONE') THEN NULL
       ELSE 'G' || regexp_replace(upper(regexp_extract(documented_ckd,'([0-9][a-bA-B]?)',1)),' ','') END AS documented_stage_norm,
  has_ckd AS is_ckd,
  (has_ckd AND upper(coalesce(documented_ckd,'NONE')) IN ('NO','NONE')) AS undocumented_ckd,
  (has_ckd AND upper(coalesce(documented_ckd,'NONE')) NOT IN ('NO','NONE')
     AND 'G' || regexp_replace(upper(regexp_extract(documented_ckd,'([0-9][a-bA-B]?)',1)),' ','') <> lab_kdigo_stage) AS possibly_mis_staged,
  (lab_kdigo_stage IN ('G4','G5') AND NOT sees_nephrology) AS high_risk_no_nephrology,
  (has_ckd AND lab_kdigo_stage IN ('G3b','G4','G5') AND NOT s.on_kidney_protective_meds) AS advanced_not_on_protective_meds,
  (egfr_trend_recent_minus_prior <= -5) AS rapid_egfr_decline
FROM staged s;
