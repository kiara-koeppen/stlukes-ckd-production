-- CKD Identification & Risk Flagging - Lakeflow pipeline, GOLD layer (see 01_bronze.sql for the overview)
-- Pipeline configuration supplies ${catalog} / ${schema}.

-- ---------------------------------------------------------------------------------------------------
-- GOLD: one row per patient - KDIGO lab stage, documented-vs-lab comparison, and risk / care-gap flags
-- KDIGO G-stage from mean outpatient eGFR: G1>=90, G2 60-89, G3a 45-59, G3b 30-44, G4 15-29, G5 <15.
-- ---------------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW patient_ckd_gold (
  patient_id STRING NOT NULL COMMENT 'De-identified patient key.',
  patient_num INT COMMENT 'Numeric patient sequence.',
  mrn STRING MASK ${catalog}.${schema}.mask_mrn COMMENT 'Synthetic MRN (PHI) - protected by a Unity Catalog column mask.',
  sex STRING COMMENT 'Patient sex.',
  age INT COMMENT 'Age in years.',
  assigned_provider_name STRING COMMENT 'Assigned primary care provider.',
  documented_ckd STRING COMMENT 'CKD stage as documented in the EHR (None when not documented).',
  actual_ckd_stage STRING COMMENT 'Clinician-ascertained CKD stage (ground truth).',
  has_ckd BOOLEAN COMMENT 'Clinician-ascertained ground truth: patient has CKD.',
  sees_nephrology BOOLEAN COMMENT 'TRUE when followed by nephrology.',
  microalbumin_category STRING COMMENT 'Albuminuria category A1/A2/A3 from the source.',
  microalbumin_value DECIMAL(6,1) COMMENT 'Most recent urine microalbumin value.',
  key_meds STRING COMMENT 'Kidney-protective medications (free text).',
  mean_egfr DOUBLE COMMENT 'Mean of the recent outpatient eGFR draws (mL/min/1.73m2).',
  mean_creatinine DOUBLE COMMENT 'Mean of the recent outpatient creatinine draws (mg/dL).',
  latest_egfr DOUBLE COMMENT 'Most recent outpatient eGFR.',
  prior_egfr DOUBLE COMMENT 'Oldest of the recent outpatient eGFR draws.',
  egfr_trend_recent_minus_prior DOUBLE COMMENT 'Latest minus prior eGFR; negative means declining kidney function.',
  lab_kdigo_stage STRING COMMENT 'KDIGO stage derived from mean outpatient eGFR (G1..G5).',
  albuminuria_cat STRING COMMENT 'Albuminuria category (A1 when missing).',
  on_kidney_protective_meds BOOLEAN COMMENT 'TRUE when on any kidney-protective medication.',
  documented_stage_norm STRING COMMENT 'Documented CKD stage normalized to KDIGO (NULL if not documented).',
  is_ckd BOOLEAN COMMENT 'TRUE when the patient has CKD (ground truth).',
  undocumented_ckd BOOLEAN COMMENT 'TRUE: patient has CKD but it is not documented in the EHR (the care gap).',
  possibly_mis_staged BOOLEAN COMMENT 'TRUE: documented stage differs from the lab-derived KDIGO stage.',
  high_risk_no_nephrology BOOLEAN COMMENT 'TRUE: advanced CKD (G4/G5) and not seeing nephrology.',
  advanced_not_on_protective_meds BOOLEAN COMMENT 'TRUE: CKD G3b+ and not on a kidney-protective medication.',
  rapid_egfr_decline BOOLEAN COMMENT 'TRUE: eGFR dropped 5+ points across the recent draws.',
  CONSTRAINT has_lab_stage EXPECT (lab_kdigo_stage IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT patient_ckd_gold_pk PRIMARY KEY (patient_id),
  CONSTRAINT gold_patient_fk FOREIGN KEY (patient_id) REFERENCES ${catalog}.${schema}.bronze_patient (patient_id)
)
COMMENT 'Gold: one row per patient with lab-derived KDIGO stage, documented-vs-lab comparison, trend features, and risk/care-gap flags for CKD identification.'
AS
WITH j AS (
  SELECT p.patient_id, p.patient_num, p.sex, p.age, p.assigned_provider_name,
         p.documented_ckd, p.actual_ckd_stage, p.has_ckd, p.sees_nephrology,
         p.microalbumin_category, p.microalbumin_value, p.key_meds,
         l.mean_egfr, l.mean_creatinine, l.latest_egfr, l.prior_egfr, l.egfr_trend_recent_minus_prior
  FROM bronze_patient p
  JOIN silver_patient_labs l USING (patient_id)
),
staged AS (
  SELECT *,
    CASE WHEN mean_egfr >= 90 THEN 'G1' WHEN mean_egfr >= 60 THEN 'G2'
         WHEN mean_egfr >= 45 THEN 'G3a' WHEN mean_egfr >= 30 THEN 'G3b'
         WHEN mean_egfr >= 15 THEN 'G4' ELSE 'G5' END AS lab_kdigo_stage,
    CASE WHEN microalbumin_category IN ('A2','A3') THEN microalbumin_category ELSE coalesce(microalbumin_category,'A1') END AS albuminuria_cat,
    (key_meds IS NOT NULL AND trim(key_meds) <> '') AS on_kidney_protective_meds,
    CASE WHEN upper(coalesce(documented_ckd,'NONE')) IN ('NO','NONE') THEN NULL
         ELSE 'G' || regexp_replace(upper(regexp_extract(documented_ckd,'([0-9][a-bA-B]?)',1)),' ','') END AS documented_stage_norm
  FROM j
)
SELECT patient_id, patient_num,
  -- recomputed from patient_num so the masked bronze value never propagates into gold
  concat('MRN', lpad(cast(patient_num AS string), 8, '0')) AS mrn,
  sex, age, assigned_provider_name, documented_ckd, actual_ckd_stage, has_ckd, sees_nephrology,
  microalbumin_category, microalbumin_value, key_meds,
  mean_egfr, mean_creatinine, latest_egfr, prior_egfr, egfr_trend_recent_minus_prior,
  lab_kdigo_stage, albuminuria_cat, on_kidney_protective_meds, documented_stage_norm,
  has_ckd AS is_ckd,
  (has_ckd AND documented_stage_norm IS NULL) AS undocumented_ckd,
  (has_ckd AND documented_stage_norm IS NOT NULL AND documented_stage_norm <> lab_kdigo_stage) AS possibly_mis_staged,
  (lab_kdigo_stage IN ('G4','G5') AND NOT sees_nephrology) AS high_risk_no_nephrology,
  (has_ckd AND lab_kdigo_stage IN ('G3b','G4','G5') AND NOT on_kidney_protective_meds) AS advanced_not_on_protective_meds,
  (egfr_trend_recent_minus_prior <= -5) AS rapid_egfr_decline
FROM staged;
