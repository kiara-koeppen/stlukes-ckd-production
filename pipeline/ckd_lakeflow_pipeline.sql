-- =====================================================================================================
-- CKD Identification & Risk Flagging - Lakeflow Spark Declarative Pipeline (bronze -> silver -> gold)
-- =====================================================================================================
-- Ingest: Auto Loader (STREAM read_files) over the raw Epic-shaped CSV extract in the landing volume.
-- Govern: Unity Catalog comments, informational PK/FK, and a column mask on the synthetic MRN (PHI).
-- Quality: expectations make the clinical rules visible as pipeline data-quality metrics, e.g. the
--          KDIGO "outpatient labs only" rule drops the spurious inpatient eGFRs in silver.
-- Pipeline configuration supplies ${catalog} / ${schema}; tables land in that catalog.schema.
-- =====================================================================================================

-- ---------------------------------------------------------------------------------------------------
-- BRONZE: raw feeds, typed on read, one streaming table per feed
-- ---------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_patient (
  patient_id STRING NOT NULL COMMENT 'De-identified patient key (drives the UI; not PHI).',
  patient_num INT COMMENT 'Numeric patient sequence from the source extract.',
  mrn STRING MASK ${catalog}.${schema}.mask_mrn COMMENT 'Synthetic MRN (PHI stand-in) - masked unless the reader is in ckd_phi_readers.',
  sex STRING COMMENT 'Patient sex (F/M).',
  dob DATE COMMENT 'Date of birth (synthetic).',
  age INT COMMENT 'Age in years.',
  assigned_provider_name STRING COMMENT 'Assigned primary care provider.',
  chronic_conditions STRING COMMENT 'Problem-list chronic conditions (free text, comma separated).',
  chronic_condition_count INT COMMENT 'Number of chronic conditions on the problem list.',
  ckd_in_problem_list BOOLEAN COMMENT 'TRUE when CKD appears on the EHR problem list.',
  documented_ckd STRING COMMENT 'CKD stage as documented in the EHR (None when not documented).',
  microalbumin_value DECIMAL(6,1) COMMENT 'Most recent urine microalbumin value.',
  microalbumin_category STRING COMMENT 'KDIGO albuminuria category A1/A2/A3.',
  microalbumin_date DATE COMMENT 'Date of the microalbumin result.',
  has_ckd BOOLEAN COMMENT 'Clinician-ascertained ground truth: patient has CKD (used to train and evaluate models).',
  actual_ckd_stage STRING COMMENT 'Clinician-ascertained CKD stage (ground truth label).',
  sees_nephrology BOOLEAN COMMENT 'TRUE when the patient is followed by nephrology.',
  key_meds STRING COMMENT 'Kidney-protective medications (SGLT2 / GLP-1 / ACE / ARB), free text.',
  CONSTRAINT valid_age EXPECT (age BETWEEN 0 AND 120),
  CONSTRAINT valid_sex EXPECT (sex IN ('F', 'M')),
  CONSTRAINT bronze_patient_pk PRIMARY KEY (patient_id)
)
COMMENT 'Bronze: one row per patient from the raw Epic-shaped extract (demographics, problem list, documented CKD, ground truth, meds).'
AS SELECT patient_id, patient_num, mrn, sex, dob, age, assigned_provider_name, chronic_conditions,
          chronic_condition_count, ckd_in_problem_list, documented_ckd, microalbumin_value,
          microalbumin_category, microalbumin_date, has_ckd, actual_ckd_stage, sees_nephrology, key_meds
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_landing/patients/',
  format => 'csv', header => true,
  schema => 'patient_id STRING, patient_num INT, mrn STRING, sex STRING, dob DATE, age INT, assigned_provider_name STRING, chronic_conditions STRING, chronic_condition_count INT, ckd_in_problem_list BOOLEAN, documented_ckd STRING, microalbumin_value DECIMAL(6,1), microalbumin_category STRING, microalbumin_date DATE, has_ckd BOOLEAN, actual_ckd_stage STRING, sees_nephrology BOOLEAN, key_meds STRING'
);

CREATE OR REFRESH STREAMING TABLE bronze_lab_events (
  CONSTRAINT has_patient EXPECT (patient_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_analyte EXPECT (analyte IN ('egfr', 'creatinine')) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: long-form lab events (eGFR, creatinine) tagged outpatient/inpatient, as an Epic lab feed emits them.'
AS SELECT patient_id, analyte, seq, value, encounter_type, measured_date
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_landing/lab_events/',
  format => 'csv', header => true,
  schema => 'patient_id STRING, analyte STRING, seq INT, value DOUBLE, encounter_type STRING, measured_date DATE'
);

CREATE OR REFRESH STREAMING TABLE bronze_clinical_notes (
  CONSTRAINT has_note_text EXPECT (note_text IS NOT NULL AND length(note_text) > 20) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: free-text outpatient progress notes. Carry indirect CKD breadcrumbs that the AI note-extraction step reads.'
AS SELECT patient_id, note_id, note_date, author, note_type, note_text
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_landing/clinical_notes/',
  format => 'csv', header => true, multiLine => true, escape => '"',
  schema => 'patient_id STRING, note_id STRING, note_date DATE, author STRING, note_type STRING, note_text STRING'
);

-- ---------------------------------------------------------------------------------------------------
-- SILVER: clinically valid labs and per-patient lab summary
-- ---------------------------------------------------------------------------------------------------
-- KDIGO ascertainment uses outpatient labs only; the expectation drops inpatient draws (e.g. AKI during a
-- hospital stay) and reports how many were removed in the pipeline's data-quality metrics.
CREATE OR REFRESH STREAMING TABLE silver_lab_events (
  CONSTRAINT outpatient_only EXPECT (encounter_type = 'outpatient') ON VIOLATION DROP ROW,
  CONSTRAINT plausible_value EXPECT (value > 0 AND value < 250) ON VIOLATION DROP ROW
)
COMMENT 'Silver: outpatient-only, plausible lab events (inpatient draws dropped by the outpatient_only expectation).'
AS SELECT * FROM STREAM(bronze_lab_events);

CREATE OR REFRESH MATERIALIZED VIEW silver_patient_labs
COMMENT 'Silver: per-patient outpatient lab summary - mean eGFR/creatinine over recent draws, latest vs prior eGFR, and trend.'
AS
WITH egfr AS (
  SELECT patient_id,
         round(avg(value), 1) AS mean_egfr,
         max(CASE WHEN seq = 1 THEN value END) AS latest_egfr,
         max(CASE WHEN seq = 3 THEN value END) AS prior_egfr,
         count(*) AS n_outpatient_egfr
  FROM silver_lab_events WHERE analyte = 'egfr'
  GROUP BY patient_id
),
creat AS (
  SELECT patient_id, round(avg(value), 2) AS mean_creatinine
  FROM silver_lab_events WHERE analyte = 'creatinine'
  GROUP BY patient_id
)
SELECT e.patient_id, e.mean_egfr, e.latest_egfr, e.prior_egfr, e.n_outpatient_egfr,
       c.mean_creatinine,
       (e.latest_egfr - e.prior_egfr) AS egfr_trend_recent_minus_prior
FROM egfr e JOIN creat c USING (patient_id);

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
