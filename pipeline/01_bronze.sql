-- =====================================================================================================
-- CKD Identification & Risk Flagging - Lakeflow Spark Declarative Pipeline (bronze -> silver -> gold)
-- =====================================================================================================
-- Ingest: Auto Loader (STREAM read_files) over the raw Epic-shaped CSV extract in the landing volume.
-- Govern: Unity Catalog comments, informational PK/FK, and a column mask on the synthetic MRN (PHI).
-- Quality: expectations make the clinical rules visible as pipeline data-quality metrics, e.g. the
--          KDIGO "outpatient labs only" rule drops the spurious inpatient eGFRs in silver.
-- Pipeline configuration supplies ${catalog} / ${schema}; tables land in that catalog.schema.
-- =====================================================================================================

-- This pipeline is split into three files (01_bronze, 02_silver, 03_gold); Lakeflow resolves the
-- dependencies between them automatically.

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
