-- CKD Production - GOVERNANCE
-- Unity Catalog keys + comments so Genie/optimizer/BI understand the model, and an ABAC column
-- mask demonstrating PHI protection. We mask a synthetic MRN (not the de-identified patient_id that
-- drives the UI), so masking protects PHI without breaking the dashboards/app.

-- 1) Synthetic MRN (stand-in for the Epic PHI you would govern in production)
ALTER TABLE slhs_test1.ckd_prod.bronze_patient ADD COLUMN IF NOT EXISTS mrn STRING;
UPDATE slhs_test1.ckd_prod.bronze_patient SET mrn = concat('MRN', lpad(cast(patient_num AS string), 8, '0'));
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ADD COLUMN IF NOT EXISTS mrn STRING;
UPDATE slhs_test1.ckd_prod.patient_ckd_gold g
  SET mrn = concat('MRN', lpad(cast(patient_num AS string), 8, '0'));

-- 2) Keys (informational RELY) - patient-level tables keyed on patient_id
ALTER TABLE slhs_test1.ckd_prod.bronze_patient ALTER COLUMN patient_id SET NOT NULL;
ALTER TABLE slhs_test1.ckd_prod.bronze_patient DROP CONSTRAINT IF EXISTS bronze_patient_pk;
ALTER TABLE slhs_test1.ckd_prod.bronze_patient ADD CONSTRAINT bronze_patient_pk PRIMARY KEY (patient_id) RELY;
ALTER TABLE slhs_test1.ckd_prod.silver_patient_labs ALTER COLUMN patient_id SET NOT NULL;
ALTER TABLE slhs_test1.ckd_prod.silver_patient_labs DROP CONSTRAINT IF EXISTS silver_patient_labs_pk;
ALTER TABLE slhs_test1.ckd_prod.silver_patient_labs ADD CONSTRAINT silver_patient_labs_pk PRIMARY KEY (patient_id) RELY;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN patient_id SET NOT NULL;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold DROP CONSTRAINT IF EXISTS patient_ckd_gold_pk;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ADD CONSTRAINT patient_ckd_gold_pk PRIMARY KEY (patient_id) RELY;
ALTER TABLE slhs_test1.ckd_prod.silver_patient_labs DROP CONSTRAINT IF EXISTS silver_fk;
ALTER TABLE slhs_test1.ckd_prod.silver_patient_labs ADD CONSTRAINT silver_fk FOREIGN KEY (patient_id) REFERENCES slhs_test1.ckd_prod.bronze_patient (patient_id) RELY;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold DROP CONSTRAINT IF EXISTS gold_fk;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ADD CONSTRAINT gold_fk FOREIGN KEY (patient_id) REFERENCES slhs_test1.ckd_prod.bronze_patient (patient_id) RELY;

-- 3) Key column comments on gold (Genie/BI semantics)
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN lab_kdigo_stage COMMENT 'KDIGO stage derived from mean outpatient eGFR (G1..G5).';
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN documented_stage_norm COMMENT 'Documented CKD stage normalized to KDIGO (NULL if not documented).';
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN undocumented_ckd COMMENT 'TRUE: patient has CKD by labs but it is not documented in the EHR (the care gap).';
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN possibly_mis_staged COMMENT 'TRUE: documented stage differs from lab-derived KDIGO stage.';
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN high_risk_no_nephrology COMMENT 'TRUE: advanced CKD (G4/G5) and not seeing nephrology.';
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN mrn COMMENT 'Synthetic MRN (PHI) - protected by an ABAC column mask.';

-- 4) ABAC column mask: only members of ckd_phi_readers see the MRN; everyone else sees it masked.
CREATE OR REPLACE FUNCTION slhs_test1.ckd_prod.mask_mrn(m STRING)
  RETURN CASE WHEN is_account_group_member('ckd_phi_readers') THEN m ELSE 'MRN-***-****' END;
ALTER TABLE slhs_test1.ckd_prod.bronze_patient ALTER COLUMN mrn SET MASK slhs_test1.ckd_prod.mask_mrn;
ALTER TABLE slhs_test1.ckd_prod.patient_ckd_gold ALTER COLUMN mrn SET MASK slhs_test1.ckd_prod.mask_mrn;
