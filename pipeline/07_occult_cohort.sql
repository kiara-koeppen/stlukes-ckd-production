-- CKD Production - OCCULT / EARLY CKD cohort (post-gold override)
-- Clinical reality (KDIGO): CKD can exist at NORMAL eGFR when a damage marker (persistent proteinuria)
-- is present — and that marker often lives only in the clinical NOTES, not a structured lab. To make the
-- "hidden in the notes" story real, we designate a cohort with clean structured labs (eGFR>=90, A1
-- albuminuria) as true, undocumented CKD. Their notes (08_breadcrumb_notes) carry the proteinuria
-- breadcrumbs; a labs-only model misses them; the fusion (labs + notes) model catches them.
-- RUN AFTER 03_gold_ckd.sql and BEFORE 08_breadcrumb_notes.sql / 05_note_extraction.sql / the model.

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.occult_ckd_cohort
COMMENT 'Occult/early CKD: normal structured labs (eGFR>=90, A1) but true CKD evidenced only by proteinuria in the notes.' AS
SELECT patient_id
FROM slhs_test1.ckd_prod.patient_ckd_gold
WHERE has_ckd = false AND lab_kdigo_stage = 'G1' AND (patient_num % 5) = 0;

-- Flip their ground truth to (undocumented, early) CKD while leaving the structured labs untouched,
-- so a labs-only view still sees "normal" and only the notes reveal them.
UPDATE slhs_test1.ckd_prod.patient_ckd_gold
SET has_ckd = true, is_ckd = true, undocumented_ckd = true, actual_ckd_stage = '1',
    documented_ckd = 'None', documented_stage_norm = NULL
WHERE patient_id IN (SELECT patient_id FROM slhs_test1.ckd_prod.occult_ckd_cohort);
