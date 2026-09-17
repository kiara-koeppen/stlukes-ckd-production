-- CKD Production - BREADCRUMB clinical notes (replaces the bronze notes for the notes->AI->model demo)
-- Generates synthetic outpatient notes that carry INDIRECT CKD indicators (proteinuria/foamy urine,
-- edema, rising creatinine, uncontrolled HTN/DM, nocturia) but NEVER say CKD/kidney/renal/nephrology —
-- so the AI (05_note_extraction) must INFER risk. Realistic noise via tiers:
--   * STRONG breadcrumbs for BORDERLINE CKD (eGFR>=60, incl. the occult cohort) where labs can't decide
--   * sparser/neutral for some CLEAR CKD (labs catch those anyway)
--   * mild non-specific symptoms for ~18% of non-CKD (false-positive noise)
-- RUN AFTER 07_occult_cohort.sql (so occult patients are has_ckd=true) and BEFORE 05_note_extraction.sql.

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.bronze_clinical_notes
COMMENT 'Synthetic outpatient notes with INDIRECT CKD breadcrumbs + realistic noise; no explicit CKD/kidney/renal/nephrology terms. Drives the notes->AI-risk->fusion-model demo.' AS
WITH g AS (
  SELECT p.patient_id, p.patient_num, g.age, g.sex, g.has_ckd, g.lab_kdigo_stage,
         (upper(coalesce(p.chronic_conditions,'')) LIKE '%DIABET%')   AS dm,
         (upper(coalesce(p.chronic_conditions,'')) LIKE '%HYPERTENS%') AS htn,
         (g.lab_kdigo_stage IN ('G1','G2')) AS borderline,
         (p.patient_num*7 + 13) % 100 AS rnd
  FROM slhs_test1.ckd_prod.patient_ckd_gold g
  JOIN slhs_test1.ckd_prod.bronze_patient p USING (patient_id)
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
  SELECT patient_id, patient_num, has_ckd, tier, dm, htn, age, sex,
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
FROM parts;
