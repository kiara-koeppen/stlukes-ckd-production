-- CKD Production - SILVER
-- Outpatient-only lab aggregation per the KDIGO ascertainment method: exclude inpatient labs,
-- use the recent outpatient values, and compute per-patient mean eGFR/creatinine plus a trend.

CREATE OR REPLACE TABLE slhs_test1.ckd_prod.silver_patient_labs
COMMENT 'Silver: per-patient outpatient lab summary - mean eGFR/creatinine over recent outpatient draws, latest vs prior eGFR, and trend. Inpatient labs excluded.' AS
WITH egfr AS (
  SELECT patient_id,
         round(avg(value),1) AS mean_egfr,
         max(CASE WHEN seq=1 THEN value END) AS latest_egfr,
         max(CASE WHEN seq=3 THEN value END) AS prior_egfr,
         count(*) AS n_outpatient_egfr
  FROM slhs_test1.ckd_prod.bronze_lab_events
  WHERE analyte='egfr' AND encounter_type='outpatient'
  GROUP BY patient_id
),
creat AS (
  SELECT patient_id, round(avg(value),2) AS mean_creatinine
  FROM slhs_test1.ckd_prod.bronze_lab_events
  WHERE analyte='creatinine' AND encounter_type='outpatient'
  GROUP BY patient_id
)
SELECT e.patient_id, e.mean_egfr, e.latest_egfr, e.prior_egfr, e.n_outpatient_egfr,
       c.mean_creatinine,
       (e.latest_egfr - e.prior_egfr) AS egfr_trend_recent_minus_prior
FROM egfr e JOIN creat c USING (patient_id);
