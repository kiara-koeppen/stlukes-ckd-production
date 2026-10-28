-- CKD Identification & Risk Flagging - Lakeflow pipeline, SILVER layer (see 01_bronze.sql for the overview)
-- Pipeline configuration supplies ${catalog} / ${schema}.

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
