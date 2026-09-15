-- CKD Production - AI note extraction (ai_query)
-- Implements the KDIGO ascertainment method's "search notes for nephrolog" step with an LLM:
-- read each clinical note and extract whether CKD is mentioned and whether the patient is seeing
-- nephrology. Model: databricks-meta-llama-3-3-70b-instruct (present in the workspace). In prod,
-- put Unity AI Gateway (rate limits, cost, PII guardrails) in front of this endpoint.

-- Per-note signals
CREATE OR REPLACE TABLE slhs_test1.ckd_prod.note_ckd_signals
COMMENT 'AI-extracted per-note signals: does the note mention CKD, and does it indicate nephrology care.' AS
SELECT patient_id, note_id, note_date,
  from_json(regexp_extract(resp, '\\{.*\\}', 0),
            'STRUCT<mentions_ckd:STRING, seeing_nephrology:STRING>') AS sig
FROM (
  SELECT patient_id, note_id, note_date,
    ai_query('databricks-meta-llama-3-3-70b-instruct',
      concat('Read this clinical note and respond with ONLY JSON: ',
             '{"mentions_ckd": "Yes" or "No", "seeing_nephrology": "Yes" or "No"}. ',
             'seeing_nephrology is Yes only if the note indicates the patient sees or was referred to a nephrologist. Note: ',
             note_text)) AS resp
  FROM slhs_test1.ckd_prod.bronze_clinical_notes
);

-- Patient-level rollup: any note evidence of CKD / nephrology
CREATE OR REPLACE TABLE slhs_test1.ckd_prod.patient_note_signals
COMMENT 'Patient-level AI note signals rolled up from note_ckd_signals (any note that mentions CKD / nephrology).' AS
SELECT patient_id,
       max(CASE WHEN lower(sig.mentions_ckd)='yes' THEN true ELSE false END) AS note_mentions_ckd,
       max(CASE WHEN lower(sig.seeing_nephrology)='yes' THEN true ELSE false END) AS note_says_nephrology,
       count(*) AS n_notes
FROM slhs_test1.ckd_prod.note_ckd_signals
GROUP BY patient_id;
