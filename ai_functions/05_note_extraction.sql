-- CKD Production - AI note extraction (ai_query): INFERENTIAL CKD-RISK from the notes
-- The clinician's insight: CKD is often hidden in the notes. Notes here carry INDIRECT breadcrumbs
-- (proteinuria/foamy urine, edema, rising creatinine, uncontrolled HTN/DM, nocturia) and NEVER state
-- "CKD". This LLM step READS each note and INFERS a CKD-risk level (none/low/moderate/high) plus the
-- indicators it found — reasoning from the breadcrumbs, not the diagnosis. The result becomes a feature
-- fed (with the labs) into the fusion CKD-risk model. Model: databricks-meta-llama-3-3-70b-instruct.
-- In prod, front this with Unity AI Gateway (rate limits, cost, PII guardrails).

-- Per-note inferred CKD risk
CREATE OR REPLACE TABLE slhs_test1.ckd_prod.note_ckd_signals
COMMENT 'AI-inferred CKD risk per note (LLM reasons from indirect breadcrumbs; note never states CKD).' AS
SELECT patient_id, note_id, note_date,
  from_json(regexp_extract(resp, '\\{.*\\}', 0),
            'STRUCT<ckd_risk:STRING, indicators:ARRAY<STRING>, rationale:STRING>') AS sig
FROM (
  SELECT patient_id, note_id, note_date,
    ai_query('databricks-meta-llama-3-3-70b-instruct',
      concat('You are a nephrology-aware clinician reviewing an outpatient progress note that may NOT mention kidney ',
             'disease by name. Judge ONLY from indirect evidence: proteinuria or foamy urine, ankle or periorbital edema, ',
             'rising or elevated creatinine, poorly controlled hypertension, poorly controlled diabetes, nocturia, fatigue, ',
             'nausea or poor appetite. Assess this patient''s risk of having undiagnosed chronic kidney disease. Respond with ',
             'ONLY minified JSON and nothing else: ',
             '{"ckd_risk":"none|low|moderate|high","indicators":["short phrases you found"],"rationale":"one sentence"}. Note: ',
             note_text)) AS resp
  FROM slhs_test1.ckd_prod.bronze_clinical_notes
);

-- Patient-level rollup: ordinal max risk + a boolean at-risk flag (moderate/high) + indicators
CREATE OR REPLACE TABLE slhs_test1.ckd_prod.patient_note_signals
COMMENT 'Patient-level AI note CKD-risk: ordinal max risk + boolean at-risk flag (moderate/high) + indicators. Feeds the fusion model.' AS
SELECT patient_id,
  max(CASE lower(sig.ckd_risk) WHEN 'high' THEN 3 WHEN 'moderate' THEN 2 WHEN 'low' THEN 1 ELSE 0 END) AS notes_ckd_risk_ord,
  element_at(array('none','low','moderate','high'),
    1 + max(CASE lower(sig.ckd_risk) WHEN 'high' THEN 3 WHEN 'moderate' THEN 2 WHEN 'low' THEN 1 ELSE 0 END)) AS notes_ckd_risk,
  (max(CASE lower(sig.ckd_risk) WHEN 'high' THEN 3 WHEN 'moderate' THEN 2 WHEN 'low' THEN 1 ELSE 0 END) >= 2) AS notes_risk_flag,
  flatten(collect_list(coalesce(sig.indicators, array()))) AS notes_indicators,
  count(*) AS n_notes
FROM slhs_test1.ckd_prod.note_ckd_signals
GROUP BY patient_id;
