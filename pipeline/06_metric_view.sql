-- CKD Production - unified analytics view + governed Metric View (semantic layer)
-- Joins gold flags + AI notes CKD-risk signal + ML stage suggestion + the fusion CKD-risk model
-- (labs + notes) into one patient-level analytics view, then a Unity Catalog Metric View so Genie,
-- dashboards, and SQL all share consistent KPIs.

CREATE OR REPLACE VIEW slhs_test1.ckd_prod.patient_ckd_analytics
COMMENT 'Unified per-patient analytics: KDIGO lab stage + documented comparison + risk flags + AI notes CKD-risk signal + ML stage suggestion + fusion (labs+notes) CKD-risk prediction incl. caught_by_notes.' AS
SELECT g.*,
       s.suggested_stage, s.confidence AS model_confidence,
       -- AI notes signal (LLM inferred CKD risk from the free-text notes; note never states CKD)
       n.notes_ckd_risk, n.notes_ckd_risk_ord, n.notes_risk_flag, n.notes_indicators,
       -- fusion CKD-risk model (labs + notes signal), plus the labs-only counterfactual
       r.ckd_risk_score, r.ckd_risk_pred, r.ckd_risk_labs_only, r.caught_by_notes,
       (s.suggested_stage IS NOT NULL AND s.suggested_stage <> coalesce(g.documented_ckd, 'None')) AS model_vs_documented_differs
FROM slhs_test1.ckd_prod.patient_ckd_gold g
LEFT JOIN slhs_test1.ckd_prod.patient_ckd_scored s USING (patient_id)
LEFT JOIN slhs_test1.ckd_prod.patient_note_signals n USING (patient_id)
LEFT JOIN slhs_test1.ckd_prod.patient_ckd_risk_scored r USING (patient_id);

CREATE OR REPLACE VIEW slhs_test1.ckd_prod.ckd_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: slhs_test1.ckd_prod.patient_ckd_analytics
comment: Governed CKD KPIs for the identification & risk-flagging solution (labs + AI notes fusion).
dimensions:
  - name: Lab KDIGO Stage
    expr: lab_kdigo_stage
  - name: Suggested Stage
    expr: suggested_stage
  - name: Notes CKD Risk
    expr: notes_ckd_risk
  - name: Sex
    expr: sex
  - name: Provider
    expr: assigned_provider_name
  - name: Albuminuria
    expr: albuminuria_cat
measures:
  - name: Patient Count
    expr: COUNT(DISTINCT patient_id)
  - name: CKD Patients
    expr: SUM(CASE WHEN is_ckd THEN 1 ELSE 0 END)
  - name: Undocumented CKD
    expr: SUM(CASE WHEN undocumented_ckd THEN 1 ELSE 0 END)
  - name: Possibly Mis-Staged
    expr: SUM(CASE WHEN possibly_mis_staged THEN 1 ELSE 0 END)
  - name: High Risk No Nephrology
    expr: SUM(CASE WHEN high_risk_no_nephrology THEN 1 ELSE 0 END)
  - name: Advanced Not On Protective Meds
    expr: SUM(CASE WHEN advanced_not_on_protective_meds THEN 1 ELSE 0 END)
  - name: Rapid eGFR Decline
    expr: SUM(CASE WHEN rapid_egfr_decline THEN 1 ELSE 0 END)
  - name: Notes Flagged At-Risk
    expr: SUM(CASE WHEN notes_risk_flag THEN 1 ELSE 0 END)
  - name: Caught By Notes
    expr: SUM(CASE WHEN caught_by_notes THEN 1 ELSE 0 END)
  - name: Avg CKD Risk Score
    expr: AVG(ckd_risk_score)
  - name: Avg Model Confidence
    expr: AVG(model_confidence)
$$;
