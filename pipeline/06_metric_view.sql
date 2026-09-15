-- CKD Production - unified analytics view + governed Metric View (semantic layer)
-- Joins gold flags + AI note signals + ML stage suggestions into one patient-level analytics view,
-- then defines a Unity Catalog Metric View so Genie, dashboards, and SQL all share consistent KPIs.

CREATE OR REPLACE VIEW slhs_test1.ckd_prod.patient_ckd_analytics
COMMENT 'Unified per-patient analytics: KDIGO lab stage + documented comparison + risk flags + AI note signals + ML stage suggestion.' AS
SELECT g.*,
       n.note_mentions_ckd, n.note_says_nephrology,
       s.suggested_stage, s.confidence AS model_confidence,
       (g.sees_nephrology <> coalesce(n.note_says_nephrology, g.sees_nephrology)) AS nephrology_evidence_conflict,
       (s.suggested_stage IS NOT NULL AND s.suggested_stage <> coalesce(g.documented_ckd, 'None')) AS model_vs_documented_differs
FROM slhs_test1.ckd_prod.patient_ckd_gold g
LEFT JOIN slhs_test1.ckd_prod.patient_note_signals n USING (patient_id)
LEFT JOIN slhs_test1.ckd_prod.patient_ckd_scored s USING (patient_id);

CREATE OR REPLACE VIEW slhs_test1.ckd_prod.ckd_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: slhs_test1.ckd_prod.patient_ckd_analytics
comment: Governed CKD KPIs for the identification & risk-flagging solution.
dimensions:
  - name: Lab KDIGO Stage
    expr: lab_kdigo_stage
  - name: Suggested Stage
    expr: suggested_stage
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
  - name: Avg Model Confidence
    expr: AVG(model_confidence)
$$;
