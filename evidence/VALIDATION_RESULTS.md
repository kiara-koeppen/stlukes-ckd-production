# CKD solution - end-to-end validation results

Run: 2026-10-28 14:00 UTC · target `slhs_test1.ckd_febar` · job run: https://adb-447340683886633.13.azuredatabricks.net/jobs/668617342983052/runs/566599301541514

**22/22 checks passed**

| Layer | Check | Result | Actual output |
|---|---|---|---|
| Synthetic data | Seeded generator built the source patient panel in this run | PASS | source_patient_registry=2000 patients, planted care-gap=500, stage 4/5 without nephrology=230 |
| Lakeflow | Bronze rows ingested by Auto Loader from the landing volume | PASS | bronze_patient=2000, bronze_lab_events=12200, bronze_clinical_notes=2000 |
| Lakeflow | KDIGO outpatient-only rule enforced in silver (expectation drops inpatient labs) | PASS | inpatient in bronze=200, inpatient in silver=0, silver_lab_events=12000 |
| Lakeflow | Silver + gold one row per patient | PASS | silver_patient_labs=2000, patient_ckd_gold=2000 |
| Lakeflow | Pipeline event log records expectation metrics (latest update) | PASS | [{"expectation": "has_note_text", "dataset": "slhs_test1.ckd_febar.bronze_clinical_notes", "passed": 2000, "failed": 0}, {"expectation": "has_patient", "dataset": "slhs_test1.ckd_febar.bronze_lab_events", "passed": 12200, "failed": 0}, {"expectation": "known_analyte", "dataset": "slhs_test1.ckd_febar.bronze_lab_events", "passed": 12200, "failed": 0}, {"expectation": "valid_age", "dataset": "slhs_test1.ckd_febar.bronze_patient", "passed": 2000, "failed": 0}, {"expectation": "valid_sex", "dataset": "slhs_test1.ckd_febar.bronze_patient", "passed": 2000, "failed": 0}, {"expectation": "has_lab_stage", "dataset": "slhs_test1.ckd_febar.patient_ckd_gold", "passed": 2000, "failed": 0}, {"expectation": "outpatient_only", "dataset": "slhs_test1.ckd_febar.silver_lab_events", "passed": 12000, "failed": 200}, {"expectation": "plausible_value", "dataset": "slhs_test1.ckd_febar.silver_lab_events", "passed": 12200, "failed": 0}] |
| Unity Catalog | Primary/foreign keys declared on pipeline tables | PASS | bronze_patient:PRIMARY KEY, patient_ckd_gold:FOREIGN KEY, patient_ckd_gold:PRIMARY KEY, patient_worklist:PRIMARY KEY, source_patient_registry:PRIMARY KEY |
| Unity Catalog | PHI column mask on MRN (reader not in ckd_phi_readers sees masked value) | PASS | gold.mrn=MRN-***-****, bronze.mrn=MRN-***-****, reader_in_phi_group=False |
| Unity Catalog | Gold columns documented with comments | PASS | 28/28 columns commented |
| Gen AI | ai_query read every note and returned structured CKD risk | PASS | notes=2000, unparsed=0, moderate/high risk=1094 |
| Gen AI | Example AI read on an occult-CKD patient (normal labs, caught by notes) | PASS | risk=moderate, indicators=['increasing fatigue', 'ankle swelling', 'rising creatinine'] |
| ML | Stage-suggestion model batch scores | PASS | scored=2000, agree with clinician stage=1972, mean confidence=0.923 |
| ML | Fusion (labs + notes) model catches CKD the labs-only model misses | PASS | scored=2000, flagged CKD risk=1615, caught_by_notes (labs-only missed)=76 |
| ML | ckd_stage_model registered in Unity Catalog with MLflow metrics | PASS | slhs_test1.ckd_febar.ckd_stage_model v3, metrics={'accuracy': 0.93, 'macro_f1': 0.839} |
| ML | ckd_risk_model registered in Unity Catalog with MLflow metrics | PASS | slhs_test1.ckd_febar.ckd_risk_model v3, metrics={'baseline_acc': 0.954, 'baseline_auc': 0.972, 'baseline_borderline_auc': 0.842, 'baseline_borderline_recall': 0.699, 'baseline_f1': 0.971, 'fusion_acc': 0.99, 'fusion_auc': 0.993, 'fusion_borderline_auc': 0.961, 'fusion_borderline_recall': 0.945, 'fusion_f1': 0.994} |
| ML | Real-time Model Serving endpoint returns a prediction | PASS | endpoint=ckd-risk-model-febar, input={"age": 55.0, "mean_egfr": 97.7, "latest_egfr": 96.0, "prior_egfr": 98.0, "egfr_trend_recent_minus_prior": -2.0, "mean_creatinine": 0.71, "notes_ckd_risk_ord": 2}, response=[1] |
| Semantic layer | Metric View KPIs (MEASURE query) | PASS | patients=2000, ckd=1627, undocumented=621, possibly_mis_staged=527, high_risk_no_nephrology=238, caught_by_notes=76 |
| Genie Agent | Live Genie answer matches the governed Metric View | PASS | question='How many patients have undocumented CKD?', genie_result=[['621']], metric_view=621, sql='SELECT MEASURE(`Undocumented CKD`) AS `undocumented_ckd_patients`\nFROM `slhs_test1`.`ckd_febar`.`ckd_metrics`\nGROUP BY ALL' |
| Lakebase | Synced table serves the gold worklist in Postgres (matches Delta) | PASS | state=SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE, postgres rows=2000, undocumented=621, last_sync=None |
| Lakebase | Low-latency point reads | PASS | 50 primary-key lookups: p50=2.4 ms, p95=2.7 ms |
| Lakebase | OLTP write-back queue round-trip (insert -> read -> delete) | PASS | inserted id=2, read back=('ACO-009EM0Q3X3K', 'G3a', True, 'validation') |
| App | App running with Lakebase + SQL warehouse resources | PASS | app=ckd-risk-worklist-febar, url=https://ckd-risk-worklist-febar-447340683886633.13.azure.databricksapps.com, compute=ACTIVE, deployment=SUCCEEDED, resources=['postgres', 'sql_warehouse'] |
| App | App service principal has least-privilege UC access (no admin) | PASS | sp=fcf71a9e-349e-446f-a2b5-38db70313676, schema grants=['EXECUTE', 'SELECT', 'USE SCHEMA', 'EXECUTE', 'READ VOLUME', 'SELECT', 'USE SCHEMA'] |
