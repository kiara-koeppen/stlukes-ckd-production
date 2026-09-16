# St. Luke's — CKD Identification & Risk Flagging (Production Solution)

A production‑grade, end‑to‑end reference solution for the St. Luke's Health System CKD use case.
Where the workshop shows the "art of the possible," this shows **what an actual build looks like** on
Databricks — medallion pipeline, deterministic KDIGO staging, an ML stage‑suggestion model, clinical
note AI, a governed semantic layer, conversational + BI access, and a clinician App with a
human‑in‑the‑loop path to write results back to Epic.

## The problem (from St. Luke's project request)
> CKD stages are often missing or incorrectly documented in Epic. Physicians must manually review
> large patient populations, which is time‑consuming and risks delayed intervention.

The solution must: (1) identify patients with CKD and unclear/missing staging, (2) **suggest the
likely CKD stage from labs and trends over time**, (3) flag high‑risk patients for clinician review,
(4) reduce manual chart‑review burden. Leadership's key concern: **writing results back to Epic**
smoothly and with a human in the loop.

## Architecture (end to end)
```
Epic feeds (labs, notes, meds, problem list)
   │  Lakeflow Connect (prod)  /  synthetic seed (this build)
   ▼
BRONZE  raw lab events, notes, meds            ─┐
   ▼                                            │ Lakeflow Declarative Pipeline (SDP)
SILVER  outpatient-only labs, 3-mo eGFR means   │  bronze → silver → gold, medallion
   ▼                                            │
GOLD    patient features + KDIGO lab stage +   ─┘
        risk / care-gap flags  (patient_ckd_gold)
   ├─▶ AI Functions: ai_query/ai_extract on notes → nephrology & CKD mentions
   ├─▶ ML (MLflow): CKD-stage suggestion model → UC Model Registry → Model Serving → batch + realtime scores
   ├─▶ Metric View: governed KPIs (CKD, undocumented, mis-staged, high-risk)
   ├─▶ Genie Agent + AI/BI Dashboard: clinician self-service + population view
   └─▶ Databricks App: clinician worklist (lab trend, suggested vs documented stage, confidence)
              └─▶ human-in-the-loop review → write-back queue (Lakebase) = Epic integration point
Cross-cutting: Unity Catalog governance (PK/FK, comments, ABAC PHI masking), Unity AI Gateway,
MLflow evaluation, Lakehouse Monitoring, Lakeflow Jobs orchestration, packaged as a DAB.
```

## KDIGO staging (validated)
Lab‑derived stage from the mean of the 3 most‑recent **outpatient** eGFRs (inpatient labs excluded):
G1 ≥90 · G2 60‑89 · G3a 45‑59 · G3b 30‑44 · G4 15‑29 · G5 <15 (eGFR ≥60 needs albuminuria to be CKD).
This reproduces the clinician‑ascertained "actual stage" closely on the sample data, and the
documented‑vs‑lab comparison is what surfaces missing/mis‑staged patients.

## Repo structure
- `pipeline/` — medallion + KDIGO staging (SDP SQL; `03_gold_ckd.sql` is the gold layer)
- `ai_functions/` — `ai_query`/`ai_extract` note extraction
- `ml/` — MLflow training + evaluation of the CKD‑stage suggestion model
- `app/` — Databricks App (clinician worklist + Epic write‑back queue)
- `resources/` — DAB resource defs (pipeline, jobs, model, app)
- `setup/` — bootstrap (schema + synthetic seed)

## Data & scope notes
- Built on **synthetic** CKD data (no PHI). The real de‑identified sample is not committed.
- "Write back to Epic" is implemented as a governed **approval queue / integration point** (there is
  no Epic in this environment) — the pattern leadership needs to see, ready to wire to Epic's
  HL7/FHIR or app‑orchard APIs.
- Target: `slhs_test1` catalog, `ckd_prod` schema. The DAB `catalog`/`schema` vars drive the train-model
  job; the `pipeline/*.sql` still hardcode the catalog/schema (see "Deploy in your own workspace").

## Deploy in your own workspace
This repo is a **reference build** wired to a Databricks demo workspace (catalog `slhs_test1`) on
**synthetic** data. Standing it up elsewhere (e.g., a St. Luke's workspace) is a guided setup, not a
one-command deploy — here's the honest sequence.

**Prerequisites:** Databricks CLI ≥ 0.239, Unity Catalog, a **serverless / Pro SQL warehouse**, and
permissions to create schemas, jobs, models, and apps. For `ai_forecast` and doc-Q&A, enable the
**Predictive AI Functions** and **Genie-on-Volumes** previews (Settings → Previews).

1. **Auth to your workspace** — `databricks auth login --host https://<your-host>` and note the profile name.
2. **Point the bundle at your workspace** — edit the `prod` target in `databricks.yml`: set `host`,
   `profile`, `catalog`, `schema`, `warehouse_id` (the four `REPLACE-…` placeholders).
3. **Seed + build the tables** ⚠️ *the one manual step*. `pipeline/*.sql` currently **hardcodes**
   `slhs_test1.ckd_prod` and reads its synthetic source from `slhs_test1.clinical.ckd_patient_registry`.
   To run elsewhere you must: (a) load an equivalent patient registry into your catalog (the synthetic
   generator lives in the `stlukes-workshop-data` repo), and (b) find-and-replace `slhs_test1.ckd_prod`
   → `<your_catalog>.<your_schema>` (and the `slhs_test1.clinical.ckd_patient_registry` source) across
   `pipeline/*.sql` + `ai_functions/*.sql`, then run them **in order** (01→06) in the SQL editor.
   *(Ask for the fully-parameterized version if you'd rather not hand-edit — see "Not yet automated".)*
4. **Deploy the bundle** — `databricks bundle deploy -t prod` (creates the train-model job + the app).
5. **Train + serve the model** — `databricks bundle run ckd_train_model -t prod` (registers the model in
   UC and, per the notebook, sets up serving + batch scores).
6. **Create your Genie space** on your gold tables (`patient_ckd_analytics`, `ckd_metrics`), grab its
   space ID, and set it — plus `CATALOG` / `SCHEMA` / `DATABRICKS_WAREHOUSE_ID` — in **`app/app.yaml`**;
   then `databricks bundle run ckd_worklist -t prod` to (re)deploy the app.
7. **(Optional)** attach the `clinical_guidelines` volume to your Genie space for doc Q&A.

**Going from synthetic to real data:** replace the step-3 seed with ingestion from Epic —
**Lakeflow Connect** off Clarity/Caboodle, or HL7/FHIR feeds — landing into bronze; then wire the
`epic_writeback_queue` to Epic (FHIR `Condition` write / HL7) with a clinician-accept step. This is the
productionization boundary; the demo stops at the governed, approved queue.

### Not yet automated (good next steps for a real handoff)
- **Parameterize `pipeline/*.sql` + `ai_functions/*.sql`** (remove the hardcoded `slhs_test1.ckd_prod`)
  and wrap them in a Lakeflow pipeline / job so `bundle deploy -t prod` builds the data end-to-end.
- **Move the app's `GENIE_SPACE_ID` / catalog / schema into the DAB** so they don't need a separate
  `app.yaml` edit.

## Status
Built incrementally and verified layer by layer. See commit history; this README tracks the design.

## Deployed assets (dev target: slhs_test1.ckd_prod)
- **Medallion**: `bronze_patient`, `bronze_lab_events`, `bronze_clinical_notes`, `silver_patient_labs`, `patient_ckd_gold`
- **AI notes**: `note_ckd_signals`, `patient_note_signals`
- **ML**: model `ckd_prod.ckd_stage_model` (UC), scores `patient_ckd_scored`, serving endpoint `ckd-stage-model`
- **Semantic layer**: view `patient_ckd_analytics`, metric view `ckd_metrics`
- **Genie agent**: "CKD Identification & Risk Flagging (Production)" — space `01f1b09e2ba113eda818aac6bf646a95`
- **App**: `ckd-risk-worklist` → https://ckd-risk-worklist-447340683886633.13.azure.databricksapps.com
- **Write-back queue**: `epic_writeback_queue` (human-in-the-loop → Epic integration point)
- **Governance**: PK/FK (RELY), column comments, ABAC PHI mask on MRN (`mask_mrn`)
- **Smoke test**: see `SMOKE_TEST.md` (11/11 passing)
- **Demo cheat sheet**: Google Doc in the St Lukes workshop Drive folder
