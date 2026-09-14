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
- Target: `slhs_test1` catalog, `ckd_prod` schema. Parameterized via the DAB `catalog` variable.

## Status
Built incrementally and verified layer by layer. See commit history; this README tracks the design.
