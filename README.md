# St. Luke's: CKD Identification & Risk Flagging (end-to-end Databricks build)

**Customer:** St. Luke's Health System (Boise, ID). **Executive sponsor:** VP of Population Health / CMO,
with value-based care finance (CFO) funding. **Domain owners:** primary care and nephrology. **Data:**
100% synthetic, seeded, no PHI. **Deck:** `deck/ckd_executive_deck.md`.

## The outcome (2,000-patient synthetic panel, from `evidence/VALIDATION_RESULTS.md`)

| Finding | Patients | Clinical meaning | Financial meaning |
|---|---|---|---|
| CKD with no stage in the chart | **621** of 1,627 | Care gap | Diagnoses not documented or coded |
| Undocumented, lab-evidenced stage 3+ | **497** | Unstaged, under-managed | Missing risk-adjustment HCCs (CMS-HCC V28 326-329) |
| Stage 4/5, not seeing nephrology | **238** | Delayed referral | Crash dialysis start: ~$49K higher median 12-month cost than a planned start (AJMC 2023) |
| Diabetic, no urine albumin test | **387** of 737 | Untested kidney damage | HEDIS KED gap (MA Star Ratings, MIPS QID 488) |
| Normal labs, CKD only in the notes | **76** | Missed by labs-only rules | Same levers, otherwise invisible |

Illustrative value per panel (assumptions in the deck): converting 10 of the 238 to planned dialysis
starts is about $490K; prioritizing the 621 instead of reviewing all 2,000 charts saves about 115
physician hours per review cycle.

**The problem.** CKD stages are often missing or wrong in Epic, and physicians review whole panels by
hand. St. Luke's asked to (1) find CKD with missing or unclear staging, (2) suggest the stage from labs
and their trend, (3) flag high-risk patients, and (4) cut chart-review burden, with **nothing written
back to Epic without clinician approval**.

## Architecture: one connected journey

```
 Seeded synthetic patient panel, generated from scratch (no PHI)     [setup/00_generate_patients.py]
        ▼
 Synthetic Epic-shaped extract (patients, lab events, notes)        [setup/01_generate_raw_extract.py]
        │  CSV files in a Unity Catalog volume (in prod: Lakeflow Connect off Clarity / HL7-FHIR feeds)
        ▼
 ┌─ LAKEFLOW (Spark Declarative Pipeline, serverless) ─────────────  [pipeline/01-03_*.sql]
 │  bronze_*  Auto Loader streaming tables, typed on read, expectations (valid age/sex, known analyte)
 │  silver    outpatient_only expectation drops inpatient eGFRs (KDIGO rule) → per-patient lab summary
 │  gold      patient_ckd_gold: KDIGO stage, documented-vs-lab, undocumented / mis-staged / high-risk flags
 └─ UNITY CATALOG: comments on every column, PK/FK, MRN column mask, lineage, grants
        │
        ├─▶ GEN AI   ai_query reads every note for indirect CKD signs → patient_note_signals   [jobs/02]
        ├─▶ ML       MLflow: stage-suggestion model + fusion (labs + notes) risk model,
        │            registered in UC, served on Model Serving, batch-scored                    [ml/]
        ├─▶ SEMANTIC patient_ckd_analytics + Metric View ckd_metrics (one KPI definition)      [jobs/04]
        ├─▶ GENIE    Genie Agent on the Metric View with instructions + 16 benchmarks          [genie/, jobs/06]
        └─▶ LAKEBASE synced table patient_worklist_synced (Postgres) for fast app reads,
                     + native OLTP table app.epic_writeback_queue                             [jobs/05]
                          │
                          ▼
 DATABRICKS APP  ckd-risk-worklist-febar (Streamlit)                                            [app/]
   KPI tiles + cohort worklist (from Lakebase) → patient review (note, eGFR trend, model stage)
   → clinician approves stage → row in Lakebase write-back queue (the Epic integration point)
   + embedded Ask Genie, + notes analyzer (live ai_query + real-time model endpoint)
```

Every layer reads the same governed data in `slhs_test1.ckd_febar`; nothing is mocked. The
validation task traces the same numbers through every layer (e.g. undocumented CKD = 621 in the
Metric View, in Genie's live answer, and in Lakebase).

## Execution evidence

`evidence/VALIDATION_RESULTS.md` is written by the last job task against the live assets and records
actual outputs: **22/22 checks passed**, including the seeded generator's patient panel, Lakeflow expectation metrics from the pipeline
event log, the MRN mask in effect, MLflow metrics, a live serving-endpoint response, Genie's answer
and generated SQL, Lakebase row counts and point-read latency (p95 under 3 ms), an OLTP write round-trip,
and the app's resources and grants.

## Prerequisites

* A recent Databricks CLI with Lakebase Autoscaling bundle support (built and tested with v1.19.0), authenticated to the workspace (`databricks auth login`)
* Unity Catalog workspace with serverless compute, a serverless/Pro SQL warehouse, Lakebase
  Autoscaling, Databricks Apps, Model Serving, Genie, and Foundation Model APIs
  (`databricks-meta-llama-3-3-70b-instruct`)
* Permission to create schemas/volumes in the target catalog, Lakebase projects, serving endpoints, and apps

## Deploy and run

```bash
# 1. Deploy all resources (Lakebase project, pipeline, job, app)
databricks bundle deploy -t febar

# 2. Build everything end to end, in dependency order, ending with validation
databricks bundle run ckd_end_to_end -t febar

# 3. Deploy the app source and start it
databricks bundle run ckd_worklist_febar -t febar
```

The job's tasks: `generate_patients` → `generate_raw_extract` → `lakeflow_pipeline` (full refresh) →
`note_ai` → `train_models` → `serve_models` → `semantic_layer` → `genie_agent` → `lakebase_and_grants`
→ `validate`. The data is generated from scratch with a fixed seed, so every run tells the same story.
Re-running is idempotent (endpoints update, Genie updates by title, the synced table re-syncs).

> If `bundle deploy` fails with "openpgp: key expired", point the CLI at a local Terraform:
> `export DATABRICKS_TF_EXEC_PATH=$(which terraform) DATABRICKS_TF_VERSION=<your version>`.

**Another workspace:** fill in the `prod` target in `databricks.yml` (host, profile, catalog,
warehouse) and run the same commands with `-t prod`.

## Configuration (bundle variables)

| Variable | Default | Purpose |
|---|---|---|
| `catalog` / `schema` | `slhs_test1` / `ckd_febar` | Where every table, view, volume, function, and model lands |
| `warehouse_id` | `9946580e2a7f4dd1` | SQL warehouse for Genie and the app |
| `lakebase_project` | `ckd-febar` | Lakebase Autoscaling project (branch `production`, db `databricks_postgres`) |
| `app_name` | `ckd-risk-worklist-febar` | Databricks App name |
| `stage_endpoint` / `risk_endpoint` | `ckd-stage-model-febar` / `ckd-risk-model-febar` | Model Serving endpoints |
| `genie_title` | `CKD Identification & Risk Flagging (FE Bar)` | Genie Agent title (the app finds it by title) |

## Repo structure

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle: variables and targets (`febar` default, `prod` template) |
| `resources/` | Bundle resources: Lakebase project, Lakeflow pipeline, end-to-end job, app |
| `setup/00_generate_patients.py` | Seeded generator: invents the 2,000-patient panel (documentation, serial labs, ground truth, meds) |
| `setup/01_generate_raw_extract.py` | Turns the panel into a raw Epic-shaped extract (patients, lab events, notes) in the landing volume; creates the MRN mask function |
| `pipeline/01_bronze.sql`, `02_silver.sql`, `03_gold.sql` | Lakeflow pipeline: bronze/silver/gold with expectations, keys, mask, comments |
| `jobs/02_note_ai.py` | `ai_query` note reading → per-note and per-patient CKD-risk signals |
| `ml/train_ckd_stage_model.py` | MLflow training, UC registration, batch scoring |
| `ml/serve_models.py` | Deploys both models to Model Serving endpoints |
| `jobs/04_semantic_layer.py` | Analytics view, Metric View, Lakebase-ready worklist table |
| `jobs/06_genie_space.py` + `genie/space_template.json` | Genie Agent definition (instructions, sample questions, benchmarks) |
| `jobs/05_lakebase_and_grants.py` | Synced table, Postgres write-back queue, app service-principal grants |
| `jobs/99_validate.py` | End-to-end checks; writes `evidence/` |
| `app/` | Streamlit clinician worklist app |
| `deck/ckd_executive_deck.md` | Customer deck: outcome first, clinical + financial value, assumptions and sources |
| `BUILD.md` | How it was built: AI-assisted workflow and key decisions |

## Path to production

Replace the generator with Lakeflow Connect from Epic Clarity (or HL7/FHIR feeds) into the same
bronze tables; put Unity AI Gateway in front of the LLM; evaluate the notes prompt with MLflow
against clinician-labeled notes; drain `app.epic_writeback_queue` through an Epic FHIR `Condition`
write with a final clinician accept; add Lakehouse Monitoring on the gold and scored tables.
