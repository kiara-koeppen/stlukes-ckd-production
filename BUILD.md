# How this was built

This file records the workflow, the AI tooling, and the decisions behind the solution, so a reviewer
can see how it came together (not only what it does).

## Context

St. Luke's Health System asked for help finding patients whose chronic kidney disease (CKD) is
missing or wrongly staged in Epic. Physicians review large panels by hand, so CKD gets missed and
interventions are late. Leadership's main concern was getting results **back into Epic** safely, with
a clinician in the loop. The first version of this build was the "art of the possible" production
reference shown at the St. Luke's onsite (Sept 2026). This version turns it into a single end-to-end,
reproducible Databricks build.

## Workflow

* **AI pair-programming:** built with Claude Code (Isaac, Databricks' internal distribution) plus the
  Databricks CLI and Asset Bundles. AI did most of the typing; every layer was checked against live
  workspace output before moving on (see `evidence/`).
* **Grounding:** the data shape, KDIGO staging rules (outpatient labs only, 3-month means), and the
  four asks come from St. Luke's project request and their de-identified sample's structure. No real
  data was used; everything is synthetic and deterministic.
* **Docs over memory:** product syntax was checked against current docs before use (Lakeflow SQL
  reference for expectations/masks/keys in pipeline tables, Lakebase Autoscaling synced tables and
  the Apps `postgres` resource).

## Key decisions

| Decision | Why |
|---|---|
| Patient panel generated **from scratch** by a seeded generator (`setup/00`), turned into a raw extract in a UC volume, then ingested by Auto Loader | Keeps synthetic patient-shaped data off laptops, and mirrors how Lakeflow Connect / HL7 feeds would land in production. |
| Clinical rules as **pipeline expectations** (e.g. `outpatient_only` drops inpatient eGFRs) | The KDIGO rule becomes a visible, counted data-quality metric instead of a silent `WHERE`. |
| MRN mask declared **in the table definitions** | Governance lives with the code; gold recomputes the MRN so a masked bronze value never propagates. |
| Notes read by an LLM (`ai_query`), then fed into an ML model | CKD at normal eGFR needs a damage marker that often lives only in the notes. The fusion model catches cases a labs-only model misses (`caught_by_notes`). |
| **Lakebase** for the worklist (synced table) and the write-back queue (native Postgres table) | The App needs fast point reads and transactional writes; a warehouse scan per click is the wrong tool. The queue is the Epic integration point (HL7/FHIR in production). |
| Genie Agent defined in `genie/space_template.json` with benchmarks | The agent is versioned and redeployable, and its answers are checked against the Metric View. |
| One job builds everything in dependency order, ending in validation | One command reproduces the build; the validation task writes text evidence that it ran. |
| Separate schema (`ckd_febar`) | The original onsite demo (`ckd_prod`) stays untouched. |

## Where AI mattered most

* Turning the brief into the medallion + KDIGO logic and the occult-CKD story (normal labs, CKD only
  in the notes) so every layer tells the same story.
* Reading 2,000 free-text notes for indirect CKD signs (`ai_query`), which is the step that makes the
  "hidden in the notes" cohort findable at all.
* Wiring the less familiar pieces (Lakebase Autoscaling synced tables, app Postgres roles, Genie
  space serialization) quickly, then verifying them live.

## What I'd do next for production

Lakeflow Connect from Epic Clarity instead of the generator; Unity AI Gateway in front of the LLM;
MLflow GenAI evaluation of the notes prompt against clinician-labeled notes; drain
`app.epic_writeback_queue` through an Epic FHIR `Condition` write with a final clinician accept.
