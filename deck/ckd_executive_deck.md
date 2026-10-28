# Finding the CKD Hiding in Plain Sight

**St. Luke's Health System x Databricks** · CKD identification and risk flagging
**For:** VP of Population Health / Chief Medical Officer (executive sponsor), CFO / value-based care finance (funding), and primary care + nephrology leaders (domain owners).
All patient data shown is synthetic (2,000-patient panel shaped like St. Luke's de-identified sample). No PHI.

---

## Slide 1. The outcome, first

In a 2,000-patient primary care panel, the solution found:

| What we found | Patients | Who it matters to |
|---|---|---|
| CKD with **no stage in the chart** | **621** of 1,627 (1 in 3) | Clinical: care gap. Finance: diagnoses not documented or coded |
| Lab-evidenced **stage 3+ CKD, undocumented** | **497** | Finance: missing risk-adjustment diagnoses (CMS-HCC V28 categories 326 to 329) |
| **Stage 4/5, not seeing nephrology** | **238** | Clinical: delayed referral. Finance: highest risk of an unplanned dialysis start |
| Diabetic patients with **no urine albumin test** on record | **387** of 737 (53%) | Quality: HEDIS KED measure gap |
| Normal labs, **CKD found only in the notes** | **76** | Missed entirely by a labs-only approach |

**Ask:** a 90-day pilot at one St. Luke's clinic on de-identified data, measuring the four outcome metrics on Slide 4.

---

## Slide 2. The executive problem

- **Documentation drives revenue and quality scores.** In value-based and Medicare Advantage contracts, a CKD stage that is not documented by a clinician is not captured for risk adjustment, so the patient's expected cost is understated. CMS coding guidelines require the stage to come from the provider's diagnosis, not from lab values, so lab evidence alone does not count.
- **Late referral is the most expensive failure.** Unplanned ("crash") dialysis starts cost far more than planned starts (Slide 4).
- **Quality measures reward testing that is often skipped.** The KED measure (eGFR plus urine albumin-creatinine ratio for adults with diabetes) is scored in Medicare Advantage Star Ratings from 2026 and is MIPS Quality ID 488.
- **Physician time is the constraint.** Today the only way to find these patients is manual chart review of whole panels.
- St. Luke's has run Medicare ACOs since 2013 (MSSP, then Next Generation ACO, then ACO REACH through 2025), so the population-health team already manages to these economics.

---

## Slide 3. The clinician problem

- CKD stages are missing or wrong in Epic; physicians review whole panels by hand.
- KDIGO staging must use **outpatient** labs only; inpatient draws (acute kidney injury) distort the stage.
- Early CKD at a normal eGFR is often visible **only in the notes** ("urine has looked foamy", ankle swelling, "creatinine has drifted up"), never as the word CKD.
- Leadership's hard requirement: **nothing is written back to Epic without clinician approval.**

---

## Slide 4. What it is worth (illustrative, assumptions stated)

These are scenario estimates to be replaced with St. Luke's contract terms and volumes in the pilot. Every input is shown.

| Lever | Input | Source | Illustrative value per 2,000-patient panel |
|---|---|---|---|
| **Avoided cost of a crash dialysis start** | Median 12-month pre-dialysis cost: $67,059 crash vs $17,891 optimal start (difference about $49,000) | Wong et al., AJMC 2023 (Cleveland Clinic, value-based health system) | If timely referral converts **10 of the 238** high-risk patients from a crash start to a planned start: **about $490,000** |
| **Slower progression to dialysis** | Medicare spend per patient per year: $42,172 at CKD stage 4 to 5 vs $99,325 on hemodialysis (difference about $57,000) | USRDS Annual Data Report (2025 CKD; 2023 ESRD) | Each year dialysis is delayed for **10** patients: **about $570,000** in avoided spend |
| **Risk-adjustment accuracy** | 497 undocumented stage 3+ patients map to CKD HCCs once a clinician confirms and documents the stage | CMS-HCC V28 mappings | Valued with St. Luke's payer contracts; the solution produces the clinician-reviewed worklist that compliant documentation requires |
| **Quality incentives** | 387 diabetic patients missing a urine albumin test | NCQA KED; CMS Star Ratings; MIPS QID 488 | Gap closure supports Star Ratings and MIPS performance in contracts that use them |
| **Clinician time** | Assumption: 5 minutes per chart review; $123.47/hour (BLS mean wage, family medicine physicians, May 2024) | BLS OEWS | Reviewing the 621 prioritized patients instead of all 2,000: **about 115 physician hours, about $14,000 per review cycle** (wage only) |

**Pilot success metrics:** stages documented, mis-staging corrected, time to nephrology referral, chart-review hours per panel.

---

## Slide 5. What we built: one governed journey

1. **Data:** a seeded generator creates the synthetic panel; raw Epic-shaped files land in a governed volume.
2. **Lakeflow:** a declarative pipeline ingests with Auto Loader into bronze, silver, gold; the KDIGO outpatient-only rule is a data-quality expectation (200 inpatient labs dropped, counted).
3. **Unity Catalog:** comments, keys, MRN masking, least-privilege access, models governed in the catalog.
4. **Gen AI + ML:** an LLM reads every note for indirect CKD signs; MLflow models suggest the stage and fuse labs with the notes signal; served on Model Serving.
5. **Metric View + Genie Agent:** one definition of every KPI; plain-English questions with the SQL shown.
6. **Lakebase:** the worklist served from Postgres in milliseconds; clinician approvals written to a transactional queue.
7. **Databricks App:** the clinician worklist, patient review, and approve-and-queue for Epic.

---

## Slide 6. Show: the clinician worklist

- Pick a cohort (undocumented, high risk without nephrology, caught by notes, mis-staged, rapid eGFR decline).
- Open a patient: the note the AI read, the indicators it found, the eGFR trend, lab vs documented vs suggested stage.
- Confirm the stage and approve. The decision is recorded with who approved it and when.
- Worklist lookups take under 3 ms (p95) from Lakebase.

---

## Slide 7. Show: AI that explains itself

- 2,000 notes read, 0 unreadable, 1,094 rated moderate or high CKD risk, each with the phrases it found.
- On borderline patients (eGFR 60 or above), labs plus notes caught **95%** of true CKD vs **70%** for labs alone (AUC 0.96 vs 0.84).
- The stage model agrees with the clinician stage for 1,972 of 2,000 patients.
- Synthetic data proves the approach; the pilot proves clinical accuracy on St. Luke's data.

---

## Slide 8. Show: ask in plain English

- "How many patients have undocumented CKD?" Genie answers **621**, matching the governed Metric View, and shows its SQL.
- Executives and analysts self-serve the same numbers that drive the worklist.

---

## Slide 9. Safe, compliant write-back to Epic

- No AI output reaches the chart without clinician approval; this is also what compliant risk-adjustment documentation requires.
- Approvals land in a governed Lakebase queue (who, when, what stage, referral flag).
- An Epic interface (HL7 or FHIR, designed with St. Luke's Epic team) writes approved rows to the chart.
- Path to production: Lakeflow Connect from Epic Clarity, Unity AI Gateway in front of the LLM, MLflow evaluation of the notes prompt on clinician-labeled notes, data profiling for drift.

---

## Slide 10. Value for each audience, and next steps

| Audience | What they get |
|---|---|
| **VP Population Health / CMO** | A quantified care-gap picture (621 undocumented, 238 high risk without nephrology, 387 KED gaps) and a safe path to Epic |
| **CFO / value-based finance** | Documentation that supports accurate risk adjustment, fewer crash dialysis starts, quality-measure performance, less physician time on chart review |
| **Primary care and nephrology** | A ranked, explainable worklist where every flag shows its evidence |

**Next steps:** (1) validate on St. Luke's de-identified data; (2) pick a pilot clinic and clinical champion; (3) baseline the four success metrics; (4) design the Epic write-back with the Epic team.

*Sources: USRDS Annual Data Report 2025 (CKD expenditures, Table 6.3) and 2023 (ESRD expenditures, Figure 9.11); Wong et al., "Dialysis Costs for a Health System Participating in Value-Based Care," AJMC 2023;29(8):e235-e241; NCQA KED measure; CMS 2026 Star Ratings measures; CMS-HCC V28 ICD-10 mappings; BLS OEWS May 2024; St. Luke's Health Network ACO public reporting.*
