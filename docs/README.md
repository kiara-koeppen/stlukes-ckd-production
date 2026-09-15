# CKD demo — synthetic reference docs + Genie curation

Two demo-support artifacts live here, both **manual ~2-minute steps in the Databricks UI** (neither is settable via the public API in this workspace):

1. **Clinical-reference PDFs** → attach as a volume to the Genie agent so leadership can ask questions *beyond the structured data*.
2. **Example questions** → paste into the Genie space so scripted demo questions are one click.

---

## 1. Synthetic clinical-reference PDFs (Genie-on-Volumes)

Four synthetic PDFs in this folder — **general public-knowledge CKD content, clearly labeled synthetic, not St. Luke's policy**:

| File | Answers questions like |
|---|---|
| `01_CKD_Staging_Reference.pdf` | "What eGFR range is stage G3b?" "What makes something CKD vs. a one-off low value?" |
| `02_Nephrology_Referral_Criteria.pdf` | "When should a CKD patient be referred to nephrology?" |
| `03_Kidney_Protective_Medications.pdf` | "What are kidney-protective medications?" "Why an SGLT2 inhibitor?" |
| `04_CKD_Monitoring_Care_Pathway.pdf` | "How often should a stage G4 patient have labs?" "What's the BP target?" |

**Already uploaded** to the UC volume `slhs_test1.ckd_prod.clinical_guidelines`
(`/Volumes/slhs_test1/ckd_prod/clinical_guidelines/`).

### Attach the volume to the Genie agent (one-time, in the UI)
> **Requires the Genie-on-Volumes Beta enabled in the workspace.** If you don't see the option, that preview isn't on yet — request it or skip this part of the demo.

1. Open the CKD Genie space → **Configure** (⚙️).
2. Find **Documents / Files** (a.k.a. *Knowledge* / *Unstructured data*).
3. **Add the volume** `slhs_test1.ckd_prod.clinical_guidelines`.
4. Save. Ask a doc question to warm it, e.g. *"According to the guidelines, when should a CKD patient be referred to nephrology?"*

**Why it matters in the demo:** it shows Genie answering from *both* the governed tables (counts, cohorts) **and** unstructured clinical guidance in one place — "ask beyond the data."

---

## 2. Example questions for the Genie space (Configure → sample/example questions)

The app's Ask-Genie bubble already runs the four example questions as **trusted SQL** (instant, in the app). Add these same four to the **Genie space itself** so they also appear as one-click samples when leadership opens the full agent. Paste each question, and optionally the SQL as a verified example query.

> Note: this cannot be set via the public data-rooms API in this workspace (the `sample_questions` field is silently dropped) — it must be added in **Genie space → Configure → Sample questions / Example SQL**.

**Q1 — Which providers have the most undocumented CKD patients?**
```sql
SELECT assigned_provider_name AS provider, count(*) AS undocumented_ckd
FROM patient_ckd_analytics WHERE undocumented_ckd = true
GROUP BY assigned_provider_name ORDER BY undocumented_ckd DESC LIMIT 10
```

**Q2 — Of the high-risk patients, how many are also not on kidney-protective meds?**
```sql
SELECT count(*) AS high_risk_not_on_protective_meds
FROM patient_ckd_analytics
WHERE high_risk_no_nephrology = true AND on_kidney_protective_meds = false
```

**Q3 — Break down undocumented CKD patients by KDIGO stage.**
```sql
SELECT lab_kdigo_stage, count(*) AS patients
FROM patient_ckd_analytics WHERE undocumented_ckd = true
GROUP BY lab_kdigo_stage ORDER BY lab_kdigo_stage
```

**Q4 — What is the average model confidence by CKD stage?**
```sql
SELECT lab_kdigo_stage, round(avg(model_confidence),3) AS avg_confidence
FROM patient_ckd_analytics WHERE is_ckd = true
GROUP BY lab_kdigo_stage ORDER BY lab_kdigo_stage
```

*(Regenerate the PDFs from the `.md` sources with the Chrome-headless step used to build them — WeasyPrint lacks the pango/cairo libs on this Mac.)*
