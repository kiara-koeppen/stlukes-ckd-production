# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Generate the synthetic patient population (the "source EHR")
# MAGIC
# MAGIC **What this does:** invents a 2,000-patient primary-care panel from scratch, shaped like St. Luke's
# MAGIC de-identified CKD sample (23 columns: demographics, problem list, documented CKD stage, three
# MAGIC outpatient creatinine + eGFR draws, microalbumin, nephrology status, key medications) and grounded in
# MAGIC the KDIGO staging rules. No real patient data is used.
# MAGIC
# MAGIC **Planted signal** (so every downstream layer has something real to find):
# MAGIC * ~500 **care gap**: CKD by labs (stage 3+) but no CKD documented in Epic
# MAGIC * ~150 **high risk**: stage 4/5 and not seeing nephrology
# MAGIC * ~200 **mis-staged**: documented stage is one stage off the true stage
# MAGIC * the rest: correctly documented controls
# MAGIC
# MAGIC **Deterministic:** seeded (`seed=42`), so every run produces the same patients and the same numbers.
# MAGIC The output table, `source_patient_registry`, plays the role of the source system. Task 01 turns it
# MAGIC into raw Epic-shaped extract files for the Lakeflow pipeline to ingest.

# COMMAND ----------
# MAGIC %pip install "faker>=20" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
dbutils.widgets.text("n_patients", "2000", "Panel size")
dbutils.widgets.text("seed", "42", "Random seed")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
N, SEED = int(dbutils.widgets.get("n_patients")), int(dbutils.widgets.get("seed"))
CARE_GAP, HIGH_RISK, MIS_STAGED = 500, 150, 200
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Clinical bands per true stage
# MAGIC eGFR and creatinine ranges follow KDIGO stage boundaries; higher stages are more likely to have
# MAGIC elevated albuminuria and nephrology involvement.

# COMMAND ----------
import random, datetime as dt
from faker import Faker

STAGES = ["None", "CKD 2", "CKD 3a", "CKD 3b", "CKD 4", "CKD 5"]
GFR_BAND = {"None": (90, 118), "CKD 2": (60, 89), "CKD 3a": (45, 59),
            "CKD 3b": (30, 44), "CKD 4": (15, 29), "CKD 5": (5, 14)}
CREAT_BAND = {"None": (0.6, 1.0), "CKD 2": (0.9, 1.2), "CKD 3a": (1.1, 1.4),
              "CKD 3b": (1.3, 1.8), "CKD 4": (1.8, 3.5), "CKD 5": (3.5, 11.0)}
ADJ = {"CKD 2": ["None", "CKD 3a"], "CKD 3a": ["CKD 2", "CKD 3b"],      # one stage off, for mis-staging
       "CKD 3b": ["CKD 3a", "CKD 4"], "CKD 4": ["CKD 3b", "CKD 5"], "CKD 5": ["CKD 4"]}
COMORBID = ["Diabetes Mellitus", "Hypertension", "Atrial Fibrillation",
            "Heart Failure", "COPD", "Hyperlipidemia", "Obesity", "Anemia"]
CLINIC_NAMES = ["UNKNOWN", "ST. LUKE'S CLINIC MAGIC VALLEY", "ST. LUKE'S CLINIC BOISE",
                "ST. LUKE'S CLINIC MERIDIAN", "ST. LUKE'S CLINIC NAMPA"]

def id_token():
    return "ACO-" + "".join(random.choices("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ", k=11))

def three_creatinines(lo_hi):
    """3 sustained outpatient creatinines: an in-band base plus ~10% noise, floored at 0.3."""
    lo, hi = lo_hi
    base = random.uniform(lo, hi)
    noise = max(0.08, base * 0.10)
    return [round(max(0.3, min(hi + 0.3, base + random.uniform(-noise, noise))), 2) for _ in range(3)]

def three_gfr(band):
    lo, hi = band
    base = random.uniform(lo, hi)
    return [int(round(min(hi + 4, max(1, base + random.uniform(-4, 4))))) for _ in range(3)]

def microalbumin(true_stage):
    """Higher stage -> more likely measured and elevated (A1 < 30, A2 30-300, A3 > 300 mg/g)."""
    r = random.random()
    sev = {"None": 0.1, "CKD 2": 0.25, "CKD 3a": 0.45, "CKD 3b": 0.6, "CKD 4": 0.8, "CKD 5": 0.9}[true_stage]
    if r > sev:
        return None, None, None  # not recently measured
    p = random.random()
    if p < 1 - sev:
        val, cat = round(random.uniform(3, 29), 1), "A1"
    elif p < 1 - sev / 3:
        val, cat = round(random.uniform(30, 300), 1), "A2"
    else:
        val, cat = round(random.uniform(300, 900), 1), "A3"
    return val, cat, dt.date(2025, 1, 1) + dt.timedelta(days=random.randint(0, 500))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate the panel
# MAGIC Cohort roles are assigned first, then each patient's true stage, labs, documentation, comorbidities,
# MAGIC and medications follow from the role. The note-sampling draws at the end of each loop are kept so the
# MAGIC random sequence (and therefore every patient) matches the original St. Luke's workshop data exactly;
# MAGIC this build writes its own clinical notes in task 01.

# COMMAND ----------
random.seed(SEED)
fake = Faker(); Faker.seed(SEED)
providers = [f"{fake.last_name().upper()}, {fake.first_name().upper()} {random.choice('ABCDEFGHJKLM')}" for _ in range(45)]

roles = ["care_gap"] * CARE_GAP + ["high_risk"] * HIGH_RISK + ["mis_staged"] * MIS_STAGED
roles += ["control"] * (N - len(roles))
random.shuffle(roles)

rows, note_file_budget = [], 45
for i, role in enumerate(roles, start=1):
    if role == "care_gap":
        true_stage = random.choices(["CKD 3a", "CKD 3b", "CKD 4"], [0.4, 0.4, 0.2])[0]
    elif role == "high_risk":
        true_stage = random.choices(["CKD 4", "CKD 5"], [0.6, 0.4])[0]
    elif role == "mis_staged":
        true_stage = random.choices(["CKD 2", "CKD 3a", "CKD 3b", "CKD 4"], [0.2, 0.35, 0.3, 0.15])[0]
    else:
        true_stage = random.choices(STAGES, [0.42, 0.13, 0.16, 0.13, 0.11, 0.05])[0]

    gfrs = three_gfr(GFR_BAND[true_stage])
    creats = three_creatinines(CREAT_BAND[true_stage])
    mval, mcat, mdate = microalbumin(true_stage)
    has_ckd = true_stage != "None"

    if role == "high_risk":
        sees_nephro = False
    else:
        p = {"None": 0.02, "CKD 2": 0.05, "CKD 3a": 0.15, "CKD 3b": 0.35, "CKD 4": 0.7, "CKD 5": 0.85}[true_stage]
        sees_nephro = random.random() < p

    # what Epic says (documented) vs the clinician-ascertained truth
    if role == "care_gap":
        documented = random.choice(["No", "None"])
    elif role == "mis_staged" and true_stage in ADJ:
        documented = random.choice(ADJ[true_stage])
        if documented == "None":
            documented = "No"
    else:
        documented = "No" if true_stage == "None" else true_stage
    ckd_in_problem_list = documented not in ("No", "None")

    conds = random.sample(COMORBID, random.randint(1, 5))
    if ckd_in_problem_list:
        conds = ["Chronic Kidney Disease"] + conds
    meds = []
    if "Diabetes Mellitus" in conds or has_ckd:          # SGLT2 / GLP-1 more likely if diabetic or CKD
        if random.random() < 0.4: meds.append("Empagliflozin")
        if random.random() < 0.35: meds.append("Semaglutide")

    sex = random.choice(["M", "F"])
    age = random.randint(45, 92)
    dob = dt.date(2026 - age, random.randint(1, 12), random.randint(1, 28))
    pid = id_token()
    provider = random.choice(providers) if random.random() > 0.15 else random.choice(CLINIC_NAMES)

    rows.append((pid, i, sex, dob, age, provider, ", ".join(conds), len(conds), ckd_in_problem_list, documented,
                 creats[0], creats[1], creats[2], gfrs[0], gfrs[1], gfrs[2], mval, mcat, mdate,
                 has_ckd, true_stage, sees_nephro, ", ".join(meds) or None))

    # keep the original generator's note-sampling draws (note date, author, 45 note files) so the
    # random sequence is unchanged
    if role == "care_gap" or random.random() < 0.35:
        random.randint(0, 400); random.choice(providers)
        if note_file_budget > 0 and (role == "care_gap" or random.random() < 0.2):
            note_file_budget -= 1

print("generated", len(rows), "patients")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write the source registry (typed, documented, keyed)

# COMMAND ----------
from decimal import Decimal
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DateType, BooleanType, DecimalType)
schema = StructType([
    StructField("patient_id", StringType()), StructField("patient_num", IntegerType()), StructField("sex", StringType()),
    StructField("dob", DateType()), StructField("age", IntegerType()), StructField("assigned_provider_name", StringType()),
    StructField("chronic_conditions", StringType()), StructField("chronic_condition_count", IntegerType()),
    StructField("ckd_in_problem_list", BooleanType()), StructField("documented_ckd", StringType()),
    StructField("creatinine_1", DecimalType(5, 2)), StructField("creatinine_2", DecimalType(5, 2)), StructField("creatinine_3", DecimalType(5, 2)),
    StructField("gfr_1", IntegerType()), StructField("gfr_2", IntegerType()), StructField("gfr_3", IntegerType()),
    StructField("microalbumin_value", DecimalType(6, 1)), StructField("microalbumin_category", StringType()),
    StructField("microalbumin_date", DateType()), StructField("has_ckd", BooleanType()),
    StructField("actual_ckd_stage", StringType()), StructField("sees_nephrology", BooleanType()), StructField("key_meds", StringType())])
def dec(v, q): return None if v is None else Decimal(str(v)).quantize(Decimal(q))
typed = [r[:10] + (dec(r[10], "0.01"), dec(r[11], "0.01"), dec(r[12], "0.01")) + r[13:16]
         + (dec(r[16], "0.1"),) + r[17:] for r in rows]
TABLE = f"{CATALOG}.{SCHEMA}.source_patient_registry"
spark.createDataFrame(typed, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

comments = {
    "patient_id": "De-identified synthetic patient key.", "patient_num": "Sequential patient number.",
    "sex": "Patient sex (F/M).", "dob": "Synthetic date of birth.", "age": "Age in years.",
    "assigned_provider_name": "Assigned primary care provider or clinic (synthetic names).",
    "chronic_conditions": "Problem-list chronic conditions, comma separated.",
    "chronic_condition_count": "Number of chronic conditions on the problem list.",
    "ckd_in_problem_list": "TRUE when CKD is on the EHR problem list.",
    "documented_ckd": "CKD stage documented in Epic (No/None when undocumented).",
    "creatinine_1": "Most recent outpatient serum creatinine (mg/dL).", "creatinine_2": "Second most recent outpatient creatinine (mg/dL).",
    "creatinine_3": "Third most recent outpatient creatinine (mg/dL).", "gfr_1": "Most recent outpatient eGFR (mL/min/1.73m2).",
    "gfr_2": "Second most recent outpatient eGFR.", "gfr_3": "Third most recent outpatient eGFR.",
    "microalbumin_value": "Urine microalbumin (mg/g); NULL when not recently measured.",
    "microalbumin_category": "KDIGO albuminuria category A1/A2/A3.", "microalbumin_date": "Date of the microalbumin result.",
    "has_ckd": "Clinician-ascertained ground truth: patient has CKD.", "actual_ckd_stage": "Clinician-ascertained CKD stage (ground truth).",
    "sees_nephrology": "TRUE when followed by nephrology.", "key_meds": "Kidney-protective medications (SGLT2 / GLP-1), comma separated."}
spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Synthetic source EHR registry (seeded, no PHI): one row per patient with documentation, serial labs, ground truth, and meds. Input to the raw-extract generator.'")
for c, t in comments.items():
    spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {c} COMMENT '{t}'")
spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN patient_id SET NOT NULL")
spark.sql(f"ALTER TABLE {TABLE} DROP CONSTRAINT IF EXISTS source_patient_registry_pk")
spark.sql(f"ALTER TABLE {TABLE} ADD CONSTRAINT source_patient_registry_pk PRIMARY KEY (patient_id)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execution evidence: the planted signal

# COMMAND ----------
display(spark.sql(f"""SELECT count(*) AS patients,
  sum(CASE WHEN actual_ckd_stage IN ('CKD 3a','CKD 3b','CKD 4','CKD 5') AND documented_ckd IN ('No','None') THEN 1 ELSE 0 END) AS care_gap,
  sum(CASE WHEN actual_ckd_stage IN ('CKD 4','CKD 5') AND NOT sees_nephrology THEN 1 ELSE 0 END) AS high_risk_no_nephrology,
  sum(CASE WHEN has_ckd THEN 1 ELSE 0 END) AS has_ckd
FROM {TABLE}"""))
