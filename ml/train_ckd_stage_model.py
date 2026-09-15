# Databricks notebook source
# MAGIC %md
# MAGIC # CKD stage-suggestion model (MLflow)
# MAGIC Trains a classifier that suggests the clinician-assigned KDIGO stage from lab values and
# MAGIC **trends over time** (per Yutong's ask: "suggest likely CKD stage based on labs and trends").
# MAGIC Logs to MLflow, registers to Unity Catalog Model Registry, evaluates vs the clinician stage,
# MAGIC and batch-scores every patient into `ckd_prod.patient_ckd_scored`. A serving endpoint (created
# MAGIC separately) exposes it for real-time scoring from the App.

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1"); dbutils.widgets.text("schema", "ckd_prod")
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema")
MODEL = f"{CATALOG}.{SCHEMA}.ckd_stage_model"

# COMMAND ----------
import mlflow, mlflow.sklearn, pandas as pd, numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------
# Features: lab levels + TRENDS + demographics + albuminuria + protective meds. Target: clinician stage.
df = spark.table(f"{CATALOG}.{SCHEMA}.patient_ckd_gold").select(
    "patient_id","age","sex","mean_egfr","latest_egfr","prior_egfr",
    "egfr_trend_recent_minus_prior","mean_creatinine","microalbumin_value",
    "albuminuria_cat","on_kidney_protective_meds","actual_ckd_stage").toPandas()
df["microalbumin_value"] = pd.to_numeric(df["microalbumin_value"], errors="coerce").fillna(0.0)
df["on_kidney_protective_meds"] = df["on_kidney_protective_meds"].astype(int)
y = df["actual_ckd_stage"].fillna("None")
num = ["age","mean_egfr","latest_egfr","prior_egfr","egfr_trend_recent_minus_prior","mean_creatinine","microalbumin_value","on_kidney_protective_meds"]
cat = ["sex","albuminuria_cat"]
X = df[num+cat]
Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.2,random_state=42,stratify=y)

pipe = Pipeline([
  ("prep", ColumnTransformer([("n",StandardScaler(),num),("c",OneHotEncoder(handle_unknown="ignore"),cat)])),
  ("clf", RandomForestClassifier(n_estimators=300,max_depth=12,random_state=42,class_weight="balanced")),
])

# COMMAND ----------
mlflow.set_experiment(f"/Users/{spark.sql('select current_user()').first()[0]}/ckd_stage_model")
with mlflow.start_run(run_name="ckd_stage_rf") as run:
    pipe.fit(Xtr,ytr)
    pred = pipe.predict(Xte)
    acc = accuracy_score(yte,pred); f1 = f1_score(yte,pred,average="macro")
    mlflow.log_metrics({"accuracy":acc,"macro_f1":f1})
    mlflow.log_params({"model":"RandomForest","n_estimators":300,"max_depth":12,"n_features":len(num+cat)})
    print("accuracy",round(acc,3),"macro_f1",round(f1,3))
    print(classification_report(yte,pred))
    sig = mlflow.models.infer_signature(Xtr, pipe.predict(Xtr))
    mlflow.sklearn.log_model(pipe,"model",signature=sig,input_example=Xtr.head(3),
                             registered_model_name=MODEL)
print("registered:",MODEL)

# COMMAND ----------
# Batch score every patient: suggested stage + confidence, agreement with clinician stage.
proba = pipe.predict_proba(X); classes = pipe.named_steps["clf"].classes_
scored = df[["patient_id","actual_ckd_stage"]].copy()
scored["suggested_stage"] = pipe.predict(X)
scored["confidence"] = proba.max(axis=1).round(3)
scored["model_agrees_with_documented"] = (scored["suggested_stage"]==scored["actual_ckd_stage"])
sdf = spark.createDataFrame(scored)
sdf.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CATALOG}.{SCHEMA}.patient_ckd_scored")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.patient_ckd_scored SET TBLPROPERTIES ('comment'='ML stage suggestions: suggested_stage + confidence per patient from the CKD stage model.')")
print("wrote", f"{CATALOG}.{SCHEMA}.patient_ckd_scored", sdf.count())
