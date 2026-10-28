# Databricks notebook source
# MAGIC %md
# MAGIC # CKD stage-suggestion model (MLflow)
# MAGIC Trains a classifier that suggests the clinician-assigned KDIGO stage from lab values and
# MAGIC **trends over time** (per Yutong's ask: "suggest likely CKD stage based on labs and trends").
# MAGIC Logs to MLflow, registers to Unity Catalog Model Registry, evaluates vs the clinician stage,
# MAGIC and batch-scores every patient into `patient_ckd_scored`. `serve_models.py` (the next job task)
# MAGIC deploys both models to Model Serving for real-time scoring from the App.

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1"); dbutils.widgets.text("schema", "ckd_febar")
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## CKD-risk model — FUSION of structured labs + the AI notes signal
# MAGIC The clinician's ask taken further: CKD is often *hidden* — at eGFR>=60 it requires a damage
# MAGIC marker (proteinuria) that frequently lives in the **clinical notes**, not a structured lab.
# MAGIC So we take the LLM-derived notes CKD-risk signal (`patient_note_signals`) and feed it, **with the
# MAGIC labs**, into a classifier that predicts `has_ckd`. We compare a **labs-only baseline** (eGFR-based,
# MAGIC no albuminuria) against the **labs+notes fusion**, and flag patients the notes *caught* that the
# MAGIC labs alone would have missed.

# COMMAND ----------
from sklearn.metrics import roc_auc_score
rdf = spark.table(f"{CATALOG}.{SCHEMA}.patient_ckd_gold").select(
    "patient_id","age","mean_egfr","latest_egfr","prior_egfr",
    "egfr_trend_recent_minus_prior","mean_creatinine","has_ckd").toPandas()
notes = spark.table(f"{CATALOG}.{SCHEMA}.patient_note_signals").select("patient_id","notes_ckd_risk_ord").toPandas()
rdf = rdf.merge(notes, on="patient_id", how="left")
rdf["notes_ckd_risk_ord"] = pd.to_numeric(rdf["notes_ckd_risk_ord"], errors="coerce").fillna(0).astype(int)
rdf["y"] = rdf["has_ckd"].astype(int)
LAB    = ["age","mean_egfr","latest_egfr","prior_egfr","egfr_trend_recent_minus_prior","mean_creatinine"]  # no albuminuria: the damage marker lives in the notes
FUSION = LAB + ["notes_ckd_risk_ord"]
Xtr,Xte,ytr,yte = train_test_split(rdf, rdf["y"], test_size=0.25, random_state=7, stratify=rdf["y"])
def fit_eval(feats):
    m = RandomForestClassifier(n_estimators=300,max_depth=10,random_state=7,class_weight="balanced").fit(Xtr[feats],ytr)
    p = m.predict_proba(Xte[feats])[:,1]; pred=(p>=0.5).astype(int); border = Xte["mean_egfr"]>=60
    met = {"auc":round(roc_auc_score(yte,p),3),"f1":round(f1_score(yte,pred),3),"acc":round(accuracy_score(yte,pred),3),
           "borderline_recall": round(float(((pred[border.values]==1)&(yte[border.values]==1)).sum())/max(1,int((yte[border.values]==1).sum())),3)}
    if border.sum()>5 and yte[border.values].nunique()>1: met["borderline_auc"]=round(roc_auc_score(yte[border.values],p[border.values]),3)
    return m, met
base_m, base_metrics = fit_eval(LAB)
fus_m,  fus_metrics  = fit_eval(FUSION)
print("BASELINE labs-only :", base_metrics)
print("FUSION labs+notes  :", fus_metrics)

# COMMAND ----------
mlflow.set_experiment(f"/Users/{spark.sql('select current_user()').first()[0]}/ckd_risk_model")
RISK_MODEL = f"{CATALOG}.{SCHEMA}.ckd_risk_model"
with mlflow.start_run(run_name="ckd_risk_fusion"):
    m = RandomForestClassifier(n_estimators=300,max_depth=10,random_state=7,class_weight="balanced").fit(rdf[FUSION],rdf["y"])
    mlflow.log_metrics({f"baseline_{k}":v for k,v in base_metrics.items()})
    mlflow.log_metrics({f"fusion_{k}":v for k,v in fus_metrics.items()})
    imp = {k:round(float(v),3) for k,v in zip(FUSION, m.feature_importances_)}
    mlflow.log_dict(imp,"feature_importance.json"); print("feature importance:", imp)
    sig = mlflow.models.infer_signature(rdf[FUSION], m.predict(rdf[FUSION]))
    mlflow.sklearn.log_model(m,"model",signature=sig,input_example=rdf[FUSION].head(3),registered_model_name=RISK_MODEL)
print("registered:", RISK_MODEL)

# COMMAND ----------
# Batch-score CKD risk (fusion) + what labs-only would have said → "caught_by_notes"
rdf["ckd_risk_score"]     = m.predict_proba(rdf[FUSION])[:,1].round(3)
rdf["ckd_risk_pred"]      = (rdf["ckd_risk_score"]>=0.5)
rdf["ckd_risk_labs_only"] = base_m.predict_proba(rdf[LAB])[:,1].round(3)
rdf["caught_by_notes"]    = (rdf["ckd_risk_pred"]) & (rdf["ckd_risk_labs_only"]<0.5)
out = spark.createDataFrame(rdf[["patient_id","ckd_risk_score","ckd_risk_pred","ckd_risk_labs_only","caught_by_notes","notes_ckd_risk_ord"]])
out.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CATALOG}.{SCHEMA}.patient_ckd_risk_scored")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.patient_ckd_risk_scored SET TBLPROPERTIES ('comment'='Fusion CKD-risk: score/pred from labs+AI notes signal, labs-only comparison, and caught_by_notes (labs missed, notes caught).')")
print("wrote", f"{CATALOG}.{SCHEMA}.patient_ckd_risk_scored", out.count(), "| caught_by_notes:", int(rdf["caught_by_notes"].sum()))
