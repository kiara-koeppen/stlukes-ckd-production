# Databricks notebook source
# MAGIC %md
# MAGIC # 03b - Serve the CKD models (Model Serving)
# MAGIC Deploys the two models registered by `train_ckd_stage_model.py` to real-time endpoints:
# MAGIC * **stage model** (`{catalog}.{schema}.ckd_stage_model`): suggests the KDIGO stage
# MAGIC * **fusion risk model** (`{catalog}.{schema}.ckd_risk_model`): labs + AI notes signal -> CKD risk;
# MAGIC   the App calls this one live in its notes analyzer
# MAGIC
# MAGIC Endpoints scale to zero when idle (cost) and cold-start on the next request (keep warm in production).

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1"); dbutils.widgets.text("schema", "ckd_febar")
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema")
MODEL = f"{CATALOG}.{SCHEMA}.ckd_stage_model"
RISK_MODEL = f"{CATALOG}.{SCHEMA}.ckd_risk_model"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Serve both models (Model Serving, scale-to-zero)
# MAGIC The App calls the fusion risk model in real time ("score this pasted note"), and the stage model
# MAGIC backs on-demand stage suggestions. Each endpoint is created on first run and updated to the newest
# MAGIC registered version afterwards, so re-running the job redeploys cleanly.

# COMMAND ----------
from datetime import timedelta
from databricks.sdk import WorkspaceClient
import mlflow
mlflow.set_registry_uri("databricks-uc")
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.tracking import MlflowClient

dbutils.widgets.text("stage_endpoint", "ckd-stage-model-febar")
dbutils.widgets.text("risk_endpoint", "ckd-risk-model-febar")
w = WorkspaceClient(); mc = MlflowClient()

def latest_version(name):
    return str(max(int(v.version) for v in mc.search_model_versions(f"name='{name}'")))

def deploy(endpoint, model):
    entity = ServedEntityInput(entity_name=model, entity_version=latest_version(model),
                               workload_size="Small", scale_to_zero_enabled=True)
    existing = {e.name for e in w.serving_endpoints.list()}
    if endpoint in existing:
        w.serving_endpoints.update_config_and_wait(endpoint, served_entities=[entity], timeout=timedelta(minutes=45))
    else:
        w.serving_endpoints.create_and_wait(endpoint, config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity]),
                                            timeout=timedelta(minutes=45))
    print("endpoint", endpoint, "->", model, "v" + entity.entity_version, w.serving_endpoints.get(endpoint).state.ready)

deploy(dbutils.widgets.get("stage_endpoint"), MODEL)
deploy(dbutils.widgets.get("risk_endpoint"), RISK_MODEL)
