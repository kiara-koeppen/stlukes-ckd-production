# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Lakebase operational serving + App permissions
# MAGIC
# MAGIC 1. **Synced table** `patient_worklist` (Delta) -> Lakebase Postgres `{schema}.patient_worklist_synced`
# MAGIC    (TRIGGERED, incremental via change data feed). The App reads its worklist and KPI tiles from
# MAGIC    Postgres instead of scanning the warehouse.
# MAGIC 2. **Postgres grants** so the App's service principal role can read the synced table. The App
# MAGIC    creates and owns its own OLTP table (`app.epic_writeback_queue`) for clinician approvals.
# MAGIC 3. **Unity Catalog + platform grants** for the App service principal: read the governed tables,
# MAGIC    query both serving endpoints, run the Genie agent. No admin rights.

# COMMAND ----------
# MAGIC %pip install "psycopg[binary]>=3.1" --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
dbutils.widgets.text("lakebase_project", "ckd-febar", "Lakebase project id")
dbutils.widgets.text("app_name", "ckd-risk-worklist-febar", "App name")
dbutils.widgets.text("stage_endpoint", "ckd-stage-model-febar")
dbutils.widgets.text("risk_endpoint", "ckd-risk-model-febar")
dbutils.widgets.text("genie_space_id", "", "Genie space id (from task 06)")
CATALOG, SCHEMA, PROJECT, APP = (dbutils.widgets.get(k) for k in ("catalog", "schema", "lakebase_project", "app_name"))
S = f"{CATALOG}.{SCHEMA}"
BRANCH = f"projects/{PROJECT}/branches/production"
SYNCED = f"{S}.patient_worklist_synced"

# COMMAND ----------
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (SyncedTable, SyncedTableSyncedTableSpec,
                                             SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as Policy)
w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Synced table: create once, then trigger an incremental sync on every run

# COMMAND ----------
try:
    st_ = w.postgres.get_synced_table(f"synced_tables/{SYNCED}")
    print("synced table exists:", st_.status.detailed_state)
    pipeline_id = st_.status.pipeline_id
    w.pipelines.start_update(pipeline_id)
    print("triggered sync pipeline", pipeline_id)
except Exception as e:
    if "does not exist" not in str(e).lower() and "not found" not in str(e).lower():
        raise
    st_ = w.postgres.create_synced_table(
        synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
            source_table_full_name=f"{S}.patient_worklist", branch=BRANCH,
            primary_key_columns=["patient_id"], scheduling_policy=Policy.TRIGGERED,
            postgres_database="databricks_postgres", create_database_objects_if_missing=True)),
        synced_table_id=SYNCED).wait()
    print("created synced table", st_.name)

# wait for the sync to land (ONLINE_NO_PENDING_UPDATE = caught up with the source)
time.sleep(30)
for _ in range(90):
    st_ = w.postgres.get_synced_table(f"synced_tables/{SYNCED}")
    state = st_.status.detailed_state.value if st_.status.detailed_state else "UNKNOWN"
    if "FAILED" in state:
        raise RuntimeError(f"synced table sync failed: {state} {st_.status.message}")
    if state == "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE":
        break
    time.sleep(20)
print("sync state:", state, "| last sync:", st_.status.last_sync_time, "|", st_.status.message)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Postgres: let the App role read the synced worklist

# COMMAND ----------
import psycopg
app = w.apps.get(APP)
sp = app.service_principal_client_id
ep = w.postgres.list_endpoints(BRANCH).__iter__().__next__()
host = ep.status.hosts.host
token = w.postgres.generate_database_credential(endpoint=ep.name).token
me = w.current_user.me().user_name
with psycopg.connect(host=host, dbname="databricks_postgres", user=me, password=token, sslmode="require") as conn:
    with conn.cursor() as cur:
        cur.execute(f'GRANT USAGE ON SCHEMA "{SCHEMA}" TO "{sp}"')
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{SCHEMA}" TO "{sp}"')
        cur.execute(f'SELECT count(*) FROM "{SCHEMA}".patient_worklist_synced')
        print("rows in Lakebase patient_worklist_synced:", cur.fetchone()[0])
        # Native OLTP table for clinician decisions (the Epic integration point). The App inserts and reads.
        cur.execute("""CREATE SCHEMA IF NOT EXISTS app;
          CREATE TABLE IF NOT EXISTS app.epic_writeback_queue (
            id BIGSERIAL PRIMARY KEY,
            patient_id TEXT NOT NULL,
            confirmed_stage TEXT NOT NULL,
            refer_nephrology BOOLEAN NOT NULL DEFAULT false,
            source TEXT NOT NULL DEFAULT 'worklist',
            reviewed_by TEXT NOT NULL,
            reviewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            status TEXT NOT NULL DEFAULT 'queued');
          COMMENT ON TABLE app.epic_writeback_queue IS 'Clinician-approved CKD stage decisions awaiting Epic write-back (human in the loop).';
          CREATE INDEX IF NOT EXISTS epic_writeback_queue_status_idx ON app.epic_writeback_queue (status, reviewed_at DESC);""")
        cur.execute(f'GRANT USAGE ON SCHEMA app TO "{sp}"')
        cur.execute(f'GRANT SELECT, INSERT, UPDATE ON app.epic_writeback_queue TO "{sp}"')
        cur.execute(f'GRANT USAGE ON SEQUENCE app.epic_writeback_queue_id_seq TO "{sp}"')
        print("write-back queue ready: app.epic_writeback_queue (app role can insert/read)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unity Catalog + endpoint + Genie permissions for the App service principal

# COMMAND ----------
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp}`")
spark.sql(f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {S} TO `{sp}`")
for ep_name in (dbutils.widgets.get("stage_endpoint"), dbutils.widgets.get("risk_endpoint")):
    eid = w.serving_endpoints.get(ep_name).id
    w.api_client.do("PATCH", f"/api/2.0/permissions/serving-endpoints/{eid}",
                    body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_QUERY"}]})
genie_id = dbutils.widgets.get("genie_space_id")
if genie_id:
    w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{genie_id}",
                    body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_RUN"}]})
print("granted app SP", sp, "UC read on", S, "+ CAN_QUERY on endpoints + CAN_RUN on Genie", genie_id)
