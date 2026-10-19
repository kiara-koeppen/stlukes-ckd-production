# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Genie Agent over the governed CKD data
# MAGIC
# MAGIC Creates (or updates, matched by title) a Genie space on the Metric View + analytics view + gold
# MAGIC table, with curated instructions, sample questions, and a benchmark set of question -> expected SQL.
# MAGIC The definition lives in `genie/space_template.json` so the agent is versioned with the code.
# MAGIC The App embeds this agent (Conversation API) and looks it up by title.

# COMMAND ----------
dbutils.widgets.text("catalog", "slhs_test1", "Target catalog")
dbutils.widgets.text("schema", "ckd_febar", "Target schema")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse")
dbutils.widgets.text("genie_title", "CKD Identification & Risk Flagging (FE Bar)", "Genie space title")
CATALOG, SCHEMA, WH, TITLE = (dbutils.widgets.get(k) for k in ("catalog", "schema", "warehouse_id", "genie_title"))

# COMMAND ----------
import json, os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
here = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
with open(f"/Workspace{here}/../genie/space_template.json") as f:
    serialized = f.read().replace("{catalog}", CATALOG).replace("{schema}", SCHEMA)
body = {"title": TITLE, "warehouse_id": WH, "serialized_space": serialized,
        "description": "Ask about CKD patients: lab-derived KDIGO stage vs documented stage, undocumented / mis-staged, "
                       "high risk (advanced, no nephrology), CKD caught only by the clinical notes, and the model's "
                       "suggested stage. Counts come from the ckd_metrics metric view."}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create or update (idempotent by title)

# COMMAND ----------
spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
match = [s for s in spaces if s.get("title") == TITLE]
if match:
    space_id = match[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print("updated Genie space", space_id)
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    print("created Genie space", space_id)
dbutils.jobs.taskValues.set("genie_space_id", space_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execution evidence: ask the live agent the headline question

# COMMAND ----------
import time
msg = w.genie.start_conversation_and_wait(space_id, "How many patients have undocumented CKD?")
for a in msg.attachments or []:
    if a.text: print("Genie:", a.text.content)
    if a.query:
        print("SQL:", a.query.query)
        res = w.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.message_id, a.attachment_id)
        print("Result:", res.statement_response.result.data_array)
