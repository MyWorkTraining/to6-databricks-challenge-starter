# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Your investigation here.
# MAGIC
# MAGIC There are 3 tables and the source lineage. The tables appear to contain sample data representative of tax processing data from tax years 2022, 2023, and 2024.
# MAGIC
# MAGIC Returns initial has null values in the income field, so it appears to be the raw-ish data input. However, the source lineage and table descriptions suggest it is the downstream results of data from parsed_course. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# Your checks and decision here.
#I used the catalog view for the initial review of data.

#There are 3 tables and the source lineage. The tables appear to contain sample data representative of tax processing data from tax years 2022, 2023, and 2024.


# return_id appears to be the primary unique value for each record. 
#returns_initial has null values in the income field, so it appears to be the raw-ish data input. However, the source lineage and table descriptions suggest it is the downstream results of data from parsed_sourse. It is not clear how a zero in parsed_source became a null value in the downstream table.
# returns_initial also contains a duplicate row for return R02, taxpayer_id F1 for year 2022. If this were a real project I would want to confirm this is in fact the correct lineage of the data. returns_initial does not appear to be a good choice for modeling off of.

#Based on initial review of the limited sample data, results_corrected appears to be identical to parsed_source, so there is not an obvious benefit to using one or the other as the source for model creation.



# COMMAND ----------

# MAGIC %sql
# MAGIC select "parsed_source" as tbl_source,return_id, taxpayer_id, tax_year, income, deduction from training.to6_challenge3_inputs.parsed_source  
# MAGIC union 
# MAGIC select "returns_initial" as tbl_source, return_id, taxpayer_id, tax_year, income, deduction from training.to6_challenge3_inputs.returns_initial  
# MAGIC union
# MAGIC select "returns_corrected" as tbl_source,return_id, taxpayer_id, tax_year, income, deduction from training.to6_challenge3_inputs.returns_corrected  
# MAGIC order by 2, 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from training.to6_challenge3_inputs.parsed_source 
# MAGIC select "parsed_source" as tbl_source, tax_year, avg(income), avg(deduction), count(*) from training.to6_challenge3_inputs.parsed_source  group by tax_year
# MAGIC union 
# MAGIC select "returns_initial" as tbl_source, tax_year, avg(income), avg(deduction), count(*) from training.to6_challenge3_inputs.returns_initial  group by tax_year
# MAGIC union
# MAGIC select "returns_corrected" as tbl_source, tax_year, avg(income), avg(deduction), count(*) from training.to6_challenge3_inputs.returns_corrected  group by tax_year
# MAGIC order by 1,2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select src.* 
# MAGIC   from training.to6_challenge3_inputs.parsed_source src
# MAGIC         inner join training.to6_challenge3_inputs.returns_corrected cor
# MAGIC         on src.return_id = cor.return_id
# MAGIC  where src.taxpayer_id <> cor.taxpayer_id
# MAGIC     or src.tax_year <> cor.tax_year
# MAGIC     or src.income <> cor.income
# MAGIC     or src.deduction <> cor.deduction
# MAGIC     or src.return_id <> cor.return_id;
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table training.to6_c3_9120544c968c.return_features
# MAGIC select return_id, tax_year, try_divide(deduction,income) as deduction_to_income from training.to6_challenge3_inputs.returns_corrected 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.to6_c3_9120544c968c.return_features;
# MAGIC
# MAGIC -- R01 incomde is 100, deduction = 20. ratio would be 0.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.