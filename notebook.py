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

# DBTITLE 1,Side-by-side rows of all three tables
# MAGIC %sql
# MAGIC select "parsed_source" as tbl_source,return_id, taxpayer_id, tax_year, income, deduction from training.to6_challenge3_inputs.parsed_source  
# MAGIC union all 
# MAGIC select "returns_initial" as tbl_source, return_id, taxpayer_id, tax_year, income, deduction from training.to6_challenge3_inputs.returns_initial  
# MAGIC union all
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

# DBTITLE 1,Quality gate and rejected-records export
# Quality gate: completeness (required values non-null) and uniqueness (one row per return_id).
# The initial batch is rejected and its affected records are exported with reasons;
# the corrected batch must pass the same rules before any feature table is written.
from pathlib import Path
from datetime import datetime, timezone
import json

repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"

REQUIRED = ["return_id", "tax_year", "income", "deduction"]

def check_batch(table):
    """Return affected rows of `table`, each with the list of rules it fails."""
    null_reasons = ",\n".join(
        f"CASE WHEN {c} IS NULL THEN 'completeness: {c} is missing' END" for c in REQUIRED
    )
    rows = spark.sql(f"""
        WITH t AS (
          SELECT *, COUNT(*) OVER (PARTITION BY return_id) AS id_count FROM {table}
        )
        SELECT return_id, taxpayer_id, tax_year, income, deduction, id_count,
               filter(array(
                 {null_reasons},
                 CASE WHEN id_count > 1 THEN concat('uniqueness: return_id occurs ', id_count, ' times') END
               ), x -> x IS NOT NULL) AS reasons
        FROM t
        ORDER BY return_id, tax_year
    """).collect()
    affected = [r.asDict() for r in rows if r["reasons"]]
    summary = {
        "table": table,
        "table_version": spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
        "rows": len(rows),
        "distinct_return_ids": len({r["return_id"] for r in rows}),
        "affected_rows": len(affected),
        "passed": len(affected) == 0,
    }
    return summary, affected

checked_at = datetime.now(timezone.utc).isoformat()

# Initial batch: expected to fail; keep the evidence.
initial_summary, initial_affected = check_batch(INITIAL_TABLE)
assert not initial_summary["passed"], "Initial batch unexpectedly passed the quality rules"
rejection = {
    "decision": "REJECTED - no feature output written from this batch",
    "checked_at_utc": checked_at,
    "rules": ["completeness: return_id, tax_year, income, deduction must be non-null",
              "uniqueness: each return_id must occur exactly once"],
    **initial_summary,
    "affected_records": initial_affected,
}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/initial_rejection.json").write_text(
    json.dumps(rejection, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(rejection, indent=2))

# Corrected batch: same rules; stop the run here if it fails.
corrected_summary, corrected_affected = check_batch(CORRECTED_TABLE)
print(json.dumps({"checked_at_utc": checked_at, **corrected_summary}, indent=2))
if not corrected_summary["passed"]:
    raise ValueError(f"{CORRECTED_TABLE} failed quality checks: {corrected_affected}")
print(f"{CORRECTED_TABLE} passed; proceeding to feature output.")

# COMMAND ----------

# DBTITLE 1,Create return_features
# MAGIC %sql
# MAGIC create or replace table training.to6_c3_9120544c968c.return_features
# MAGIC select return_id, tax_year, try_divide(deduction,income) as deduction_to_income from training.to6_challenge3_inputs.returns_corrected 

# COMMAND ----------

# MAGIC %sql
# MAGIC select rf.return_id, rf.tax_year, rf.deduction_to_income from training.to6_c3_9120544c968c.return_features rf;
# MAGIC
# MAGIC -- R01 incomde is 100, deduction = 20. ratio would be 0.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Create year split tables
# MAGIC %sql
# MAGIC create or replace table training.to6_c3_9120544c968c.features_train
# MAGIC select rf.return_id, 
# MAGIC        rf.tax_year, 
# MAGIC        rf.deduction_to_income 
# MAGIC   from training.to6_c3_9120544c968c.return_features rf 
# MAGIC  where rf.tax_year = 2022;
# MAGIC  create or replace table training.to6_c3_9120544c968c.features_validation
# MAGIC select rf.return_id, 
# MAGIC        rf.tax_year, 
# MAGIC        rf.deduction_to_income 
# MAGIC   from training.to6_c3_9120544c968c.return_features rf 
# MAGIC  where rf.tax_year = 2023;
# MAGIC  create or replace table training.to6_c3_9120544c968c.features_test
# MAGIC select rf.return_id, 
# MAGIC        rf.tax_year, 
# MAGIC        rf.deduction_to_income 
# MAGIC   from training.to6_c3_9120544c968c.return_features rf 
# MAGIC  where rf.tax_year = 2024;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.to6_c3_9120544c968c.features_train;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.to6_c3_9120544c968c.features_validation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.to6_c3_9120544c968c.features_test

# COMMAND ----------

# DBTITLE 1,Export features_train
from pathlib import Path
import json

repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
feature_train = spark.sql(f"select * from {OUTPUT_SCHEMA}.features_train")
# asDict keeps NULL ratios as explicit null (toJSON drops NULL fields)
rows = [r.asDict() for r in feature_train.orderBy("return_id").collect()]
json_str = json.dumps(rows, indent=2)
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/features_train.json").write_text(
    json_str + "\n", encoding="utf-8"
)

# COMMAND ----------

# DBTITLE 1,Export features_validation
from pathlib import Path
import json

repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
feature_validation = spark.sql(f"select * from {OUTPUT_SCHEMA}.features_validation")
# asDict keeps NULL ratios as explicit null (toJSON drops NULL fields)
rows = [r.asDict() for r in feature_validation.orderBy("return_id").collect()]
json_str = json.dumps(rows, indent=2)
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/features_validation.json").write_text(
    json_str + "\n", encoding="utf-8"
)

# COMMAND ----------

# DBTITLE 1,Export features_test
from pathlib import Path
import json

repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
feature_test = spark.sql(f"select * from {OUTPUT_SCHEMA}.features_test")
# asDict keeps NULL ratios as explicit null (toJSON drops NULL fields)
rows = [r.asDict() for r in feature_test.orderBy("return_id").collect()]
json_str = json.dumps(rows, indent=2)
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/features_test.json").write_text(
    json_str + "\n", encoding="utf-8"
)

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Run snapshot and handoff checks
# Snapshot this run's saved feature and year tables (IDs and values), verify the data contract,
# and write outputs/run_snapshots/run_<UTC timestamp>.json plus outputs/return_features.json.
from pathlib import Path
from datetime import datetime, timezone
import json

repo_root = Path("/Workspace/Users/kenneth.w.patton.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"

COLUMNS = ["return_id", "tax_year", "deduction_to_income"]
TABLES = {"return_features": None, "features_train": 2022, "features_validation": 2023, "features_test": 2024}
captured_at = datetime.now(timezone.utc)

snapshot = {
    "captured_at_utc": captured_at.isoformat(),
    "notebook_url": "https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557622",
    "source_table": CORRECTED_TABLE,
    "tables": {},
}
for name in TABLES:
    fq = f"{OUTPUT_SCHEMA}.{name}"
    assert spark.table(fq).columns == COLUMNS, f"{fq} columns are {spark.table(fq).columns}"
    h = spark.sql(f"DESCRIBE HISTORY {fq} LIMIT 1").first()
    rows = [r.asDict() for r in spark.sql(f"SELECT {', '.join(COLUMNS)} FROM {fq} ORDER BY return_id").collect()]
    snapshot["tables"][name] = {
        "table": fq,
        "delta_version": h["version"],
        "written_at_utc": h["timestamp"].replace(tzinfo=timezone.utc).isoformat(),
        "operation": h["operation"],
        "row_count": len(rows),
        "rows": rows,
    }

# --- Contract checks on this run ---
source = {r["return_id"]: r.asDict() for r in spark.table(CORRECTED_TABLE).collect()}
accepted = sorted(source)
features = snapshot["tables"]["return_features"]["rows"]
feature_ids = [r["return_id"] for r in features]
assert sorted(feature_ids) == accepted, "return_features must hold every accepted return exactly once"

split_ids = []
for name, year in TABLES.items():
    if year is None:
        continue
    rows = snapshot["tables"][name]["rows"]
    assert all(r["tax_year"] == year for r in rows), f"{name} contains a row outside {year}"
    expected_rows = [r for r in features if r["tax_year"] == year]
    assert rows == expected_rows, f"{name} differs from return_features for {year}"
    split_ids += [r["return_id"] for r in rows]
assert sorted(split_ids) == accepted, "Every accepted return must appear in exactly one year table"

# Expected ratios derived independently from the source amounts.
by_id = {r["return_id"]: r for r in features}
for rid, src in source.items():
    got = by_id[rid]["deduction_to_income"]
    if src["income"] == 0:
        assert got is None, f"{rid}: zero income must give a NULL ratio, got {got}"
    else:
        assert abs(got - src["deduction"] / src["income"]) < 1e-12, f"{rid}: ratio mismatch"

snapshot["checks"] = {
    "columns_exact": COLUMNS,
    "accepted_returns": len(accepted),
    "return_features_rows": len(feature_ids),
    "year_table_rows": len(split_ids),
    "each_accepted_return_once_in_features": True,
    "each_accepted_return_once_across_year_tables": True,
    "year_assignment_correct": True,
    "expected_nonzero": {"R01": {"expected": 20 / 100, "saved": by_id["R01"]["deduction_to_income"]},
                         "R10": {"expected": 137 / 1000, "saved": by_id["R10"]["deduction_to_income"]}},
    "zero_income_null_ratio": {rid: by_id[rid]["deduction_to_income"] for rid, s in source.items() if s["income"] == 0},
}

snap_dir = repo_root / "outputs/run_snapshots"
snap_dir.mkdir(parents=True, exist_ok=True)
snap_path = snap_dir / f"run_{captured_at.strftime('%Y%m%dT%H%M%SZ')}.json"
snap_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
(repo_root / "outputs/return_features.json").write_text(json.dumps(features, indent=2) + "\n", encoding="utf-8")
print(f"Wrote {snap_path.relative_to(repo_root)}")
print(json.dumps({n: {k: v for k, v in t.items() if k != 'rows'} for n, t in snapshot['tables'].items()}, indent=2))
print(json.dumps(snapshot["checks"], indent=2))

# COMMAND ----------

# DBTITLE 1,Compare the two most recent runs
# Compare the two most recent run snapshots: identical return IDs and values in every table,
# same row counts, and a newer Delta version (tables replaced, not appended).
snap_paths = sorted((repo_root / "outputs/run_snapshots").glob("run_*.json"))
assert len(snap_paths) >= 2, "Run the whole notebook twice before comparing"
first_path, second_path = snap_paths[-2:]
first, second = (json.loads(p.read_text(encoding="utf-8")) for p in (first_path, second_path))

comparison = {
    "run_1": {"snapshot": str(first_path.relative_to(repo_root)), "captured_at_utc": first["captured_at_utc"]},
    "run_2": {"snapshot": str(second_path.relative_to(repo_root)), "captured_at_utc": second["captured_at_utc"]},
    "notebook_url": second["notebook_url"],
    "tables": {},
}
for name in TABLES:
    a, b = first["tables"][name], second["tables"][name]
    result = {
        "run_1_delta_version": a["delta_version"], "run_1_written_at_utc": a["written_at_utc"],
        "run_2_delta_version": b["delta_version"], "run_2_written_at_utc": b["written_at_utc"],
        "run_1_rows": a["row_count"], "run_2_rows": b["row_count"],
        "identical_return_ids": [r["return_id"] for r in a["rows"]] == [r["return_id"] for r in b["rows"]],
        "identical_ids_and_values": a["rows"] == b["rows"],
        "rewritten_between_runs": b["delta_version"] > a["delta_version"],
        "no_appended_copies": b["row_count"] == a["row_count"] == len({r["return_id"] for r in b["rows"]}),
        "return_ids": [r["return_id"] for r in b["rows"]],
    }
    comparison["tables"][name] = result
    assert result["identical_ids_and_values"] and result["no_appended_copies"], f"{name}: runs differ"
    assert result["rewritten_between_runs"], f"{name}: not rewritten between the two snapshots"
comparison["all_tables_identical"] = True

(repo_root / "outputs/run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
print(json.dumps({k: v for k, v in comparison.items() if k != "tables"}, indent=2))
print(json.dumps({n: {k: v for k, v in t.items() if k != 'return_ids'} for n, t in comparison['tables'].items()}, indent=2))