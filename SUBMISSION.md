# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

**Inputs (read-only, schema `training.to6_challenge3_inputs`)**

| Table | Role | Catalog description |
| --- | --- | --- |
| `training.to6_challenge3_inputs.returns_initial` | First delivery (rejected) | "Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output." |
| `training.to6_challenge3_inputs.returns_corrected` | Replacement delivery (used for final run) | "Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness." |
| `training.to6_challenge3_inputs.parsed_source` | Upstream source of both deliveries | "Synthetic parsed source: one row is one return; return_id identifies the return, taxpayer_id identifies a filer who may recur across years. Nonnegative amounts; zero income is valid." |
| `training.to6_challenge3_inputs.source_lineage` | Supplied lineage view | Maps both delivery tables to upstream `parsed_source` (initial: "Synthetic initial delivery transformation; validation required"; corrected: "Synthetic corrected delivery; same validation rules"). |

**Outputs (assigned schema `training.to6_c3_9120544c968c`)**

* `training.to6_c3_9120544c968c.return_features` (12 rows, all accepted returns 2022-2024)
* `training.to6_c3_9120544c968c.features_train` (tax_year 2022, 4 rows: R01-R04)
* `training.to6_c3_9120544c968c.features_validation` (tax_year 2023, 4 rows: R05-R08)
* `training.to6_c3_9120544c968c.features_test` (tax_year 2024, 4 rows: R09-R12)

Each output table has exactly the columns `return_id`, `tax_year`, `deduction_to_income`.
`taxpayer_id`, `income` and `deduction` are excluded under the need-to-know rule.

**Row meaning:** one row is one tax return, identified uniquely by `return_id`. `taxpayer_id`
identifies the filer, who may have returns in several years (e.g. F0 files R01, R05, R09).

**Ratio definition:** `deduction_to_income = deduction / income`, computed with
`try_divide(deduction, income)` so that a zero-income return is kept with a NULL ratio rather
than being dropped or raising a divide-by-zero error.

## Checks and decisions

**Source references.** Catalog descriptions above, the `source_lineage` view, and full-table
sample queries in the notebook (section 1/2 cells comparing all rows of `parsed_source`,
`returns_initial` and `returns_corrected`, and per-year counts/averages).

**Full-batch profile**

| Table | Rows | 2022 | 2023 | 2024 | Missing required values | Duplicate return_id | Zero-income rows |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `returns_initial` | 13 | 5 | 4 | 4 | 1 (R02 income NULL) | 1 (R01 appears twice) | 2 (R03, R09) |
| `returns_corrected` | 12 | 4 | 4 | 4 | 0 | 0 | 2 (R03, R09) |

**Initial batch rejection.** Rules: (1) completeness - `return_id`, `tax_year`, `income`,
`deduction` must be non-null; (2) uniqueness - each `return_id` occurs exactly once.
`returns_initial` fails both rules and was rejected before any feature output was written:

| return_id | taxpayer_id | tax_year | income | deduction | Rule failed |
| --- | --- | --- | --- | --- | --- |
| R01 | F0 | 2022 | 100 | 20 | Uniqueness: duplicate return_id (2 identical rows) |
| R01 | F0 | 2022 | 100 | 20 | Uniqueness: duplicate return_id (2 identical rows) |
| R02 | F1 | 2022 | NULL | 33 | Completeness: income missing (upstream `parsed_source` has 200) |

Next action: the unknown income was not replaced with zero and the duplicate was not silently
dropped; the batch was rejected and the replacement delivery `returns_corrected` was assessed
instead.

Rejection reference: notebook cell "Quality gate and rejected-records export" (section 2), first
run 2026-11-23T13:47:29Z UTC and re-run in both successful runs; the exported file reflects the
run 2 execution (2026-11-23T13:52:22Z) against `returns_initial` Delta version 2 (13 rows,
12 distinct IDs, 3 affected rows). Affected records and reasons are exported to `outputs/initial_rejection.json`.
The same cell applies the rules to `returns_corrected` and raises an error, stopping the run, if
it fails; it is placed before the cells that write any feature table.

**Replacement checks.** The same two rules applied to `returns_corrected` (Delta version 2):
0 null required values, 0 duplicate `return_id`s, 12 rows (4 per year), 0 affected rows. A row-by-row comparison with `parsed_source`
(join on `return_id`, any differing column) returned 0 rows, so the corrected delivery matches its
upstream source. Amounts are all nonnegative. `returns_corrected` was accepted for the final run.

**Independent expected value (nonzero).** From `parsed_source`, R01 has income 100 and deduction
20, so the expected ratio is 20 / 100 = 0.2. A second check: R10 has 137 / 1000 = 0.137.

**Saved-table readback.** Reading back `return_features` returned R01 = 0.2 and R10 = 0.137,
matching the expected values. Zero-income returns R03 (2022) and R09 (2024) are present with
`deduction_to_income` = NULL. The table holds 12 rows with 12 distinct `return_id`s.

## Two successful runs

Both runs are complete interactive runs of every code cell in order (setup, inspection, quality
gate, feature table, readback, year tables, exports, snapshot), in the notebook
https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557622
(`to6-databricks-challenge-starter/notebook`). No Jobs were used.

| | Run 1 | Run 2 |
| --- | --- | --- |
| Quality gate executed (UTC) | 2026-11-23 13:50:12 | 2026-11-23 13:52:22 |
| `return_features` written (Delta version) | 13:50:25 (v1) | 13:52:33 (v2) |
| `features_train` written | 13:50:38 (v1) | 13:52:44 (v2) |
| `features_validation` written | 13:50:40 (v1) | 13:52:47 (v2) |
| `features_test` written | 13:50:43 (v1) | 13:52:48 (v2) |
| Snapshot (IDs and values of all four tables) | `outputs/run_snapshots/run_20261123T135124Z.json` | `outputs/run_snapshots/run_20261123T135327Z.json` |

**Comparison evidence:** `outputs/run_comparison.json`, produced by the notebook cell "Compare the
two most recent runs". For all four tables it reports identical return IDs, identical IDs and
values, equal row counts (12 / 4 / 4 / 4), and a newer Delta version in run 2. As an independent
cross-check, Delta time travel (version 1 vs version 2, `exceptAll` in both directions) returned 0
differing rows for every table.

**How the contract was checked in each run** (cell "Run snapshot and handoff checks", which
asserts and stops the run on any failure):

* Year assignment: every row of `features_train` has tax_year 2022, `features_validation` 2023,
  `features_test` 2024, and each year table equals the `return_features` rows for that year
  (2022 -> R01-R04, 2023 -> R05-R08, 2024 -> R09-R12).
* One copy of every accepted return: the sorted IDs of `return_features`, and the combined IDs of
  the three year tables, each equal the 12 IDs of `returns_corrected` exactly.
* No appended copies: tables are written with `CREATE OR REPLACE TABLE`; after run 2 each table
  still has the same row count with all IDs distinct, while its Delta version advanced.
* Columns are exactly `return_id`, `tax_year`, `deduction_to_income`; every ratio equals
  deduction / income from the source, and R03 / R09 (zero income) are NULL.

An earlier preliminary run on 2026-11-20 (Delta version 0, before the quality-gate cell existed)
is superseded by the two runs above.

## Final files and limitations

**Final exports under `outputs/`**

* `outputs/return_features.json` - saved feature table (run 2), with explicit NULL ratios
* `outputs/features_train.json` - `features_train` export (run 2)
* `outputs/features_validation.json` - `features_validation` export (run 2)
* `outputs/features_test.json` - `features_test` export (run 2)
* `outputs/initial_rejection.json` - rejected `returns_initial` records with rule failures
* `outputs/run_snapshots/run_20261123T135124Z.json` - run 1 IDs and values, Delta versions, checks
* `outputs/run_snapshots/run_20261123T135327Z.json` - run 2 IDs and values, Delta versions, checks
* `outputs/run_comparison.json` - run 1 vs run 2 comparison

The three `features_*.json` files were re-exported from the run 2 tables after a formatting fix
(explicit `null` for R03 and R09); they equal the run 2 snapshot exactly, and no table was
rewritten.

**Practice file (ungraded, not part of the submission):** `outputs/connection-practice.json`.

**Not yet done / not verified**

* Both runs used the same interactive serverless session; a run from a fresh session or a Job
  was not performed.
* The quality gate stops a run by raising an error; it protects the feature tables only when the
  cells are run in notebook order.
* This data contract does not establish model labels or rule out leakage; later modelling work
  must assess what information is available at prediction time.
