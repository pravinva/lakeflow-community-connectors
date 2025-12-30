## OSIPI example CSVs for multi-pipeline ingestion

### Is `discover_and_classify_tables.py` required?

No. It’s just a convenience to *generate* a CSV automatically from connector metadata.

`generate_ingestion_dab_yaml.py` only needs a CSV with the required columns.

### Prerequisites for `generate_ingestion_dab_yaml.py`

- Input **CSV** must include at least:
  - `source_table`
  - `connection_name` (either as a column or passed via `--connection-name`)
- Recommended columns (used by the generator):
  - `destination_catalog`, `destination_schema`, `destination_table`
  - `pipeline_group` (otherwise you must pass `--num-pipelines`)
  - `schedule` (optional; enables `--emit-jobs`)
  - `table_options_json` (optional; merged into `table_configuration`)
  - `weight` (optional; used by auto-balance)

### Provided examples

- `osipi_single_pipeline_minimal.csv`
  - Everything goes into one pipeline (`pipeline_group=all`).

- `osipi_by_category_and_ingestion_type.csv`
  - Explicit grouping (e.g. `time_series_append`, `asset_framework_snapshot`, etc.).

- `osipi_auto_balance_no_groups.csv`
  - Leaves `pipeline_group` empty; use `--num-pipelines` to auto-balance.

### Commands

Single-pipeline example:

```bash
python3 ../../generate_ingestion_dab_yaml.py \
  --input-csv osipi_single_pipeline_minimal.csv \
  --output-yaml /tmp/osipi_single.yml \
  --connector-name osipi \
  --connection-name osipi_connection \
  --dest-catalog main \
  --dest-schema bronze
```

Category-based example (multiple pipelines + jobs):

```bash
python3 ../../generate_ingestion_dab_yaml.py \
  --input-csv osipi_by_category_and_ingestion_type.csv \
  --output-yaml /tmp/osipi_multi.yml \
  --connector-name osipi \
  --connection-name osipi_connection \
  --dest-catalog main \
  --dest-schema bronze \
  --emit-jobs \
  --max-items-per-pipeline 10
```

Auto-balance example (multiple pipelines + jobs):

```bash
python3 ../../generate_ingestion_dab_yaml.py \
  --input-csv osipi_auto_balance_no_groups.csv \
  --output-yaml /tmp/osipi_balanced.yml \
  --connector-name osipi \
  --connection-name osipi_connection \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 3 \
  --emit-jobs
```
