## OSIPI example: multi-pipeline grouping by category + ingestion_type

In general, `discover_and_classify_tables.py` is connector-agnostic:
- It uses `TABLES_*` lists when a connector provides them (best option)
- Otherwise it falls back to a generic prefix-based category

OSI PI has stable table name prefixes, so you *can* optionally pass a prefix->category mapping.
This example file shows what that mapping could look like.

### Example

```bash
python3 tools/ingestion_dab_generator/discover_and_classify_tables.py \
  --connector-name osipi \
  --output-csv /tmp/osipi_by_category.csv \
  --connection-name osipi_connection \
  --dest-catalog main \
  --dest-schema bronze \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat tools/ingestion_dab_generator/examples/osipi/category_prefix_map.example.json)" \
  --schedule-snapshot "0 0 * * *" \
  --schedule-append "*/15 * * * *"

python3 tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /tmp/osipi_by_category.csv \
  --output-yaml /tmp/osipi_pipelines.yml \
  --connector-name osipi \
  --connection-name osipi_connection \
  --dest-catalog main \
  --dest-schema bronze \
  --emit-jobs \
  --max-items-per-pipeline 12
```
