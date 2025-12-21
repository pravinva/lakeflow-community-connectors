## OSIPI sample output script

This README describes how to use `print_osipi_samples.py` to show what the OSIPI connector can retrieve.

### What it does

- Instantiates `LakeflowConnect` directly (no Spark required)
- Iterates tables and prints:
  - `table_options` used
  - `next_offset`
  - up to **N sample records** (default 5)
- With `--verbose`, prints step-by-step telemetry (probe, per-table timing)

### Example command

```bash
python3 sources/osipi/test/print_osipi_samples.py \
  --pi-base-url https://<pi-web-api-host> \
  --probe \
  --verbose \
  --access-token <BEARER_TOKEN> \
  --max-records 5
```

### Parameters

- `--pi-base-url`: base URL of PI Web API
- `--access-token`: provide a Bearer token directly
- `--max-records`: records per table to print (default 5)
- `--tables`: comma-separated subset of tables to print (default: all tables)
- `--probe`: runs a lightweight `GET /piwebapi/dataservers` check before reading tables
- `--verbose`: prints telemetry logs

