# Generic Lakeflow Connect Deployment for HTTP-Based Connectors

## Problem Statement

The Databricks Lakeflow `ingestion_definition` framework **does not automatically inject** credentials from Unity Catalog connection properties into custom connectors.

**Affected connectors:**
- OSI PI (OAuth with Service Principal)
- HubSpot (Private App access token)
- Zendesk (API token)
- Zoho CRM (OAuth refresh token)
- Salesforce (username/password/token)
- GitHub (Personal Access Token)
- Any custom HTTP-based connector requiring authentication

## Solution

Use **custom DLT notebooks** that:
1. Read secrets in driver context (where `dbutils` works)
2. Pass credentials as options to the connector
3. Deploy via Databricks Asset Bundles (DAB)

---

## Architecture

```
Metadata CSV
  ↓
Notebook Generator (generic, connector-agnostic)
  ↓
DLT Notebooks (one per pipeline group)
  ├─ Reads secrets from dbutils (driver)
  ├─ Passes to connector as options
  └─ Defines @dlt.table for each table
  ↓
DAB YAML Generator
  ↓
Pipeline Definitions (reference notebooks)
  ↓
Deploy via DAB
  ↓
Production DLT Pipelines with OAuth/Auth
```

---

## Quick Start

## Run entirely from within a Databricks workspace (no local CLI)

If your repo is synced into Databricks Repos and you do **not** want to run any commands on your laptop, open and run:

- **Recommended (preset-driven, connector-agnostic)**: `tools/notebook_based_deployment/workspace_notebooks/deploy_dlt_pipelines_from_preset.py`
- Legacy (single-notebook, manual widgets): `tools/notebook_based_deployment/workspace_notebooks/deploy_notebook_based_dlt_pipelines.py`

The preset-driven notebook:
- Discovers connector “resources” at `sources/<connector>/deployment/deploy_preset.json`
- Builds the ingestion CSV using one of:
  - **static**: a checked-in CSV path
  - **discover**: auto-generate CSV from connector metadata
  - **manual**: a comma-separated list of tables (no CSV needed)
- Stores the connector source in a **Unity Catalog Volume** under `/Volumes/...` (no DBFS)
- Generates + uploads one DLT notebook per `pipeline_group`
- Creates/updates DLT pipelines (and optional scheduled jobs) via the Databricks SDK

### Connector “resource” preset format

To onboard a new connector into this workflow, add:

- `sources/<connector>/deployment/deploy_preset.json`

At minimum, it should include:
- `connector_name`
- `generated_source_rel_path`
- `csv.mode` (one of `static|discover|manual`)
- `connector_config` (secrets scope + secret mappings + any static options)
- `storage.uc_volume_name` (volume name to store the generated connector source)

1) Prepare a metadata CSV with at least `source_table` and `pipeline_group`.

2) Generate notebooks:

```bash
python tools/notebook_based_deployment/generate_dlt_notebooks_generic.py \
  --connector-name <connector> \
  --input-csv <metadata.csv> \
  --output-dir dlt_notebooks/<connector> \
  --connector-path /Workspace/Users/<you>@company.com/connectors \
  --generate-all-groups
```

3) Upload notebooks and deploy using the generated DAB YAML (see per-connector examples).
