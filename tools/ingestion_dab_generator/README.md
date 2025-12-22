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

## Quick Start (Any Connector)

### Step 1: Prepare Metadata CSV

**Format:**
```csv
source_table,destination_table,destination_catalog,destination_schema,pipeline_group,schedule,table_options_json,weight
contacts,contacts,marketing,bronze,main,0 2 * * *,"{""maxCount"":""10000""}",100
companies,companies,marketing,bronze,main,0 2 * * *,,50
```

**Required columns:**
- `source_table` - Connector table name
- `pipeline_group` - Logical grouping for pipelines

**Optional columns:**
- `destination_table` - Defaults to source_table
- `destination_catalog` - Defaults to --dest-catalog
- `destination_schema` - Defaults to --dest-schema
- `schedule` - Cron expression (5-field format)
- `table_options_json` - Table-specific options as JSON
- `weight` - For load balancing (numeric)

### Step 2: Generate Notebooks

```bash
python generate_dlt_notebooks_generic.py \
    --connector-name <connector> \
    --input-csv metadata.csv \
    --output-dir dlt_notebooks/<connector> \
    --connector-path /Workspace/Users/<you>@company.com/connectors \
    --generate-all-groups
```

**This creates one notebook per unique `pipeline_group`.**

### Step 3: Upload Notebooks

```bash
databricks workspace upload-dir \
    dlt_notebooks/<connector> \
    /Workspace/Users/<you>@company.com/<connector>_dlt_pipelines \
    --overwrite-existing
```

### Step 4: Generate DAB YAML

```bash
python generate_dab_yaml_notebooks.py \
    --connector-name <connector> \
    --input-csv metadata.csv \
    --output-yaml dab_template/resources/<connector>_pipelines.yml \
    --notebook-base-path /Workspace/Users/<you>@company.com/<connector>_dlt_pipelines \
    --dest-catalog <catalog> \
    --dest-schema <schema> \
    --emit-jobs
```

### Step 5: Deploy

```bash
cd dab_template
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

---

## Connector-Specific Examples

### OSI PI

```bash
# Generate notebooks
python generate_dlt_notebooks_generic.py \
    --connector-name osipi \
    --input-csv examples/osipi/osipi_tables_metadata.csv \
    --output-dir dlt_notebooks/osipi \
    --connector-path /Workspace/Users/pravin.varma@databricks.com/connectors \
    --generate-all-groups

# Upload
databricks workspace upload-dir \
    dlt_notebooks/osipi \
    /Workspace/Users/pravin.varma@databricks.com/osipi_dlt_pipelines

# Generate YAML
python generate_dab_yaml_notebooks.py \
    --connector-name osipi \
    --input-csv examples/osipi/osipi_tables_metadata.csv \
    --output-yaml dab_template/resources/osipi_pipelines.yml \
    --notebook-base-path /Workspace/Users/pravin.varma@databricks.com/osipi_dlt_pipelines \
    --dest-catalog osipi \
    --dest-schema bronze \
    --emit-jobs

# Deploy
cd dab_template && databricks bundle deploy -t dev
```

### HubSpot

```bash
# Generate notebooks
python generate_dlt_notebooks_generic.py \
    --connector-name hubspot \
    --input-csv examples/hubspot/tables.csv \
    --output-dir dlt_notebooks/hubspot \
    --connector-path /Workspace/Users/user@company.com/connectors \
    --generate-all-groups

# Upload
databricks workspace upload-dir \
    dlt_notebooks/hubspot \
    /Workspace/Users/user@company.com/hubspot_dlt_pipelines

# Generate YAML
python generate_dab_yaml_notebooks.py \
    --connector-name hubspot \
    --input-csv examples/hubspot/tables.csv \
    --output-yaml dab_template/resources/hubspot_pipelines.yml \
    --notebook-base-path /Workspace/Users/user@company.com/hubspot_dlt_pipelines \
    --dest-catalog marketing \
    --dest-schema bronze \
    --emit-jobs

# Deploy
cd dab_template && databricks bundle deploy -t dev
```

### Zendesk

```bash
python generate_dlt_notebooks_generic.py \
    --connector-name zendesk \
    --input-csv examples/zendesk/tables.csv \
    --output-dir dlt_notebooks/zendesk \
    --connector-path /Workspace/Users/user@company.com/connectors \
    --generate-all-groups

databricks workspace upload-dir \
    dlt_notebooks/zendesk \
    /Workspace/Users/user@company.com/zendesk_dlt_pipelines

python generate_dab_yaml_notebooks.py \
    --connector-name zendesk \
    --input-csv examples/zendesk/tables.csv \
    --output-yaml dab_template/resources/zendesk_pipelines.yml \
    --notebook-base-path /Workspace/Users/user@company.com/zendesk_dlt_pipelines \
    --dest-catalog support \
    --dest-schema bronze \
    --emit-jobs

cd dab_template && databricks bundle deploy -t dev
```

---

## Adding a New Connector

### Option 1: Use Built-in Config

If your connector matches a built-in pattern (see `connector_configs.json`):

```bash
# Just use --connector-name
python generate_dlt_notebooks_generic.py \
    --connector-name hubspot \
    ...
```

### Option 2: Provide Custom Config

Create a JSON config file:

```json
{
  "secrets_scope": "my-connector",
  "secret_mappings": {
    "api_key": ["api_key", "token"],
    "api_url": ["api_url", "base_url"]
  },
  "static_options": {
    "verify_ssl": "true",
    "timeout": "60"
  },
  "dynamic_options": {
    "workspace_id": "spark.conf.get('spark.databricks.workspaceId')"
  }
}
```

Then use it:

```bash
python generate_dlt_notebooks_generic.py \
    --connector-name mycustom \
    --connector-config configs/mycustom_config.json \
    --input-csv metadata.csv \
    ...
```

---

## Automated Deployment (One Command)

Use the master script:

```bash
./deploy_connector_pipelines.sh \
    --connector-name osipi \
    --input-csv examples/osipi/osipi_tables_metadata.csv \
    --workspace-path /Workspace/Users/pravin.varma@databricks.com \
    --dest-catalog osipi \
    --dest-schema bronze \
    --emit-jobs
```

**This runs all 5 steps automatically:**
1. ✓ Generates notebooks
2. ✓ Uploads to Databricks
3. ✓ Generates DAB YAML
4. ✓ Validates bundle
5. ✓ Deploys (with confirmation)

---

## Configuration Files

### Connector Config Structure

```json
{
  "secrets_scope": "string",           // Databricks secrets scope name
  "secret_mappings": {                 // Map variable names to secret keys
    "var_name": ["primary_key", "fallback_key"]
  },
  "static_options": {                  // Hardcoded connector options
    "option_name": "value"
  },
  "dynamic_options": {                 // Options computed at runtime
    "option_name": "python_expression"
  }
}
```

### Example Configs for Common Connectors

**OSI PI (OAuth):**
```json
{
  "secrets_scope": "sp-osipi",
  "secret_mappings": {
    "client_id": ["client_id", "sp-client-id"],
    "client_secret": ["client_secret", "sp-client-secret"]
  },
  "static_options": {
    "pi_base_url": "https://your-pi-server.com",
    "verify_ssl": "false"
  },
  "dynamic_options": {
    "workspace_host": "f\"https://{spark.conf.get('spark.databricks.workspaceUrl')}\""
  }
}
```

**HubSpot (Private App Token):**
```json
{
  "secrets_scope": "hubspot",
  "secret_mappings": {
    "access_token": ["access_token"]
  },
  "static_options": {},
  "dynamic_options": {}
}
```

---

## Benefits of This Approach

✅ **Connector-agnostic** - Works for any HTTP-based connector
✅ **Reusable** - Same scripts for OSI PI, HubSpot, Zendesk, etc.
✅ **Automated** - Generate from CSV metadata
✅ **DAB deployment** - Standard Databricks deployment workflow
✅ **Scheduled jobs** - Automatic scheduling support
✅ **OAuth support** - Handles token refresh for OAuth connectors
✅ **Maintainable** - Edit CSV and regenerate

---

## Repository Integration

Add these files to `lakeflow-community-connectors`:

```
tools/ingestion_dab_generator/
├── generate_dlt_notebooks_generic.py    # ← Generic notebook generator
├── generate_dab_yaml_notebooks.py       # ← Generic DAB YAML generator
├── deploy_connector_pipelines.sh        # ← Master automation script
├── connector_configs.json               # ← Built-in connector configs
├── examples/
│   ├── osipi/
│   │   ├── osipi_tables_metadata.csv
│   │   └── discover_osipi_partitions.py
│   ├── hubspot/
│   │   └── hubspot_tables.csv
│   ├── zendesk/
│   │   └── zendesk_tables.csv
│   └── zoho_crm/
│       └── zoho_tables.csv
└── dab_template/
    ├── databricks.yml
    └── resources/
        ├── osipi_pipelines.yml          # ← Generated
        ├── hubspot_pipelines.yml        # ← Generated
        └── zendesk_pipelines.yml        # ← Generated
```

---

## For Each Connector in the Repo

To enable notebook-based deployment for existing connectors:

1. **Create metadata CSV** (examples/<connector>/<connector>_tables.csv)
2. **Add connector config** to `connector_configs.json` (if not already there)
3. **Run deployment script**

That's it! The generic tools handle the rest.

---

## Summary

**You now have:**
- ✅ Generic notebook generator (works for any connector)
- ✅ Generic DAB YAML generator (notebook-based)
- ✅ Automated deployment script
- ✅ Configuration for common connectors
- ✅ Works for all connectors in lakeflow-community-connectors repo

**This solves the secret injection problem for ALL HTTP-based connectors!** 🎉
