# Load-Balanced Deployment Notebooks

This directory contains Databricks notebooks for deploying load-balanced multi-pipeline configurations.

## Available Notebooks

### `00_load_balanced_deployment_all_in_one.py`

**Comprehensive all-in-one workflow** for deploying load-balanced pipelines.

**What it does**:
1. (Optional) Auto-discover tables and classify into groups
2. Generate Python ingest files for each group
3. Generate DAB YAML configuration
4. Upload ingest files to workspace
5. Deploy pipelines using Databricks Asset Bundle
6. Verify deployment

**Use when**:
- You want a complete automated workflow
- First time setting up load-balanced deployment
- You prefer notebook-based deployment over CLI

**Configuration**:
Edit the configuration cell to set:
- Connector name and UC Connection
- Destination catalog/schema
- Grouping strategy
- Schedules per ingestion type
- Cluster sizing

## Usage

### Option 1: Upload to Databricks Workspace

```bash
databricks workspace import \
  notebooks/00_load_balanced_deployment_all_in_one.py \
  /Workspace/Users/your.email@company.com/load_balanced_deployment \
  --language PYTHON
```

Then run the notebook in your Databricks workspace.

### Option 2: Run Locally (requires Databricks Connect)

```bash
# From tools/load_balanced_deployment directory
python3 notebooks/00_load_balanced_deployment_all_in_one.py
```

## Workflow Comparison: Notebook vs CLI

### Notebook Approach (this directory)

**Pros**:
- Interactive step-by-step execution
- Visual feedback and previews
- Easy to customize per-step
- Great for learning and experimentation

**Cons**:
- Requires Databricks workspace
- May need Databricks Connect for local execution

**Use when**: First time setup, learning, or interactive experimentation

### CLI Approach (Python scripts)

**Pros**:
- Automation-friendly (CI/CD pipelines)
- No workspace required (can run locally)
- Easy to script and version control
- Faster for repeated deployments

**Cons**:
- Less interactive feedback
- Requires understanding of all parameters upfront

**Use when**: Production deployments, CI/CD automation, repeated use

## Step-by-Step vs All-in-One

The current notebook (`00_load_balanced_deployment_all_in_one.py`) provides a complete workflow in a single file.

You can also split this into separate notebooks:
- `01_discover_tables.py` - Auto-discover tables (optional)
- `02_generate_deployment.py` - Generate ingest files and DAB YAML
- `03_deploy_pipelines.py` - Upload and deploy

Use the all-in-one approach for simplicity, or split into separate notebooks for more granular control.

## Prerequisites

Before running these notebooks, ensure:

1. **UC Connection created**:
   ```bash
   community-connector create_connection <source> <conn_name> -o '{...}'
   ```

2. **Connector source code deployed** to workspace

3. **Databricks CLI configured** (for DAB deployment):
   ```bash
   databricks configure
   ```

4. **Python dependencies installed** (if running locally):
   ```bash
   pip install databricks-sdk pyyaml
   ```

## Configuration Tips

### Using Preset CSVs

Set `USE_PRESET = True` and point to a preset CSV:

```python
USE_PRESET = True
PRESET_CSV_PATH = "../examples/osipi/preset_by_category_and_ingestion.csv"
```

This skips the auto-discovery step and uses a pre-defined table configuration.

### Grouping Strategies

Choose the right grouping strategy for your use case:

```python
GROUP_BY = "category_and_ingestion_type"  # Recommended
# Other options: ingestion_type, category, none
```

### Schedules

Set custom schedules per ingestion type:

```python
SCHEDULE_SNAPSHOT = "0 0 * * *"      # Daily
SCHEDULE_APPEND = "*/15 * * * *"     # Every 15 min
SCHEDULE_CDC = "*/5 * * * *"         # Every 5 min
SCHEDULE_UNKNOWN = ""                # No schedule
```

### Cluster Sizing

Adjust cluster size based on your data volume:

```python
CLUSTER_NUM_WORKERS = 2  # Increase for larger datasets
```

### Job Pausing

Create jobs in PAUSED state for manual review:

```python
PAUSE_JOBS = True  # Set to False to start jobs immediately
```

## Troubleshooting

### Error: "databricks command not found"

Install and configure the Databricks CLI:
```bash
pip install databricks-cli
databricks configure
```

### Error: "Connection not found"

Ensure UC Connection exists:
```bash
databricks connections list | grep <connector_name>
```

Create if missing:
```bash
community-connector create_connection <source> <conn_name> -o '{...}'
```

### Error: "Module not found"

Install required Python packages:
```bash
pip install databricks-sdk pyyaml
```

### Notebook hangs during deployment

Check that:
- Databricks CLI is authenticated
- You have permissions to create pipelines
- Bundle YAML is valid (check Step 3 output)

## Next Steps After Deployment

1. **Monitor pipelines**:
   - Navigate to **Workflows > Delta Live Tables**
   - Check each pipeline group independently

2. **Enable scheduled jobs** (if created in PAUSED state):
   ```python
   from databricks.sdk import WorkspaceClient
   from databricks.sdk.service.jobs import PauseStatus

   w = WorkspaceClient()
   w.jobs.update(
       job_id=<job_id>,
       new_settings={
           "schedule": {
               "quartz_cron_expression": "<cron>",
               "timezone_id": "UTC",
               "pause_status": PauseStatus.UNPAUSED
           }
       }
   )
   ```

3. **Trigger manual runs**:
   ```bash
   databricks bundle run <pipeline_key>
   ```

4. **Customize further**:
   - Edit generated ingest files in workspace
   - Modify DAB YAML for different cluster configs
   - Adjust schedules per pipeline

## Contributing

When adding new connectors, consider creating preset CSVs and updating this notebook to support them out-of-the-box.

See `../examples/osipi/README.md` for guidelines on creating preset CSVs.
