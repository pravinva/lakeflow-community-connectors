# OSI PI Lakeflow Community Connector

Industrial data connector for OSI PI historians via PI Web API.

## Features

- **39 tables** covering PI Data Archive, Asset Framework, and Event Frames
- **Multiple authentication methods**: OIDC, Bearer token, Basic auth
- **UC Connections integration**: Secure credential management
- **Incremental loading**: Automatic time-based cursors for time-series data
- **Production-ready**: Error handling, retry logic, token refresh

## Quick Start

### 1. Create UC Connection

#### Current Method (With UC Bug Workaround)

Due to a known UC bug (being fixed Dec 2024), use workaround property names:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()

w.connections.create(
    name="osipi_connection",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://your-pi-webapi-host",
        "workspace_host": "https://your-workspace.cloud.databricks.com",
        "client_id": "{{secrets/your-scope/client-id}}",
        "client_value_tmp": "{{secrets/your-scope/client-secret}}",  # ⚠️ Workaround name
        "verify_ssl": "true"
    },
    comment="OSI PI connection for industrial data"
)
```

#### Future Method (After UC Fix)

Once Databricks merges the UC fix, use normal property names:

```python
w.connections.create(
    name="osipi_connection",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://your-pi-webapi-host",
        "workspace_host": "https://your-workspace.cloud.databricks.com",
        "client_id": "{{secrets/your-scope/client-id}}",
        "client_secret": "{{secrets/your-scope/client-secret}}",  # ✅ Normal name
        "verify_ssl": "true"
    }
)
```

**Note**: The connector automatically supports both naming patterns. No code changes needed after UC fix.

### 2. Register Connector

```python
# In Databricks notebook
exec(open("/path/to/_generated_osipi_python_source.py").read())
register_lakeflow_source(spark)
```

### 3. Read Data

```python
# Read PI data servers
df = (
    spark.read.format("lakeflow_connect")
    .option("databricks.connection", "osipi_connection")
    .option("tableName", "pi_dataservers")
    .load()
)

# Read time-series data
df = (
    spark.read.format("lakeflow_connect")
    .option("databricks.connection", "osipi_connection")
    .option("tableName", "pi_timeseries")
    .option("tag_webids", "I9zHPD3w-2pki0OYUCdU-RnQQgAQABAC1...")
    .option("startTime", "*-24h")
    .option("endTime", "*")
    .load()
)
```

## Authentication Methods

### 1. OIDC Client Credentials (Recommended)

**Required properties:**
- `client_id` (or use workaround name)
- `client_secret` → use `client_value_tmp` (workaround for current UC bug)
- `workspace_host`

**How it works:**
1. Connector exchanges client credentials for access token
2. Token cached and auto-refreshed
3. Supports long-running pipelines

### 2. Bearer Token

**Required properties:**
- `access_token`

**Use when:**
- You manage token acquisition separately
- Short-lived queries only

### 3. Basic Authentication

**Required properties:**
- `username`
- `password`

**Use only if:**
- PI Web API has basic auth enabled
- No OIDC available

## Known Issues

### UC Connection Bug (Dec 2024)

**Issue**: UC Connections strips properties containing `secret` or `token` from connection object retrieval.

**Status**: Confirmed by Databricks (Sheldon Tauro). Fix identified and will be merged soon.

**Impact**: Requires using workaround property names temporarily.

**Workaround**: Use alternative property names:
- `client_secret` → `client_value_tmp`
- `refresh_token` → `refresh_value_tmp`

**Resolution**: After UC fix is deployed, normal property names will work automatically. No connector changes needed.

## Available Tables

### Discovery & Inventory (4 tables)
- `pi_dataservers` - PI Data Archive servers
- `pi_points` - PI tags/points
- `pi_point_attributes` - Point metadata
- `pi_point_type_catalog` - Point type statistics

### Time Series (14 tables)
- `pi_timeseries` - Recorded values
- `pi_streamset_recorded` - Multi-tag recorded values
- `pi_interpolated` - Interpolated values
- `pi_current_value` - Latest values
- `pi_summary` - Statistical summaries
- And 9 more...

### Asset Framework (13 tables)
- `pi_assetservers` - AF servers
- `pi_af_hierarchy` - Asset hierarchy
- `pi_element_attributes` - Asset attributes
- `pi_element_templates` - Templates
- And 9 more...

### Event Frames (7 tables)
- `pi_event_frames` - Events/batches
- `pi_eventframe_attributes` - Event metadata
- And 5 more...

See full documentation for table schemas and options.

## Examples

### Incremental Time-Series Pipeline

```python
# DLT Pipeline
import dlt

@dlt.table
def pi_temperature_data():
    return (
        spark.readStream
        .format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection")
        .option("tableName", "pi_timeseries")
        .option("tag_webids", "temp_sensor_webids")
        .load()
    )
```

### Asset Hierarchy Analysis

```python
# Join hierarchy with attributes
hierarchy = spark.table("osipi.bronze.pi_af_hierarchy")
attributes = spark.table("osipi.bronze.pi_element_attributes")

assets = (
    hierarchy
    .join(attributes, "element_webid")
    .select("name", "path", "template_name", "attribute_name", "value")
)
```

## Support

- **Issues**: Report via GitHub
- **Slack**: #lakeflow-community-connectors
- **Documentation**: See `/docs` folder

## License

[Your License]
