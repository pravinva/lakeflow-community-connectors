# Databricks notebook source
import base64
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Connector source name
source_name = "osipi"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
# OSIPI Ingestion - Core tables to osipi.bronze
# NOTE: Using inline options instead of UC Connection due to platform limitation
# (HTTP connection type not supported for options injection)

# Resolve secrets at runtime using dbutils
# Note: Secret is base64 encoded, decode it for bearer token
bearer_token_encoded = dbutils.secrets.get(scope="sp-osipi", key="mock-bearer-token")
bearer_token = base64.b64decode(bearer_token_encoded).decode('utf-8').strip()

pipeline_spec = {
    "inline_options": {
        "sourceName": "osipi",
        "pi_base_url": "https://mock-piwebapi-912141448724.us-central1.run.app",
        "bearer_value": bearer_token
    },
    "objects": [
        # Data Servers table
        {
            "table": {
                "source_table": "pi_dataservers",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
            }
        },
        # Points (tags) table
        {
            "table": {
                "source_table": "pi_points",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
            }
        },
        # Time series data table
        {
            "table": {
                "source_table": "pi_timeseries",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
