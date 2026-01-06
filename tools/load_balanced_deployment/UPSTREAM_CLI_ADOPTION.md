# Upstream CLI Best Practices - Adoption Summary

## Overview

This document tracks the adoption of best practices from the upstream `community-connector` CLI tool into the load-balanced deployment toolkit.

## Source

**Upstream CLI Location:** `tools/community_connector/`
- **Purpose:** Single-pipeline deployment for community connectors
- **Architecture:** Click-based CLI with configuration management, validation, and error handling

## Adopted Best Practices

### 1. Configuration Merging with `deep_merge`

**Source:** `tools/community_connector/src/databricks/labs/community_connector/config.py:169-189`

**What it does:**
- Recursively merges two dictionaries
- Nested dictionaries are merged deeply (not replaced)
- Override values take precedence

**Our implementation:** `tools/load_balanced_deployment/utils.py::deep_merge()`

**Use cases in load-balanced toolkit:**
- Merge CSV preset config with CLI overrides
- Merge default pipeline settings with user customizations
- Combine multiple configuration sources with clear precedence

**Example:**
```python
from tools.load_balanced_deployment.utils import deep_merge

# Default config
defaults = {
    "pipeline": {"catalog": "main", "target": "bronze"},
    "schedule": {"snapshot": "0 0 * * *"}
}

# User overrides
overrides = {
    "pipeline": {"target": "silver"},  # Override target, keep catalog
    "schedule": {"append": "*/15 * * * *"}  # Add append schedule
}

# Result: {"pipeline": {"catalog": "main", "target": "silver"}, "schedule": {...}}
config = deep_merge(defaults, overrides)
```

**Benefits:**
- Users don't have to specify full config when customizing
- Predictable configuration precedence
- No accidental deletion of nested values

---

### 2. Placeholder Resolution with `replace_placeholder_in_value`

**Source:** `tools/community_connector/src/databricks/labs/community_connector/cli.py:178-198`

**What it does:**
- Recursively replaces placeholders in strings, lists, and dictionaries
- Handles nested data structures
- Preserves non-string values unchanged

**Our implementation:** `tools/load_balanced_deployment/utils.py::replace_placeholder_in_value()`

**Use cases in load-balanced toolkit:**
- Resolve `{CURRENT_USER}` in workspace paths
- Resolve `{PIPELINE_NAME}` in ingest file paths
- Template-based configuration generation

**Example:**
```python
from tools.load_balanced_deployment.utils import replace_placeholder_in_value

# Template config with placeholders
config = {
    "workspace_path": "/Users/{USER}/pipelines",
    "ingest_files": ["{USER}/ingest_1.py", "{USER}/ingest_2.py"],
    "nested": {"owner": "{USER}"}
}

# Resolve placeholders
resolved = replace_placeholder_in_value(config, "{USER}", "john.doe@company.com")
# Result: all {USER} replaced with "john.doe@company.com"
```

**Benefits:**
- Dynamic path generation without hardcoding
- Template-based workflows
- User-agnostic configuration files

---

### 3. CSV Validation with Clear Error Messages

**Source:** `tools/community_connector/src/databricks/labs/community_connector/pipeline_spec_validator.py:38-212`

**What it does:**
- Validates CSV structure before processing
- Reports errors with row numbers and column names
- Distinguishes warnings (non-fatal) from errors (fatal)
- Clear, actionable error messages

**Our implementation:** `tools/load_balanced_deployment/utils.py::`
- `CSVValidationError` - Custom exception with row/column context
- `validate_csv_structure()` - Comprehensive validation function
- `validate_and_report_csv()` - Convenience function for simple error handling

**Use cases in load-balanced toolkit:**
- Validate preset CSV files before deployment
- Catch configuration errors early (before workspace API calls)
- Provide clear feedback to users about what's wrong

**Example:**
```python
from tools/load_balanced_deployment.utils import validate_and_report_csv

# Validate before processing
error = validate_and_report_csv(
    "tables.csv",
    required_columns=["pipeline_name", "table_name", "scd_type"]
)

if error:
    print(f"CSV validation failed: {error}")
    sys.exit(1)

# If we get here, CSV is valid
process_csv("tables.csv")
```

**Error message examples:**
```
CSV Validation Error (Row 15, Column 'scd_type'): Invalid scd_type 'snapshot'. Must be one of: SCD_TYPE_1, SCD_TYPE_2, APPEND_ONLY

CSV Validation Error: Missing required columns: primary_keys, destination_table

CSV Validation Error (Row 42, Column 'pipeline_name'): Required field 'pipeline_name' is empty
```

**Benefits:**
- Fail fast with clear error messages
- Users know exactly what to fix and where
- Reduced debugging time
- Professional error handling

---

## Integration Plan

### Phase 1: Core Utilities (✅ Complete)
- [x] Create `utils.py` with adopted functions
- [x] Add inline documentation and examples
- [x] Include self-test code in utils.py

### Phase 2: Update Existing Scripts (Recommended)
- [ ] Update `discover_and_classify_tables.py` to use `validate_csv_structure()`
- [ ] Update `generate_ingest_files.py` to use `replace_placeholder_in_value()`
- [ ] Update `generate_dab_yaml.py` to use `deep_merge()` for config

### Phase 3: New Features (Future)
- [ ] Add `--config-file` support to discovery script (using `deep_merge`)
- [ ] Add template-based ingest file generation (using `replace_placeholder_in_value`)
- [ ] Add comprehensive validation before DAB generation

## Usage Examples

### Example 1: Validate CSV Before Processing

```python
#!/usr/bin/env python3
from tools.load_balanced_deployment.utils import validate_and_report_csv
import sys

csv_file = "examples/github/preset_by_category_and_ingestion.csv"
required = ["pipeline_name", "table_name", "scd_type"]

error = validate_and_report_csv(csv_file, required)
if error:
    print(f"Error: {error}")
    sys.exit(1)

print("CSV validation passed!")
# Proceed with processing...
```

### Example 2: Merge User Config with Defaults

```python
from tools.load_balanced_deployment.utils import deep_merge
import yaml

# Load defaults
with open("default_pipeline_config.yaml") as f:
    defaults = yaml.safe_load(f)

# Load user overrides
with open("user_config.yaml") as f:
    user_config = yaml.safe_load(f)

# Merge with precedence: user > defaults
config = deep_merge(defaults, user_config)

# Use merged config for deployment
deploy_pipelines(config)
```

### Example 3: Resolve Placeholders in Templates

```python
from tools.load_balanced_deployment.utils import replace_placeholder_in_value
from databricks.sdk import WorkspaceClient

# Get current user
w = WorkspaceClient()
current_user = w.current_user.me().user_name

# Template config
template = {
    "workspace_path": "/Users/{USER}/.lakeflow_connectors/{CONNECTOR}",
    "pipelines": [
        {"name": "{CONNECTOR} - Pipeline 1", "owner": "{USER}"},
        {"name": "{CONNECTOR} - Pipeline 2", "owner": "{USER}"}
    ]
}

# Resolve USER placeholder
config = replace_placeholder_in_value(template, "{USER}", current_user)

# Resolve CONNECTOR placeholder
config = replace_placeholder_in_value(config, "{CONNECTOR}", "github")

# Deploy with resolved paths
deploy(config)
```

## Comparison: Before and After

### Before (without utils)
```python
# Hard-coded paths
workspace_path = f"/Users/pravin.varma@databricks.com/pipelines"

# Manual validation
with open(csv_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        if not row.get("pipeline_name"):
            raise Exception("Missing pipeline_name")  # Generic error

# No config merging - last value wins
config = {**defaults, **user_config}  # Shallow merge, nested dicts replaced
```

### After (with utils)
```python
from tools.load_balanced_deployment.utils import (
    replace_placeholder_in_value,
    validate_and_report_csv,
    deep_merge
)

# Dynamic path resolution
template = {"workspace_path": "/Users/{USER}/pipelines"}
config = replace_placeholder_in_value(template, "{USER}", current_user)

# Clear validation with context
error = validate_and_report_csv(csv_file, ["pipeline_name", "table_name"])
if error:
    print(error)  # "CSV Validation Error (Row 15, Column 'pipeline_name'): Required field is empty"
    sys.exit(1)

# Deep merge with precedence
config = deep_merge(defaults, user_config)  # Nested dicts merged intelligently
```

## Testing

The `utils.py` module includes self-tests. Run them:

```bash
python3 tools/load_balanced_deployment/utils.py
```

Expected output:
```
Testing deep_merge:
  Result: {...}
  ✓ Passed

Testing replace_placeholder_in_value:
  Result: {...}
  ✓ Passed

Testing CSVValidationError:
  Error message: CSV Validation Error (Row 5, Column 'table_name'): Test error
  ✓ Passed

All tests passed!
```

## Future Enhancements

### 1. Add More Validation Rules
Extend `validate_csv_structure()` with domain-specific checks:
- Validate cron expressions in `schedule` column
- Check if `primary_keys` are comma-separated valid identifiers
- Validate `table_configuration` JSON syntax
- Check for duplicate table names within same pipeline

### 2. Config File Support
Add YAML config file support to all scripts:
```bash
python discover_and_classify_tables.py \
  --config default_github_config.yaml \
  --override-catalog production  # CLI override takes precedence
```

### 3. Template Library
Create reusable templates for common scenarios:
```
templates/
├── github_standard.yaml          # Standard GitHub deployment
├── hubspot_standard.yaml         # Standard HubSpot deployment
├── high_frequency_ingest.yaml    # High-frequency append workloads
└── snapshot_only.yaml            # Snapshot-only workloads
```

## Related Documentation

- [Category Prefix Map Guide](CATEGORY_PREFIX_MAP_GUIDE.md) - How to use prefix maps for automatic discovery
- [GitHub Example README](examples/github/README.md) - GitHub connector preset examples
- [HubSpot Example README](examples/hubspot/README.md) - HubSpot connector preset examples
- [Upstream CLI Source](../community_connector/) - Original implementation

## Credits

Best practices adopted from:
- `tools/community_connector/src/databricks/labs/community_connector/config.py` - Configuration management
- `tools/community_connector/src/databricks/labs/community_connector/cli.py` - Placeholder resolution
- `tools/community_connector/src/databricks/labs/community_connector/pipeline_spec_validator.py` - Validation patterns
