# Pipeline Configuration Notes

## Upstream CLI vs Load-Balanced Deployment

### Upstream CLI Defaults

From `tools/community_connector/default_config.yaml` (before deletion):
```yaml
pipeline:
  channel: PREVIEW          # PREVIEW channel (latest features)
  continuous: false
  development: true
  serverless: true          # Serverless compute (auto-scaling)
```

### Load-Balanced Deployment Current Settings

The load-balanced deployment toolkit currently does NOT explicitly set channel or serverless in the DAB YAML. When these are not specified, Databricks uses the following defaults:
- `channel`: defaults to `CURRENT` (stable releases)
- `serverless`: defaults to `false` (requires explicit cluster configuration)

The toolkit provides classic cluster configuration:
```yaml
clusters:
  - label: default
    num_workers: 1  # or configurable via --num-workers
```

## Testing Results

### Serverless Testing (January 2026)

**Result**: ❌ Serverless compute DOES NOT currently work with community connectors

**Test Configuration**:
- `channel: PREVIEW`
- `serverless: true`
- No clusters configuration

**Issues Encountered**:
- Pipelines fail to start or execute properly with serverless compute
- Classic clusters with `channel: CURRENT` work reliably

### Working Configuration

**Result**: ✅ Classic clusters work correctly

**Working Configuration**:
- `channel: CURRENT` (or default)
- `serverless: false` (or omit serverless field)
- Classic cluster with `node_type_id: i3.xlarge` or similar

## Recommendations

### For Now (January 2026)

**DO NOT** blindly adopt upstream CLI defaults. While the upstream CLI uses `serverless: true`, this configuration does not currently work with community connectors in practice.

**KEEP** using classic clusters:
```python
clusters = [
    PipelineCluster(
        label="default",
        node_type_id="i3.xlarge",
        autoscale=PipelineClusterAutoscale(
            min_workers=1,
            max_workers=5,
            mode=PipelineClusterAutoscaleMode.ENHANCED
        )
    )
]
```

### Future Investigation

When serverless support is fixed or improved:

1. **Test again** with:
   - `channel: PREVIEW`
   - `serverless: true`
   - No clusters configuration

2. **Benefits of serverless** (when it works):
   - Auto-scaling based on workload
   - No need to specify node_type_id
   - Cost efficiency (pay only for actual usage)
   - Latest DLT features in PREVIEW channel

3. **Monitor** Databricks release notes for:
   - Serverless compute improvements for DLT
   - Community connector compatibility updates
   - PySpark Data Source API enhancements

### Channel Choice

**CURRENT vs PREVIEW**:
- `CURRENT`: Stable releases, recommended for production
- `PREVIEW`: Latest features, may have bugs, good for development/testing

For production deployments, `CURRENT` channel is safer even if serverless becomes available.

## Implementation in Load-Balanced Toolkit

### Current Approach

The toolkit generates DAB YAML without explicit channel/serverless settings:

```yaml
# generate_dab_yaml.py output
resources:
  pipelines:
    my_pipeline:
      name: "Pipeline Name"
      catalog: "${var.catalog}"
      target: "${var.schema}"
      development: true
      continuous: false
      libraries: [...]
      clusters:  # Explicit cluster config
        - label: default
          num_workers: 1
```

This approach works because:
1. Defaults to `CURRENT` channel (stable)
2. Requires explicit cluster config (classic clusters)
3. Avoids serverless compatibility issues

### If Serverless Works in Future

Update `generate_dab_yaml.py` to add conditional serverless support:

```python
def generate_dab_yaml(..., use_serverless=False, channel="CURRENT"):
    pipeline_def = {
        "name": f"...",
        "catalog": "${var.catalog}",
        "target": "${var.schema}",
        "channel": channel,  # Add explicit channel
        "development": True,
        "continuous": False,
        "libraries": [...]
    }

    if use_serverless:
        pipeline_def["serverless"] = True
        # No clusters configuration needed
    else:
        pipeline_def["clusters"] = [{
            "label": "default",
            **cluster_config
        }]
```

## Related Issues

- Community connectors require PySpark Data Source API support
- Serverless compute may have limitations with custom Python data sources
- UC Connections injection may behave differently in serverless vs classic

## References

- Upstream CLI default config: `tools/community_connector/default_config.yaml` (commit a81d32e)
- Upstream CLI adoption notes: `tools/load_balanced_deployment/UPSTREAM_CLI_ADOPTION.md`
- Testing history: This document records testing as of January 2026
