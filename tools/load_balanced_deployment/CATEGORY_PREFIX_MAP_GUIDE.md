# Category Prefix Map Guide

## Overview

For connectors that don't have built-in `TABLES_*` class attributes (like GitHub and HubSpot), the automatic discovery script can use a **category prefix map** to classify tables into logical groups.

## What is a Category Prefix Map?

A JSON mapping from **table name prefixes** to **category names**, used to classify tables when automatic categorization isn't available.

**Example:**
```json
{
  "pull": "core",
  "issue": "core",
  "commit": "activity",
  "user": "metadata"
}
```

This maps:
- Tables starting with "pull" (like `pull_requests`) → "core" category
- Tables starting with "issue" (like `issues`) → "core" category
- Tables starting with "commit" (like `commits`) → "activity" category
- Tables starting with "user" (like `users`) → "metadata" category

## How Prefix Matching Works

The discovery script uses the following logic:

1. **Check for `TABLES_*` attributes first** (OSIPI has these)
   - If found, use the category from the class attribute
   - Example: OSIPI has `TABLES_TIME_SERIES = ["Time Series - Interpolated", ...]`

2. **If no `TABLES_*` attributes, try the prefix map** (GitHub/HubSpot need this)
   - Split table name by first `_` to get prefix
   - Example: `pull_requests` → prefix is `pull`
   - Look up prefix in the category prefix map
   - Return the mapped category

3. **Fallback to generic categorization**
   - Use the prefix itself as the category
   - Example: `pull_requests` → category is `pull`

## When to Use Category Prefix Maps

**Use category prefix maps when:**
- The connector source code cannot be modified (maintained by another team)
- You want consistent categorization without code changes
- You need to override default categorization behavior
- You're working with connectors that lack `TABLES_*` attributes

**Don't use category prefix maps when:**
- The connector has `TABLES_*` attributes (they take precedence automatically)
- You can modify the connector source code (add `TABLES_*` instead)
- Default prefix-based categorization is sufficient

## Format

### Option 1: JSON File
```json
{
  "prefix1": "category_name",
  "prefix2": "category_name",
  "prefix3": "another_category"
}
```

### Option 2: Inline JSON String
```bash
--category-prefix-map-json '{"pull":"core","issue":"core","commit":"activity"}'
```

### Option 3: From File
```bash
--category-prefix-map-json "$(cat path/to/category_map.json)"
```

## Examples by Connector

### GitHub Connector

**Category Strategy:**
- **core**: Primary GitHub entities
- **activity**: User activity and interactions
- **metadata**: Reference and configuration data

**Prefix Map:**
```json
{
  "pull": "core",
  "issue": "core",
  "repositories": "core",
  "commit": "activity",
  "review": "activity",
  "comment": "activity",
  "user": "metadata",
  "organization": "metadata",
  "team": "metadata",
  "assignee": "metadata",
  "collaborator": "metadata",
  "branch": "metadata"
}
```

**Usage:**
```bash
python discover_and_classify_tables.py \
  --connector-name github \
  --config-json '{"token": "ghp_xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat examples/github/category_prefix_map.json)" \
  --output-csv github_tables.csv
```

### HubSpot Connector

**Category Strategy:**
- **crm_objects**: Core CRM entities (contacts, companies, deals, tickets)
- **engagement**: Activity records (calls, emails, meetings, tasks)

**Prefix Map:**
```json
{
  "contact": "crm_objects",
  "compan": "crm_objects",
  "deal": "crm_objects",
  "ticket": "crm_objects",
  "call": "engagement",
  "email": "engagement",
  "meeting": "engagement",
  "task": "engagement",
  "note": "engagement"
}
```

**Note:** `compan` matches both `companies` and `company` tables.

**Usage:**
```bash
python discover_and_classify_tables.py \
  --connector-name hubspot \
  --config-json '{"access_token": "xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat examples/hubspot/category_prefix_map.json)" \
  --output-csv hubspot_tables.csv
```

## Grouping Modes

The `--group-by` parameter determines how tables are grouped into pipelines:

### 1. `none`
All tables in a single pipeline.
```bash
--group-by none
```
**Output:** 1 pipeline

### 2. `ingestion_type`
Group by snapshot/append/cdc.
```bash
--group-by ingestion_type
```
**Output:** 2-3 pipelines (e.g., `snapshot`, `append`, `cdc`)

### 3. `category`
Group by logical category (requires prefix map or `TABLES_*`).
```bash
--group-by category \
--category-prefix-map-json '{"pull":"core","commit":"activity"}'
```
**Output:** N pipelines based on categories (e.g., `core`, `activity`, `metadata`)

### 4. `category_and_ingestion_type` (Recommended)
Group by category AND ingestion type for maximum parallelism.
```bash
--group-by category_and_ingestion_type \
--category-prefix-map-json '{"pull":"core","commit":"activity"}'
```
**Output:** N×M pipelines (e.g., `core_snapshot`, `core_append`, `activity_snapshot`)

## Best Practices

### 1. Choose Semantic Categories
Use business-meaningful names that reflect the purpose of tables:
- ✅ Good: `crm_objects`, `engagement`, `metadata`
- ❌ Bad: `group1`, `tables_a`, `misc`

### 2. Keep Prefix Maps Simple
- Use the shortest unique prefix
- Example: `compan` covers both `companies` and `company`
- Avoid over-specific prefixes that won't match variations

### 3. Balance Pipeline Count
- Too few pipelines: Limited parallelism, resource contention
- Too many pipelines: Management overhead, scheduling complexity
- Sweet spot: 3-8 pipelines per connector

### 4. Test First
Always test the prefix map before deployment:
```bash
python discover_and_classify_tables.py \
  --connector-name github \
  --config-json '{"token": "xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json '{"pull":"core"}' \
  --output-csv test.csv

# Review the output
cat test.csv
```

### 5. Document Your Categories
Add README.md files in example directories explaining:
- Category definitions
- Table groupings
- Rationale for the categorization

## Troubleshooting

### Problem: Tables Not Categorized
**Symptom:** Tables appear in "unknown" category

**Solution:**
1. Check prefix map JSON is valid: `cat category_map.json | jq .`
2. Verify table names: `python -c "from sources.github.github import LakeflowConnect; inst = LakeflowConnect({'token': 'xxx'}); print(inst.list_tables())"`
3. Ensure prefixes match table name patterns
4. Add debug output to see what's happening

### Problem: Wrong Category Assignment
**Symptom:** Table assigned to unexpected category

**Solution:**
1. Remember: prefix matching uses the first token before `_`
2. Example: `deal_split` → prefix is `deal`, not `split`
3. Adjust prefix map accordingly

### Problem: Prefix Map Ignored
**Symptom:** Fallback categorization used instead

**Solution:**
1. Ensure `--category-prefix-map-json` is passed correctly
2. Check JSON is properly escaped in shell
3. Use file input to avoid shell escaping issues: `--category-prefix-map-json "$(cat map.json)"`

## Comparison: Category Prefix Map vs TABLES_* Attributes

| Aspect | Category Prefix Map | TABLES_* Attributes |
|--------|---------------------|---------------------|
| **Location** | External config file | Connector source code |
| **Flexibility** | Easy to modify without code changes | Requires connector modification |
| **Maintenance** | Must sync with table changes | Self-contained in connector |
| **Use Case** | Connectors you don't control | Connectors you maintain |
| **Example** | GitHub, HubSpot | OSIPI |

## Migration Path

If you gain control of a connector later, consider migrating from prefix maps to `TABLES_*` attributes:

**Before (prefix map):**
```bash
python discover_and_classify_tables.py \
  --connector-name github \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat map.json)"
```

**After (connector with TABLES_*):**
```python
# In sources/github/github.py
class LakeflowConnect:
    TABLES_CORE = ["issues", "pull_requests", "repositories"]
    TABLES_ACTIVITY = ["commits", "reviews", "comments"]
    # ...
```

```bash
python discover_and_classify_tables.py \
  --connector-name github \
  --group-by category_and_ingestion_type
  # No prefix map needed - uses TABLES_* automatically
```

## Related Documentation

- [GitHub Example README](examples/github/README.md)
- [HubSpot Example README](examples/hubspot/README.md)
- [OSIPI Example README](examples/osipi/README.md) - shows `TABLES_*` approach
- [Load Balanced Deployment README](README.md)
