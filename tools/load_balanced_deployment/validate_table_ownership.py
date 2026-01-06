"""Validate table ownership before deploying load-balanced pipelines.

This script checks if any tables in the deployment plan are already owned by
other pipelines, preventing runtime failures due to DLT table ownership conflicts.

Usage:
    python3 validate_table_ownership.py \
        --csv tables.csv \
        --catalog osipi \
        --schema bronze \
        --profile dogfood

Exit codes:
    0 - No conflicts detected, safe to deploy
    1 - Conflicts detected, deployment will fail
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

from databricks.sdk import WorkspaceClient


def read_tables_from_csv(csv_path: Path) -> Dict[str, List[str]]:
    """Read tables from CSV and group by pipeline group (category).

    Returns:
        Dict mapping pipeline_group -> list of table names
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    pipeline_tables: Dict[str, List[str]] = {}

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            source_table = (row.get("source_table") or "").strip()
            category = (row.get("category") or "").strip() or "default"

            if not source_table:
                continue

            pipeline_tables.setdefault(category, []).append(source_table)

    return pipeline_tables


def check_table_ownership(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    table_name: str,
) -> Tuple[bool, str | None]:
    """Check if a table is owned by another pipeline.

    Returns:
        (is_owned, owner_pipeline_id)
    """
    try:
        table_info = w.tables.get(full_name=f"{catalog}.{schema}.{table_name}")

        # Check if table has a pipeline owner
        if table_info.properties and "pipelines.pipelineId" in table_info.properties:
            owner_id = table_info.properties["pipelines.pipelineId"]
            return True, owner_id

        return False, None

    except Exception as e:
        # Table doesn't exist - no conflict
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return False, None
        # Other errors - print warning but don't block
        print(f"  ⚠️  Warning: Could not check {table_name}: {e}")
        return False, None


def validate_deployment(
    csv_path: Path,
    catalog: str,
    schema: str,
    profile: str | None = None,
) -> bool:
    """Validate that no tables are owned by other pipelines.

    Returns:
        True if safe to deploy, False if conflicts detected
    """
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    print(f"Validating table ownership for: {catalog}.{schema}")
    print(f"Reading deployment plan from: {csv_path}\n")

    # Read tables from CSV
    pipeline_tables = read_tables_from_csv(csv_path)

    total_tables = sum(len(tables) for tables in pipeline_tables.values())
    print(f"Found {len(pipeline_tables)} pipeline groups with {total_tables} total tables\n")

    # Check ownership for all tables
    conflicts: Dict[str, List[Tuple[str, str]]] = {}  # pipeline_group -> [(table, owner_id)]
    checked_tables: Set[str] = set()

    for pipeline_group, tables in sorted(pipeline_tables.items()):
        print(f"Checking {pipeline_group} ({len(tables)} tables)...")

        group_conflicts = []
        for table_name in tables:
            if table_name in checked_tables:
                continue  # Already checked (same table in multiple groups)

            checked_tables.add(table_name)
            is_owned, owner_id = check_table_ownership(w, catalog, schema, table_name)

            if is_owned and owner_id:
                group_conflicts.append((table_name, owner_id))

        if group_conflicts:
            conflicts[pipeline_group] = group_conflicts
        else:
            print(f"  ✓ No conflicts\n")

    # Report results
    print("=" * 70)

    if not conflicts:
        print("✓ VALIDATION PASSED")
        print(f"\nNo table ownership conflicts detected.")
        print(f"Safe to deploy {len(pipeline_tables)} pipelines to {catalog}.{schema}")
        return True

    # Conflicts found
    print("✗ VALIDATION FAILED")
    print(f"\n⚠️  TABLE OWNERSHIP CONFLICTS DETECTED:\n")

    # Get pipeline names for better error messages
    pipeline_names: Dict[str, str] = {}
    try:
        all_pipelines = list(w.pipelines.list_pipelines())
        pipeline_names = {p.pipeline_id: p.name or p.pipeline_id for p in all_pipelines}
    except Exception:
        pass  # Fall back to just showing IDs

    for pipeline_group, group_conflicts in sorted(conflicts.items()):
        print(f"\n{pipeline_group}:")
        for table_name, owner_id in group_conflicts:
            owner_name = pipeline_names.get(owner_id, owner_id)
            print(f"  - {table_name}")
            print(f"    Currently owned by: {owner_name}")

    # Provide solutions
    print("\n" + "=" * 70)
    print("RECOMMENDED SOLUTIONS:\n")

    print("Option 1: Use a different target schema (RECOMMENDED)")
    print("  --schema bronze_v2")
    print("  --schema bronze_load_balanced")
    print("  This avoids conflicts and allows side-by-side comparison.\n")

    print("Option 2: Delete conflicting pipelines first")
    print("  Use the Databricks UI or SDK to delete old pipelines.")
    print("  WARNING: This removes pipeline history and lineage.\n")

    print("Option 3: Drop the conflicting tables")
    print("  Use SQL to drop tables before redeployment.")
    print("  WARNING: This causes data loss.\n")

    print("For more details, see:")
    print("  tools/load_balanced_deployment/README.md")
    print("  Section: '⚠️ CRITICAL: Table Ownership and Load Balancing Changes'")

    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate table ownership before deploying load-balanced pipelines"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file with table assignments"
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Target catalog name"
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Target schema name"
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile to use (optional)"
    )

    args = parser.parse_args()

    csv_path = Path(args.csv)

    try:
        is_valid = validate_deployment(
            csv_path=csv_path,
            catalog=args.catalog,
            schema=args.schema,
            profile=args.profile,
        )

        sys.exit(0 if is_valid else 1)

    except Exception as e:
        print(f"\n✗ Validation error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
