#!/usr/bin/env python3
"""Add 'tableConfigs' to UC Connection external options allowlist.

This script updates a Unity Catalog connection to include 'tableConfigs'
in its externalOptionsAllowList, which is required after the upstream
breaking change (commit 9e5c251).

Usage:
    python add_tableconfigs_to_connection.py <connection_name>
    python add_tableconfigs_to_connection.py <connection_name> --profile dogfood
"""

import argparse
import sys
from databricks.sdk import WorkspaceClient


def add_tableconfigs_to_connection(connection_name: str, profile: str = None):
    """Add tableConfigs to connection's external options allowlist."""

    print(f"{'='*80}")
    print(f"Adding tableConfigs to connection: {connection_name}")
    print('='*80)

    # Initialize workspace client
    if profile:
        w = WorkspaceClient(profile=profile)
        print(f"Using profile: {profile}")
    else:
        w = WorkspaceClient()
        print("Using default profile")

    print()

    try:
        # Get current connection
        conn_info = w.api_client.do("GET", f"/api/2.1/unity-catalog/connections/{connection_name}")
        print(f"✓ Found connection")
        print(f"  Type: {conn_info.get('connection_type')}")

        current_options = conn_info.get('options', {})
        external_options_allowlist = current_options.get('externalOptionsAllowList', '')

        print(f"  Current allowlist: '{external_options_allowlist}'")

        # Add tableConfigs to allowlist
        if 'tableConfigs' not in external_options_allowlist:
            if external_options_allowlist:
                new_allowlist = f"{external_options_allowlist},tableConfigs"
            else:
                new_allowlist = "tableConfigs"

            print(f"\n  Adding 'tableConfigs' to allowlist...")
            print(f"  New allowlist: '{new_allowlist}'")

            current_options['externalOptionsAllowList'] = new_allowlist

            # Update connection
            result = w.api_client.do(
                "PATCH",
                f"/api/2.1/unity-catalog/connections/{connection_name}",
                body={"options": current_options}
            )

            print(f"\n✓ Connection updated successfully!")

            # Verify update
            updated_options = result.get('options', {})
            updated_allowlist = updated_options.get('externalOptionsAllowList', '')
            print(f"  Verified allowlist: '{updated_allowlist}'")

            return True
        else:
            print(f"\n✓ 'tableConfigs' already in allowlist - no update needed")
            return True

    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "not found" in str(e).lower():
            print(f"\n✗ Error: Connection '{connection_name}' does not exist")
            return False
        else:
            print(f"\n✗ Error updating connection: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Add 'tableConfigs' to UC Connection external options allowlist"
    )
    parser.add_argument(
        "connection_name",
        help="Name of the Unity Catalog connection to update"
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile to use (optional)",
        default=None
    )

    args = parser.parse_args()

    success = add_tableconfigs_to_connection(args.connection_name, args.profile)

    if success:
        print(f"\n{'='*80}")
        print("Next Steps")
        print('='*80)
        print(f"\nConnection '{args.connection_name}' is now ready to use tableConfigs.")
        print("\nIn your pipeline, you can now pass table-specific options:")
        print(f"""
  table_configs = {{
      "table1": {{"option1": "value1"}},
      "table2": {{"option2": "value2"}}
  }}

  df = spark.read.format("lakeflow_connect")
      .option("databricks.connection", "{args.connection_name}")
      .option("tableName", "_lakeflow_metadata")
      .option("tableNameList", "table1,table2")
      .option("tableConfigs", json.dumps(table_configs))
      .load()
        """)
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
