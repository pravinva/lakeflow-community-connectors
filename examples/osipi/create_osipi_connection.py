#!/usr/bin/env python3
"""
Interactive CLI script to create or update OSIPI Unity Catalog connections.

This script helps you create a UC connection for the OSIPI community connector
with all necessary options including:
- sourceName
- externalOptionsAllowList (for table_configuration options)
- Authentication credentials (retrieved from Databricks secrets and base64-decoded)

Usage:
    python create_osipi_connection.py
    python create_osipi_connection.py --connection-name my_osipi_conn
    python create_osipi_connection.py --update  # Update existing connection
"""

import argparse
import base64
import sys
from databricks.sdk import WorkspaceClient


# OSIPI external options that can be passed via table_configuration
OSIPI_EXTERNAL_OPTIONS = [
    "endTime",
    "end_time",
    "startTime",
    "start_time",
    "lookback_minutes",
    "window_seconds",
    "dataserver_webid",
    "maxCount",
    "startIndex",
    "maxTotalCount",
    "nameFilter",
    "tag_webids",
    "point_webids",
    "assetdatabase_webid",
    "eventframe_template_webid",
    "selectedFields",
    "tags_per_request",
    "prefer_streamset",
    "interval",
    "sampleInterval",
    "intervals",
    "summaryType",
    "calculationBasis",
    "summaryDuration",
    "time",
    "default_tags",
    "default_points",
]


def get_secret_value(w: WorkspaceClient, scope: str, key: str) -> str:
    """
    Retrieve a secret from Databricks secrets and decode if base64-encoded.

    Args:
        w: WorkspaceClient instance
        scope: Secret scope name
        key: Secret key name

    Returns:
        Decoded secret value
    """
    try:
        # Get the secret value (returns base64-encoded string)
        secret_bytes = w.secrets.get_secret(scope=scope, key=key)

        # Secret API returns base64-encoded bytes
        secret_value = secret_bytes.value

        # Try to decode as base64 first
        try:
            decoded = base64.b64decode(secret_value).decode('utf-8')
            return decoded
        except Exception:
            # If base64 decode fails, return as-is
            return secret_value

    except Exception as e:
        print(f"Error retrieving secret {scope}/{key}: {e}")
        return None


def list_secret_scopes(w: WorkspaceClient):
    """List all available secret scopes."""
    try:
        scopes = list(w.secrets.list_scopes())
        if not scopes:
            print("No secret scopes found.")
            return []

        print("\nAvailable secret scopes:")
        for i, scope in enumerate(scopes, 1):
            print(f"  {i}. {scope.name}")
        return scopes
    except Exception as e:
        print(f"Error listing secret scopes: {e}")
        return []


def list_secrets_in_scope(w: WorkspaceClient, scope: str):
    """List all secrets in a given scope."""
    try:
        secrets = list(w.secrets.list_secrets(scope=scope))
        if not secrets:
            print(f"No secrets found in scope '{scope}'.")
            return []

        print(f"\nSecrets in scope '{scope}':")
        for i, secret in enumerate(secrets, 1):
            print(f"  {i}. {secret.key}")
        return secrets
    except Exception as e:
        print(f"Error listing secrets in scope '{scope}': {e}")
        return []


def prompt_for_secret(w: WorkspaceClient, credential_name: str) -> dict:
    """
    Interactively prompt for secret location and retrieve the credential.

    Args:
        w: WorkspaceClient instance
        credential_name: Name of the credential to retrieve (e.g., "access_token", "username")

    Returns:
        Dict with 'value' key containing the credential, or None if skipped
    """
    print(f"\n{'='*60}")
    print(f"Configure {credential_name}")
    print(f"{'='*60}")

    # Ask user how to provide the credential
    print("\nHow would you like to provide this credential?")
    print("  1. Retrieve from Databricks Secret")
    print("  2. Enter manually")
    print("  3. Skip (not required)")

    choice = input("\nEnter choice (1-3): ").strip()

    if choice == "1":
        # List secret scopes
        scopes = list_secret_scopes(w)
        if not scopes:
            print("No secret scopes available. Please enter manually or skip.")
            return None

        # Prompt for scope
        scope_choice = input(f"\nEnter scope number (1-{len(scopes)}): ").strip()
        try:
            scope_idx = int(scope_choice) - 1
            scope_name = scopes[scope_idx].name
        except (ValueError, IndexError):
            print("Invalid choice. Skipping.")
            return None

        # List secrets in scope
        secrets = list_secrets_in_scope(w, scope_name)
        if not secrets:
            print("No secrets available in this scope. Please enter manually or skip.")
            return None

        # Prompt for secret key
        key_choice = input(f"\nEnter secret number (1-{len(secrets)}): ").strip()
        try:
            key_idx = int(key_choice) - 1
            secret_key = secrets[key_idx].key
        except (ValueError, IndexError):
            print("Invalid choice. Skipping.")
            return None

        # Retrieve and decode the secret
        print(f"\nRetrieving secret from {scope_name}/{secret_key}...")
        value = get_secret_value(w, scope_name, secret_key)

        if value:
            print("  ✓ Secret retrieved and decoded successfully")
            return {"value": value, "source": f"{scope_name}/{secret_key}"}
        else:
            print("  ✗ Failed to retrieve secret")
            return None

    elif choice == "2":
        # Manual entry
        value = input(f"\nEnter {credential_name}: ").strip()
        if value:
            return {"value": value, "source": "manual"}
        else:
            print("Empty value. Skipping.")
            return None

    elif choice == "3":
        # Skip
        print(f"Skipping {credential_name}.")
        return None

    else:
        print("Invalid choice. Skipping.")
        return None


def create_connection(
    w: WorkspaceClient,
    connection_name: str,
    credentials: dict,
    osipi_url: str = None,
    update: bool = False
):
    """
    Create or update a UC connection for OSIPI.

    Args:
        w: WorkspaceClient instance
        connection_name: Name for the connection
        credentials: Dict of credential values (e.g., {"access_token": "Bearer xyz"})
        osipi_url: Optional OSIPI Web API URL (for MockPI or custom endpoints)
        update: If True, update existing connection; if False, create new
    """
    # Build connection options (includes credentials - platform filters them from API responses)
    options = {
        "sourceName": "osipi",
        "externalOptionsAllowList": ",".join(OSIPI_EXTERNAL_OPTIONS),
    }

    # Add OSIPI URL if provided (e.g., for MockPI)
    if osipi_url:
        options["pi_base_url"] = osipi_url

    # Add credentials to options (like the UI does)
    # Platform automatically filters these from API GET responses but injects them at runtime
    if credentials:
        options.update(credentials)

    print("\n" + "="*60)
    print("Connection Configuration")
    print("="*60)
    print(f"Connection Name: {connection_name}")
    print(f"Source: osipi")
    if osipi_url:
        print(f"OSIPI URL: {osipi_url}")
    print(f"External Options: {len(OSIPI_EXTERNAL_OPTIONS)} table options allowed")
    if credentials:
        print(f"Credentials: {list(credentials.keys())} (stored in options, filtered from API responses)")
    print("="*60)

    # Confirm before proceeding
    confirm = input("\nProceed with connection creation/update? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("Aborted.")
        return

    try:
        if update:
            # Update existing connection
            # Note: Credentials cannot be updated via PATCH - must recreate connection
            body = {
                "name": connection_name,
                "options": options
            }

            print(f"\nUpdating connection '{connection_name}'...")
            print("Note: Credentials cannot be updated via PATCH API")
            print("To update credentials, delete and recreate the connection")
            result = w.api_client.do(
                "PATCH",
                f"/api/2.1/unity-catalog/connections/{connection_name}",
                body=body
            )
            print("\n✓ Connection options updated successfully!")
        else:
            # Create new connection with credentials in options (like UI does)
            body = {
                "name": connection_name,
                "connection_type": "GENERIC_LAKEFLOW_CONNECT",
                "options": options,  # Credentials are in options, not properties!
                "comment": "OSIPI community connector connection"
            }

            print(f"\nCreating connection '{connection_name}'...")
            result = w.api_client.do(
                "POST",
                "/api/2.1/unity-catalog/connections",
                body=body
            )
            print("\n✓ Connection created successfully!")

        print(f"\nConnection Details:")
        print(f"  Name: {result.get('name')}")
        print(f"  Type: {result.get('connection_type')}")
        print(f"  ID: {result.get('connection_id')}")
        if result.get('credential_type'):
            print(f"  Credential Type: {result.get('credential_type')}")

        print(f"\nNext Steps:")
        print(f"  1. The connection is now ready to use")
        print(f"  2. Reference it in your DLT pipeline ingest files:")
        print(f"     connection_name=\"{connection_name}\"")
        if credentials:
            print(f"  3. Credentials are stored in options field and injected automatically at runtime")
        else:
            print(f"  3. No credentials configured - use platform credential injection")

    except Exception as e:
        print(f"\n✗ Failed to {'update' if update else 'create'} connection: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Interactive CLI to create/update OSIPI Unity Catalog connections"
    )
    parser.add_argument(
        "--connection-name",
        default="osipi_connection_lakeflow",
        help="Name for the UC connection (default: osipi_connection_lakeflow)"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing connection instead of creating new"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile to use (default: None, uses default auth)"
    )
    parser.add_argument(
        "--osipi-url",
        default=None,
        help="OSIPI Web API URL (for MockPI or custom endpoints)"
    )

    args = parser.parse_args()

    # Initialize workspace client
    print("Initializing Databricks workspace client...")
    try:
        if args.profile:
            w = WorkspaceClient(profile=args.profile)
        else:
            w = WorkspaceClient()

        # Test connection
        current_user = w.current_user.me()
        print(f"✓ Connected as: {current_user.user_name}")
    except Exception as e:
        print(f"✗ Failed to connect to Databricks: {e}")
        sys.exit(1)

    print("\n" + "="*60)
    print("OSIPI Unity Catalog Connection Setup")
    print("="*60)
    print("\nThis script will help you create a UC connection for the OSIPI")
    print("community connector with proper configuration.")
    print("\nThe connection will include:")
    print("  - sourceName: osipi")
    print(f"  - externalOptionsAllowList: {len(OSIPI_EXTERNAL_OPTIONS)} table options")
    if args.osipi_url:
        print(f"  - url: {args.osipi_url} (custom OSIPI endpoint)")
    print("\nCredentials are stored in OPTIONS field (like the UI does).")
    print("The platform filters them from API responses but injects them at runtime.")

    # Prompt for OSIPI URL if not provided
    osipi_url = args.osipi_url
    if not osipi_url:
        configure_url = input("\nDo you want to configure a custom OSIPI URL (e.g., MockPI)? (yes/no): ").strip().lower()
        if configure_url in ["yes", "y"]:
            osipi_url = input("Enter OSIPI Web API URL: ").strip()
            if osipi_url:
                print(f"✓ Custom URL configured: {osipi_url}")

    # Prompt for credentials (optional)
    print("\n" + "="*60)
    print("Optional: Configure Authentication Credentials")
    print("="*60)
    print("\nCredentials will be stored in the OPTIONS field (matching UI behavior).")
    print("They are filtered from API responses but injected at runtime.")

    configure_creds = input("\nDo you want to configure credentials? (yes/no): ").strip().lower()

    credentials = {}
    if configure_creds in ["yes", "y"]:
        print("\nOSIPI supports multiple authentication methods:")
        print("  1. Bearer token (access_token)")
        print("  2. Basic auth (username/password)")
        print("  3. OIDC/OAuth (client credentials)")
        print("  4. Skip for now")

        auth_choice = input("\nSelect authentication method (1-4): ").strip()

        if auth_choice == "1":
            # Bearer token authentication
            token_cred = prompt_for_secret(w, "access_token")
            if token_cred:
                # Format as Bearer token if not already
                token_value = token_cred["value"]
                if not token_value.startswith("Bearer "):
                    token_value = f"Bearer {token_value}"
                credentials["access_token"] = token_value
                print(f"✓ Access token configured (from {token_cred['source']})")

        elif auth_choice == "2":
            # Basic auth (username + password)
            username_cred = prompt_for_secret(w, "username")
            if username_cred:
                credentials["username"] = username_cred["value"]
                print(f"✓ Username configured (from {username_cred['source']})")

            password_cred = prompt_for_secret(w, "password")
            if password_cred:
                credentials["password"] = password_cred["value"]
                print(f"✓ Password configured (from {password_cred['source']})")

        elif auth_choice == "3":
            # OIDC/OAuth with client credentials
            print("\nOIDC/OAuth Configuration:")
            print("The connector will automatically handle token refresh.")
            print("You need to provide client_id and client_secret.")

            client_id_cred = prompt_for_secret(w, "client_id")
            if client_id_cred:
                credentials["client_id"] = client_id_cred["value"]
                print(f"✓ Client ID configured (from {client_id_cred['source']})")

            client_secret_cred = prompt_for_secret(w, "client_secret")
            if client_secret_cred:
                credentials["client_secret"] = client_secret_cred["value"]
                print(f"✓ Client secret configured (from {client_secret_cred['source']})")

            # Optional workspace_host for custom OIDC endpoints
            print("\nOptional: Specify workspace_host for custom OIDC token endpoint.")
            print("Default: Uses pi_base_url host with /oidc/v1/token path")
            workspace_host_input = input("Enter workspace host (press Enter to skip): ").strip()
            if workspace_host_input:
                credentials["workspace_host"] = workspace_host_input
                print(f"✓ Workspace host configured: {workspace_host_input}")

    # Create or update the connection
    create_connection(
        w=w,
        connection_name=args.connection_name,
        credentials=credentials,
        osipi_url=osipi_url,
        update=args.update
    )


if __name__ == "__main__":
    main()
