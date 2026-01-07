#!/usr/bin/env python3
"""
Debug script to simulate the load-balanced deployment notebook execution.
This replicates cells 1-7 to identify where the failure occurs.
"""

import subprocess
import os
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
import base64

# Configuration (Cell 3 from notebook)
CONNECTOR_NAME = "osipi"
CONNECTION_NAME = "osipi_connection_lakeflow"
DEST_CATALOG = "osipi"
DEST_SCHEMA = "bronze1"

USE_PRESET = True  # Using preset to skip discovery
PRESET_CSV_PATH = f"examples/{CONNECTOR_NAME}/preset_by_category_and_ingestion.csv"

WORK_DIR = "/tmp/load_balanced_deployment"
CSV_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_tables.csv"
INGEST_FILES_DIR = f"{WORK_DIR}/{CONNECTOR_NAME}_ingest_files"
DAB_YAML_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_bundle/databricks.yml"

w = WorkspaceClient(profile="dogfood")
USERNAME = w.current_user.me().user_name
WORKSPACE_INGEST_PATH = f"/Workspace/Users/{USERNAME}/{CONNECTOR_NAME}_ingest"

CLUSTER_NUM_WORKERS = 2
EMIT_SCHEDULED_JOBS = True
PAUSE_JOBS = True

print("="*60)
print("LOAD-BALANCED DEPLOYMENT DEBUG SCRIPT")
print("="*60)
print(f"Connector: {CONNECTOR_NAME}")
print(f"Username: {USERNAME}")
print(f"Workspace Ingest Path: {WORKSPACE_INGEST_PATH}")
print()

# Step 1: Create work directories (Cell 4)
print("Step 1: Creating work directories...")
Path(WORK_DIR).mkdir(parents=True, exist_ok=True)
Path(INGEST_FILES_DIR).mkdir(parents=True, exist_ok=True)
Path(DAB_YAML_PATH).parent.mkdir(parents=True, exist_ok=True)

if USE_PRESET:
    CSV_PATH = PRESET_CSV_PATH
    print(f"Using preset CSV: {CSV_PATH}")

    if not Path(CSV_PATH).exists():
        print(f"ERROR: Preset CSV not found at {CSV_PATH}")
        exit(1)

    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))

print("\n" + "="*60)

# Step 2: Generate Ingest Files (Cell 5)
print("Step 2: Generating ingest files...")
print(f"Running: generate_ingest_files.py")

cmd = [
    "python3",
    "generate_ingest_files.py",
    "--csv", CSV_PATH,
    "--output-dir", INGEST_FILES_DIR,
    "--source-name", CONNECTOR_NAME,
    "--connection-name", CONNECTION_NAME,
    "--catalog", DEST_CATALOG,
    "--schema", DEST_SCHEMA,
    "--common-table-config-json", "{}",
]

result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)

if result.returncode != 0:
    print("ERROR during ingest file generation:")
    print(result.stderr)
    exit(1)

print(result.stdout)

ingest_files = list(Path(INGEST_FILES_DIR).glob("ingest_*.py"))
print(f"\nGenerated {len(ingest_files)} ingest files:")
for f in sorted(ingest_files):
    print(f"  - {f.name}")

print("\n" + "="*60)

# Step 3: Generate DAB YAML (Cell 6)
print("Step 3: Generating DAB YAML...")
print(f"Running: generate_dab_yaml.py")

cmd = [
    "python3",
    "generate_dab_yaml.py",
    "--connector-name", CONNECTOR_NAME,
    "--input-csv", CSV_PATH,
    "--output-yaml", DAB_YAML_PATH,
    "--ingest-files-path", WORKSPACE_INGEST_PATH,
    "--connection-name", CONNECTION_NAME,
    "--catalog", DEST_CATALOG,
    "--schema", DEST_SCHEMA,
    "--num-workers", str(CLUSTER_NUM_WORKERS),
]

if EMIT_SCHEDULED_JOBS:
    cmd.append("--emit-jobs")

if PAUSE_JOBS:
    cmd.append("--pause-jobs")

result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)

if result.returncode != 0:
    print("ERROR during DAB YAML generation:")
    print(result.stderr)
    exit(1)

print(result.stdout)

print(f"\nPreview of generated YAML:")
with open(DAB_YAML_PATH, 'r') as f:
    lines = f.readlines()
    print("".join(lines[:30]))
    if len(lines) > 30:
        print(f"... ({len(lines) - 30} more lines)")

print("\n" + "="*60)

# Step 4: Upload Ingest Files to Workspace (Cell 7)
print("Step 4: Uploading ingest files to workspace...")
print(f"Target: {WORKSPACE_INGEST_PATH}")

# Ensure target directory exists
try:
    w.workspace.mkdirs(WORKSPACE_INGEST_PATH)
    print(f"  ✓ Created directory: {WORKSPACE_INGEST_PATH}")
except Exception as e:
    print(f"  Directory already exists or error: {e}")

uploaded_count = 0
for ingest_file in sorted(Path(INGEST_FILES_DIR).glob("ingest_*.py")):
    target_path = f"{WORKSPACE_INGEST_PATH}/{ingest_file.name}"

    with open(ingest_file, 'r') as f:
        content = f.read()
        encoded_content = base64.b64encode(content.encode()).decode()

    try:
        w.workspace.import_(
            path=target_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=encoded_content
        )
        print(f"  ✓ Uploaded {ingest_file.name} -> {target_path}")
        uploaded_count += 1
    except Exception as e:
        print(f"  ✗ Failed to upload {ingest_file.name}: {e}")

print(f"\n✓ Uploaded {uploaded_count} ingest files to workspace")

print("\n" + "="*60)

# Step 5: Deploy with Databricks Asset Bundle (Cell 8)
print("Step 5: Deploying with Databricks Asset Bundle...")

bundle_dir = str(Path(DAB_YAML_PATH).parent)
print(f"Bundle directory: {bundle_dir}")
print(f"Bundle directory contents:")

for f in Path(bundle_dir).iterdir():
    print(f"  - {f.name}")

os.chdir(bundle_dir)

print("\nRunning: databricks bundle validate...")
result = subprocess.run(
    ["databricks", "--profile", "dogfood", "bundle", "validate"],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    print("ERROR during bundle validation:")
    print("STDERR:")
    print(result.stderr)
    print("\nSTDOUT:")
    print(result.stdout)
    exit(1)

print(result.stdout)

print("\nRunning: databricks bundle deploy...")
result = subprocess.run(
    ["databricks", "--profile", "dogfood", "bundle", "deploy"],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    print("ERROR during bundle deployment:")
    print("STDERR:")
    print(result.stderr)
    print("\nSTDOUT:")
    print(result.stdout)
    exit(1)

print(result.stdout)

print("\n" + "="*60)
print("SUCCESS: All steps completed successfully!")
print("="*60)
