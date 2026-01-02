# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy notebook-based DLT pipelines (no local CLI)
# MAGIC
# MAGIC This notebook creates/updates **DLT pipelines** (and optional **Jobs schedules**) for Lakeflow Community Connectors
# MAGIC by generating DLT notebooks from a metadata CSV and deploying resources via the **Databricks SDK**.
# MAGIC
# MAGIC **No Databricks CLI. No local laptop commands.**
# MAGIC
# MAGIC What it does:
# MAGIC - Uploads the connector generated source into your workspace (`/Workspace/Users/<you>/connectors/<connector>_generated_source.py`)
# MAGIC - Generates one DLT notebook per `pipeline_group` from the CSV
# MAGIC - Uploads those notebooks to workspace
# MAGIC - Creates (or updates) DLT pipelines that reference those notebooks
# MAGIC - Optionally creates Jobs with schedules to trigger pipelines

# COMMAND ----------

# DBTITLE 1,Configuration (widgets)
# This notebook is connector-agnostic. Pick a preset (OSIPI/HubSpot/Zendesk) or use CUSTOM.
# Required in most cases: CONNECTOR_NAME, CSV_REL_PATH, DEST_CATALOG, DEST_SCHEMA.

import json as _json
import os as _os
import sys as _sys

# Prevent Python from writing __pycache__ into the synced Repo checkout (keeps Repos git status clean).
_os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
_sys.dont_write_bytecode = True

# Preset configs (edit/extend as needed)
_PRESETS = {
    # OSIPI mock bearer token example
    "osipi": {
        "default_csv": "tools/notebook_based_deployment/examples/osipi/osipi_by_category_and_ingestion_type.csv",
        "default_generated_source": "sources/osipi/_generated_osipi_python_source.py",
        "default_connector_config": {
            "secrets_scope": "sp-osipi",
            "secret_mappings": {"bearer_value_tmp": ["mock-bearer-token"]},
            "static_options": {
                "pi_base_url": "https://mock-piwebapi-912141448724.us-central1.run.app",
                "verify_ssl": "true",
            },
            "dynamic_options": {},
        },
    },
    "hubspot": {
        "default_csv": "tools/notebook_based_deployment/examples/hubspot/hubspot_tables.csv",
        "default_generated_source": "sources/hubspot/_generated_hubspot_python_source.py",
        "default_connector_config": {
            "secrets_scope": "hubspot",
            "secret_mappings": {"access_token": ["access_token"]},
            "static_options": {},
            "dynamic_options": {},
        },
    },
    "zendesk": {
        "default_csv": "tools/notebook_based_deployment/examples/zendesk/zendesk_tables.csv",
        "default_generated_source": "sources/zendesk/_generated_zendesk_python_source.py",
        "default_connector_config": {
            "secrets_scope": "zendesk",
            "secret_mappings": {
                "api_token": ["api_token"],
                "subdomain": ["subdomain"],
            },
            "static_options": {},
            "dynamic_options": {},
        },
    },
    "custom": {
        "default_csv": "",
        "default_generated_source": "",
        "default_connector_config": {
            "secrets_scope": "",
            "secret_mappings": {},
            "static_options": {},
            "dynamic_options": {},
        },
    },
}

try:
    dbutils.widgets.dropdown("CONNECTOR_PRESET", "osipi", ["osipi", "hubspot", "zendesk", "custom"], "Connector preset")
    preset = dbutils.widgets.get("CONNECTOR_PRESET")

    dbutils.widgets.text("CONNECTOR_NAME", preset if preset != "custom" else "", "Connector name (e.g. osipi) NOT UC connection name")

    dbutils.widgets.text("CSV_REL_PATH", _PRESETS[preset]["default_csv"], "CSV relative path (in repo)")

    # Destination tables location
    dbutils.widgets.text("DEST_CATALOG", "", "Destination catalog")
    dbutils.widgets.text("DEST_SCHEMA", "", "Destination schema")

    # Where to put generated notebooks in the Workspace
    dbutils.widgets.text("WORKSPACE_NOTEBOOKS_DIR", "", "Workspace dir for generated DLT notebooks (optional)")

    # Store connector source where DLT clusters can open() it
    dbutils.widgets.dropdown("USE_UC_VOLUME", "true", ["true", "false"], "Store connector source in UC Volume")
    dbutils.widgets.text("UC_VOLUME_NAME", "lakeflow_connectors", "UC Volume name")

    # Connector source file inside this repo (relative to repo root)
    dbutils.widgets.text("CONNECTOR_GENERATED_SOURCE_REL_PATH", _PRESETS[preset]["default_generated_source"], "Generated source rel path (in repo)")

    # Pipeline creation behavior
    dbutils.widgets.dropdown("DEVELOPMENT", "false", ["true", "false"], "Pipeline development")
    dbutils.widgets.dropdown("CONTINUOUS", "false", ["true", "false"], "Pipeline continuous")

    # Jobs / lifecycle
    dbutils.widgets.dropdown("CREATE_SCHEDULED_JOBS", "true", ["true", "false"], "Create jobs from CSV schedules")
    dbutils.widgets.dropdown("RECREATE_EXISTING", "false", ["true", "false"], "Delete/recreate existing resources")
    dbutils.widgets.dropdown("PURGE_WORKSPACE_DIRS", "false", ["true", "false"], "Purge workspace notebooks dir before upload")
    dbutils.widgets.text("DLT_NUM_WORKERS", "1", "DLT num_workers (classic DLT)")

    dbutils.widgets.text(
        "CONNECTOR_CONFIG_JSON",
        _json.dumps(_PRESETS[preset]["default_connector_config"]),
        "Connector config JSON (secrets + static/dynamic options)",
    )
except Exception:
    pass


def _w(name: str, default: str) -> str:
    try:
        v = dbutils.widgets.get(name)
        return v if v is not None else default
    except Exception:
        return default


def _w_bool(name: str, default: bool) -> bool:
    raw = _w(name, "true" if default else "false").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _w_int(name: str, default: int) -> int:
    raw = _w(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        return default


CONNECTOR_PRESET = _w("CONNECTOR_PRESET", "osipi").strip()
CONNECTOR_NAME = _w("CONNECTOR_NAME", "osipi").strip()
CSV_REL_PATH = _w("CSV_REL_PATH", "").strip()
DEST_CATALOG = _w("DEST_CATALOG", "").strip()
DEST_SCHEMA = _w("DEST_SCHEMA", "").strip()

WORKSPACE_NOTEBOOKS_DIR = _w("WORKSPACE_NOTEBOOKS_DIR", "").strip() or None
WORKSPACE_CONNECTORS_DIR = None  # not used in v2 (connector source stored on Volumes/DBFS)

USE_UC_VOLUME = _w_bool("USE_UC_VOLUME", True)
UC_VOLUME_NAME = _w("UC_VOLUME_NAME", "lakeflow_connectors").strip()

CONNECTOR_GENERATED_SOURCE_REL_PATH = _w("CONNECTOR_GENERATED_SOURCE_REL_PATH", "").strip()

DEVELOPMENT = _w_bool("DEVELOPMENT", False)
CONTINUOUS = _w_bool("CONTINUOUS", False)
CREATE_SCHEDULED_JOBS = _w_bool("CREATE_SCHEDULED_JOBS", True)
RECREATE_EXISTING = _w_bool("RECREATE_EXISTING", False)
PURGE_WORKSPACE_DIRS = _w_bool("PURGE_WORKSPACE_DIRS", False)
DLT_NUM_WORKERS = _w_int("DLT_NUM_WORKERS", 1)

_connector_config_raw = _w("CONNECTOR_CONFIG_JSON", "").strip()
if not _connector_config_raw:
    raise ValueError("CONNECTOR_CONFIG_JSON widget is empty; provide connector auth/options config.")
connector_config = _json.loads(_connector_config_raw)
if not isinstance(connector_config, dict):
    raise ValueError("CONNECTOR_CONFIG_JSON must be a JSON object.")

# Minimal validation
if not CONNECTOR_NAME:
    raise ValueError("CONNECTOR_NAME is empty")
if not CSV_REL_PATH:
    raise ValueError("CSV_REL_PATH is empty")
if not DEST_CATALOG or not DEST_SCHEMA:
    raise ValueError("DEST_CATALOG/DEST_SCHEMA must be set")
if not CONNECTOR_GENERATED_SOURCE_REL_PATH:
    # default if not provided
    CONNECTOR_GENERATED_SOURCE_REL_PATH = f"sources/{CONNECTOR_NAME}/_generated_{CONNECTOR_NAME}_python_source.py"

# COMMAND ----------

# DBTITLE 1,Imports / context helpers
import csv
import importlib.util
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, workspace


def _ctx_notebook_path() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return ""


def _ctx_user() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    except Exception:
        return ""


def _repo_root_fs_from_notebook_path(nb_path: str) -> Path:
    """Infer the project root on the driver filesystem from the workspace notebook path.

    Works for both:
    - Databricks Repos paths:      /Repos/<user>/<repo>/...
    - Workspace user folder paths: /Users/<user>/<folder>/...

    Strategy:
    - Convert the workspace notebook path to the driver-mounted workspace filesystem:
        /Workspace/<notebookPath>
    - Walk up parents until we find a folder containing `sources/`.
    """
    nb_path = (nb_path or "").strip()
    if not nb_path.startswith("/"):
        raise ValueError(f"Unexpected notebookPath (expected absolute): {nb_path!r}")

    fs_path = Path("/Workspace") / nb_path.lstrip("/")

    for p in [fs_path] + list(fs_path.parents):
        if (p / "sources").is_dir():
            return p

    raise ValueError(
        "Unable to infer project root containing `sources/` from notebookPath. "
        "Run this notebook from within a synced project folder that contains `sources/` "
        f"(e.g. under /Repos/... or /Users/...). notebookPath={nb_path!r}"
    )


def _mkdirs(w: WorkspaceClient, path: str) -> None:
    w.workspace.mkdirs(path)


def _import_py_source(w: WorkspaceClient, *, local_path: Path, workspace_path: str) -> None:
    import base64

    w.workspace.import_(
        path=workspace_path,
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        # Databricks Workspace import expects base64-encoded content in JSON.
        content=base64.b64encode(local_path.read_bytes()).decode("utf-8"),
        overwrite=True,
    )


def _convert_cron_to_quartz(expr: str) -> str:
    parts = (expr or "").strip().split()
    if len(parts) != 5:
        return (expr or "").strip()
    minute, hour, dom, mon, dow = parts
    if dow == "*":
        dow = "?"
    else:
        dom = "?"
    return f"0 {minute} {hour} {dom} {mon} {dow}"


def _as_payload(obj: Any) -> dict:
    """Normalize SDK request objects to plain dict payloads."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    as_dict_attr = getattr(obj, 'as_dict', None)
    # Some SDK versions expose .as_dict() method
    if callable(as_dict_attr):
        out = as_dict_attr()
        if not isinstance(out, dict):
            raise TypeError(f'as_dict() must return dict, got: {type(out)}')
        return out
    # Some SDK versions expose .as_dict as a dict property
    if isinstance(as_dict_attr, dict):
        return as_dict_attr
    raise TypeError(f'Unsupported payload type: {type(obj)}')


@dataclass
class TableRow:
    source_table: str
    destination_table: str
    pipeline_group: str
    schedule: str
    table_options_json: str
    weight: int


def _read_csv_rows(csv_path: Path) -> List[TableRow]:
    rows: List[TableRow] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            st = (row.get("source_table") or "").strip()
            if not st:
                continue
            rows.append(
                TableRow(
                    source_table=st,
                    destination_table=(row.get("destination_table") or "").strip() or st,
                    pipeline_group=(row.get("pipeline_group") or row.get("category") or "").strip() or "default",
                    schedule=(row.get("schedule") or "").strip(),
                    table_options_json=(row.get("table_options_json") or "").strip() or "{}",
                    weight=int((row.get("weight") or "1").strip() or "1"),
                )
            )
    if not rows:
        raise ValueError(f"No rows found in CSV: {csv_path}")
    return rows


def _group_rows(rows: Iterable[TableRow]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        out.setdefault(r.pipeline_group, []).append(
            {
                "source_table": r.source_table,
                "destination_table": r.destination_table,
                "pipeline_group": r.pipeline_group,
                "schedule": r.schedule,
                "table_options_json": r.table_options_json,
                "weight": str(r.weight),
            }
        )
    return out


# COMMAND ----------

# DBTITLE 1,Derive paths (repo root + workspace destinations)
nb_path = _ctx_notebook_path()
user = _ctx_user()
print("notebookPath:", nb_path)
print("user:", user)

repo_root = _repo_root_fs_from_notebook_path(nb_path)
print("repo_root:", repo_root)

if WORKSPACE_NOTEBOOKS_DIR is None:
    if not user:
        raise ValueError("Unable to determine userName; set WORKSPACE_NOTEBOOKS_DIR explicitly.")
    WORKSPACE_NOTEBOOKS_DIR = f"/Workspace/Users/{user}/{CONNECTOR_NAME}_dlt_pipelines"

print("WORKSPACE_NOTEBOOKS_DIR:", WORKSPACE_NOTEBOOKS_DIR)

csv_path = repo_root / CSV_REL_PATH
connector_src_path = repo_root / CONNECTOR_GENERATED_SOURCE_REL_PATH
print("CSV:", csv_path)
print("Connector source:", connector_src_path)

connector_dir = repo_root / "sources" / CONNECTOR_NAME
if not connector_dir.is_dir():
    raise ValueError(
        f"CONNECTOR_NAME={CONNECTOR_NAME!r} does not match a directory under sources/. "
        "This is usually because CONNECTOR_NAME was set to a UC connection name. "
        "Set CONNECTOR_NAME to the connector id (e.g. 'osipi')."
    )

if not csv_path.exists():
    raise FileNotFoundError(csv_path)
if not connector_src_path.exists():
    raise FileNotFoundError(connector_src_path)

# COMMAND ----------

# DBTITLE 1,Upload connector generated source into workspace
# DBTITLE 1,Write connector generated source to DBFS (/dbfs/...)
# DBTITLE 1,Write connector generated source to UC Volume (preferred) or DBFS fallback
# DLT pipeline clusters can reliably read /Volumes/... (UC Volumes). DBFS is used only as a fallback.

w = WorkspaceClient()

# Optionally purge stale workspace notebooks/connectors dirs (workspace objects)
if PURGE_WORKSPACE_DIRS:
    try:
        print("Purging workspace notebooks dir:", WORKSPACE_NOTEBOOKS_DIR)
        w.workspace.delete(path=WORKSPACE_NOTEBOOKS_DIR, recursive=True)
    except Exception as e:
        print("(ignore) purge notebooks dir failed:", e)

# Compute a filesystem directory where DLT can open() the connector source.
# UC Volume path: /Volumes/<catalog>/<schema>/<volume>

vol = UC_VOLUME_NAME
if not vol:
    raise ValueError('UC_VOLUME_NAME is empty')
# Create volume if needed (requires UC privileges)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DEST_CATALOG}.{DEST_SCHEMA}.{vol}")
connector_fs_dir = f"/Volumes/{DEST_CATALOG}/{DEST_SCHEMA}/{vol}"
connector_write_uri_dir = 'dbfs:' + connector_fs_dir
print('Using UC Volume for connector source:', connector_fs_dir)

# Ensure destination directory exists
if connector_write_uri_dir is None:
    raise ValueError('Internal error: connector_write_uri_dir is None')

dbutils.fs.mkdirs(connector_write_uri_dir)

connector_fs_path = f"{connector_fs_dir.rstrip('/')}/{CONNECTOR_NAME}_generated_source.py"
content = connector_src_path.read_text(encoding='utf-8')

# dbutils.fs.put expects dbfs:/ URIs
write_uri = 'dbfs:' + connector_fs_path

dbutils.fs.put(write_uri, content, overwrite=True)
print('Wrote connector source to:', connector_fs_path)
# COMMAND ----------

# DBTITLE 1,Generate DLT notebooks locally (driver) using the generator in this repo
gen_path = repo_root / "tools/notebook_based_deployment/generate_dlt_notebooks_generic.py"
spec = importlib.util.spec_from_file_location("_nbgen", str(gen_path))
if spec is None or spec.loader is None:
    raise ValueError(f"Unable to load notebook generator module: {gen_path}")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore[attr-defined]

# connector_config comes from the CONNECTOR_CONFIG_JSON widget (parsed at the top).

rows = _read_csv_rows(csv_path)
groups = _group_rows(rows)
print(f"Groups: {len(groups)}")
for g, items in sorted(groups.items()):
    print(f"  - {g}: {len(items)} tables")

tmp_dir = Path(tempfile.mkdtemp(prefix=f"{CONNECTOR_NAME}_dlt_notebooks_"))
print("temp notebook dir:", tmp_dir)

for group, table_dicts in sorted(groups.items()):
    out_file = tmp_dir / f"{CONNECTOR_NAME}_{group}.py"
    mod.generate_dlt_notebook(  # type: ignore[attr-defined]
        CONNECTOR_NAME,
        connector_config,
        table_dicts,
        out_file,
        connector_fs_dir,
        pipeline_group=group,
    )

print("Generated notebook files:", len(list(tmp_dir.glob("*.py"))))

# COMMAND ----------

# DBTITLE 1,Upload DLT notebooks to workspace
_mkdirs(w, WORKSPACE_NOTEBOOKS_DIR)

uploaded: List[Tuple[str, str]] = []
for f in sorted(tmp_dir.glob("*.py")):
    base = f.stem  # <connector>_<group>
    ws_path = f"{WORKSPACE_NOTEBOOKS_DIR}/{base}"
    _import_py_source(w, local_path=f, workspace_path=ws_path)
    uploaded.append((base, ws_path))

print("Uploaded notebooks:")
for _, ws_path in uploaded:
    print("  -", ws_path)

# COMMAND ----------

# DBTITLE 1,Create / update DLT pipelines (one per group)
def _pipeline_name(connector: str, group: str) -> str:
    return f"{connector.upper()} - {group.replace('_', ' ').title()}"


existing = {p.name: p for p in w.pipelines.list_pipelines()}  # type: ignore[assignment]
created_pipeline_ids: Dict[str, str] = {}

for group in sorted(groups.keys()):
    name = _pipeline_name(CONNECTOR_NAME, group)
    notebook_path = f"{WORKSPACE_NOTEBOOKS_DIR}/{CONNECTOR_NAME}_{group}"

    if name in existing and RECREATE_EXISTING:
        pid = existing[name].pipeline_id
        print("Deleting existing pipeline:", name, pid)
        w.pipelines.delete(pipeline_id=pid)
        existing.pop(name, None)

    if name in existing:
        pid = existing[name].pipeline_id
        print("Updating existing pipeline:", name, pid)
        w.pipelines.update(
            pipeline_id=pid,
            name=name,
            catalog=DEST_CATALOG,
            target=DEST_SCHEMA,
            development=DEVELOPMENT,
            continuous=CONTINUOUS,
            libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))],
            clusters=[pipelines.PipelineCluster(label="default", num_workers=DLT_NUM_WORKERS)],
        )
        created_pipeline_ids[group] = pid
    else:
        print("Creating pipeline:", name)
        created = w.pipelines.create(
            name=name,
            catalog=DEST_CATALOG,
            target=DEST_SCHEMA,
            development=DEVELOPMENT,
            continuous=CONTINUOUS,
            libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))],
            clusters=[pipelines.PipelineCluster(label="default", num_workers=DLT_NUM_WORKERS)],
        )
        created_pipeline_ids[group] = created.pipeline_id

print("Pipelines:")
for g, pid in created_pipeline_ids.items():
    print("  -", g, pid)

# COMMAND ----------

# DBTITLE 1,Create / update scheduled Jobs (optional)
if not CREATE_SCHEDULED_JOBS:
    print("CREATE_SCHEDULED_JOBS is False; skipping job creation.")
else:
    # Allow rerunning just this cell: define helper if earlier cells were not rerun.
    if '_as_payload' not in globals():
        def _as_payload(obj: Any) -> dict:
            """Normalize SDK request objects to plain dict payloads."""
            if obj is None:
                return {}
            if isinstance(obj, dict):
                return obj
            as_dict_attr = getattr(obj, 'as_dict', None)
            # Some SDK versions expose .as_dict() method
            if callable(as_dict_attr):
                out = as_dict_attr()
                if not isinstance(out, dict):
                    raise TypeError(f'as_dict() must return dict, got: {type(out)}')
                return out
            # Some SDK versions expose .as_dict as a dict property
            if isinstance(as_dict_attr, dict):
                return as_dict_attr
            raise TypeError(f'Unsupported payload type: {type(obj)}')

    schedules: Dict[str, str] = {}
    for r in rows:
        if r.schedule and r.pipeline_group not in schedules:
            schedules[r.pipeline_group] = r.schedule

    print("Groups with schedules:", len(schedules))

    existing_jobs = {j.settings.name: j for j in w.jobs.list(name=None)}  # type: ignore[assignment]
    created_jobs: Dict[str, int] = {}

    for group, cron in sorted(schedules.items()):
        if group not in created_pipeline_ids:
            continue

        job_name = f"{CONNECTOR_NAME.upper()} Scheduler - {group.replace('_', ' ').title()}"
        quartz = _convert_cron_to_quartz(cron)
        pipeline_id = created_pipeline_ids[group]

        if job_name in existing_jobs and RECREATE_EXISTING:
            jid = existing_jobs[job_name].job_id
            print("Deleting existing job:", job_name, jid)
            w.jobs.delete(job_id=jid)
            existing_jobs.pop(job_name, None)

        job_settings = jobs.JobSettings(
            name=job_name,
            schedule=jobs.CronSchedule(
                quartz_cron_expression=quartz,
                timezone_id="UTC",
                pause_status=jobs.PauseStatus.PAUSED,
            ),
            tasks=[
                jobs.Task(
                    task_key=f"run_{CONNECTOR_NAME}_{group}",
                    pipeline_task=jobs.PipelineTask(pipeline_id=pipeline_id),
                )
            ],
        )

        if job_name in existing_jobs:
            jid = existing_jobs[job_name].job_id
            print("Resetting existing job:", job_name, jid)
            # JobsAPI.reset expects a JobSettings object (not a dict)
            w.jobs.reset(job_id=jid, new_settings=job_settings)
            created_jobs[group] = jid
        else:
            print("Creating job:", job_name)
            # JobsAPI.create expects keyword args with typed SDK objects (CronSchedule/Task/etc.)
            out = w.jobs.create(
                name=job_settings.name,
                schedule=job_settings.schedule,
                tasks=job_settings.tasks,
            )
            created_jobs[group] = out.job_id

    print("Jobs:")
    for g, jid in created_jobs.items():
        print("  -", g, jid)

# COMMAND ----------

# DBTITLE 1,Done
print("Done. Pipelines and jobs are now created/updated.")
