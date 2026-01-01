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

# DBTITLE 1,Configuration (edit these)
CONNECTOR_NAME = "osipi"

# CSV inside this repo (relative to repo root)
CSV_REL_PATH = "tools/notebook_based_deployment/examples/osipi/osipi_by_category_and_ingestion_type.csv"

# Destination tables location
DEST_CATALOG = "osipi"
DEST_SCHEMA = "bronzeosipi"

# Where to put uploaded artifacts in your workspace (defaults to your user home)
WORKSPACE_CONNECTORS_DIR = None  # e.g. "/Workspace/Users/you@databricks.com/connectors"
WORKSPACE_NOTEBOOKS_DIR = None  # e.g. "/Workspace/Users/you@databricks.com/osipi_dlt_pipelines"

# Connector source file inside this repo (relative to repo root)
CONNECTOR_GENERATED_SOURCE_REL_PATH = f"sources/{CONNECTOR_NAME}/_generated_{CONNECTOR_NAME}_python_source.py"

# Pipeline creation behavior
DEVELOPMENT = False
CONTINUOUS = False

# If True: create/update scheduled jobs when CSV has a `schedule` value.
CREATE_SCHEDULED_JOBS = True

# If True: delete & recreate pipelines/jobs when name matches (most reliable).
RECREATE_EXISTING = False

# Cluster settings for DLT classic clusters (DLT serverless may not be available in all workspaces via API)
DLT_NUM_WORKERS = 1

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
    """Infer /Workspace/Repos/<user>/<repo> from /Repos/<user>/<repo>/..."""
    m = re.match(r"^/Repos/([^/]+)/([^/]+)(/.*)?$", nb_path or "")
    if not m:
        raise ValueError(
            "Unable to infer repo root from notebookPath. "
            "Run this notebook from within a Databricks Repo (under /Repos/...). "
            f"notebookPath={nb_path!r}"
        )
    user, repo = m.group(1), m.group(2)
    root = Path("/Workspace/Repos") / user / repo
    if not (root / "sources").is_dir():
        raise ValueError(f"Inferred repo root does not contain sources/: {root}")
    return root


def _mkdirs(w: WorkspaceClient, path: str) -> None:
    w.workspace.mkdirs(path)


def _import_py_source(w: WorkspaceClient, *, local_path: Path, workspace_path: str) -> None:
    w.workspace.import_(
        path=workspace_path,
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=local_path.read_bytes(),
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

if WORKSPACE_CONNECTORS_DIR is None:
    if not user:
        raise ValueError("Unable to determine userName; set WORKSPACE_CONNECTORS_DIR explicitly.")
    WORKSPACE_CONNECTORS_DIR = f"/Workspace/Users/{user}/connectors"

if WORKSPACE_NOTEBOOKS_DIR is None:
    if not user:
        raise ValueError("Unable to determine userName; set WORKSPACE_NOTEBOOKS_DIR explicitly.")
    WORKSPACE_NOTEBOOKS_DIR = f"/Workspace/Users/{user}/{CONNECTOR_NAME}_dlt_pipelines"

print("WORKSPACE_CONNECTORS_DIR:", WORKSPACE_CONNECTORS_DIR)
print("WORKSPACE_NOTEBOOKS_DIR:", WORKSPACE_NOTEBOOKS_DIR)

csv_path = repo_root / CSV_REL_PATH
connector_src_path = repo_root / CONNECTOR_GENERATED_SOURCE_REL_PATH
print("CSV:", csv_path)
print("Connector source:", connector_src_path)

if not csv_path.exists():
    raise FileNotFoundError(csv_path)
if not connector_src_path.exists():
    raise FileNotFoundError(connector_src_path)

# COMMAND ----------

# DBTITLE 1,Upload connector generated source into workspace
w = WorkspaceClient()

_mkdirs(w, WORKSPACE_CONNECTORS_DIR)

connector_ws_path = f"{WORKSPACE_CONNECTORS_DIR}/{CONNECTOR_NAME}_generated_source.py"
_import_py_source(w, local_path=connector_src_path, workspace_path=connector_ws_path)
print("Uploaded connector source to:", connector_ws_path)

# COMMAND ----------

# DBTITLE 1,Generate DLT notebooks locally (driver) using the generator in this repo
gen_path = repo_root / "tools/notebook_based_deployment/generate_dlt_notebooks_generic.py"
spec = importlib.util.spec_from_file_location("_nbgen", str(gen_path))
if spec is None or spec.loader is None:
    raise ValueError(f"Unable to load notebook generator module: {gen_path}")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore[attr-defined]

# OSIPI mock bearer-token config. Edit if you want OAuth instead.
connector_config: Dict[str, Any] = {
    "secrets_scope": "sp-osipi",
    "secret_mappings": {"bearer_value_tmp": ["mock-bearer-token"]},
    "static_options": {
        "pi_base_url": "https://mock-piwebapi-912141448724.us-central1.run.app",
        "verify_ssl": "true",
    },
    "dynamic_options": {},
}

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
        WORKSPACE_CONNECTORS_DIR,
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
            w.jobs.reset(job_id=jid, new_settings=job_settings)
            created_jobs[group] = jid
        else:
            print("Creating job:", job_name)
            out = w.jobs.create(**job_settings.as_dict())  # type: ignore[arg-type]
            created_jobs[group] = out.job_id

    print("Jobs:")
    for g, jid in created_jobs.items():
        print("  -", g, jid)

# COMMAND ----------

# DBTITLE 1,Done
print("Done. Pipelines and jobs are now created/updated.")
