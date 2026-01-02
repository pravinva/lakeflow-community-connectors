# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy DLT pipelines from connector preset (UC Volumes only)
# MAGIC
# MAGIC This notebook is connector-agnostic. It discovers `deploy_preset.json` files under:
# MAGIC
# MAGIC - `sources/*/deployment/deploy_preset.json`
# MAGIC
# MAGIC It supports 3 CSV modes:
# MAGIC - **static**: use a CSV checked into the repo
# MAGIC - **discover**: auto-generate a CSV by running `discover_and_classify_tables.py`
# MAGIC - **manual**: supply a comma-separated table list (no CSV file needed)
# MAGIC
# MAGIC **Storage policy**: connector source is written to a **Unity Catalog Volume** and loaded from `/Volumes/...`.

# COMMAND ----------

import base64
import csv
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, workspace

# Keep Repos clean
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

# COMMAND ----------

# DBTITLE 1,Context helpers

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


def _find_project_root(nb_path: str) -> Path:
    nb_path = (nb_path or "").strip()
    if not nb_path.startswith("/"):
        raise ValueError(f"Unexpected notebookPath: {nb_path!r}")
    fs_path = Path("/Workspace") / nb_path.lstrip("/")
    for p in [fs_path] + list(fs_path.parents):
        if (p / "sources").is_dir():
            return p
    raise ValueError(f"Unable to infer project root from notebookPath={nb_path!r}")


def _read_json(path: Path) -> dict:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"{path} must be a JSON object")
    return obj


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


# COMMAND ----------

# DBTITLE 1,Discover presets
nb_path = _ctx_notebook_path()
user = _ctx_user()
print("notebookPath:", nb_path)
print("user:", user)

project_root = _find_project_root(nb_path)
print("project_root:", project_root)

preset_paths = sorted(project_root.glob("sources/*/deployment/deploy_preset.json"))
if not preset_paths:
    raise ValueError("No presets found. Expected sources/<connector>/deployment/deploy_preset.json")

presets: Dict[str, Dict[str, Any]] = {}
for p in preset_paths:
    d = _read_json(p)
    name = (d.get("connector_name") or p.parts[p.parts.index("sources") + 1]).strip()
    d["_preset_path"] = str(p)
    presets[name] = d

connector_names = sorted(presets.keys())
print("Found presets:", connector_names)

# COMMAND ----------

# DBTITLE 1,Widgets
try:
    dbutils.widgets.dropdown("CONNECTOR_NAME", connector_names[0], connector_names, "Connector")
    dbutils.widgets.dropdown("CSV_MODE_OVERRIDE", "", ["", "static", "discover", "manual"], "CSV mode override")

    dbutils.widgets.text("DEST_CATALOG", "", "Destination catalog (override)")
    dbutils.widgets.text("DEST_SCHEMA", "", "Destination schema (override)")

    dbutils.widgets.text("WORKSPACE_NOTEBOOKS_DIR", "", "Workspace dir for generated notebooks (override)")
    dbutils.widgets.dropdown("PURGE_WORKSPACE_NOTEBOOKS_DIR", "false", ["true", "false"], "Purge workspace notebooks dir")
    dbutils.widgets.dropdown("RECREATE_EXISTING", "false", ["true", "false"], "Delete/recreate existing pipelines/jobs")

    dbutils.widgets.text("UC_VOLUME_NAME", "", "UC Volume name (override)")

    dbutils.widgets.text("MANUAL_TABLES", "", "Manual tables (comma-separated) when csv.mode=manual")
    dbutils.widgets.text("DISCOVER_INIT_OPTIONS_JSON", "", "Discover init options JSON (override)")
except Exception:
    pass

selected = _w("CONNECTOR_NAME", connector_names[0]).strip()
preset = presets[selected]

csv_mode_override = _w("CSV_MODE_OVERRIDE", "").strip()
dest_catalog_override = _w("DEST_CATALOG", "").strip()
dest_schema_override = _w("DEST_SCHEMA", "").strip()
ws_nb_dir_override = _w("WORKSPACE_NOTEBOOKS_DIR", "").strip()

purge_ws_nb_dir = _w_bool("PURGE_WORKSPACE_NOTEBOOKS_DIR", False)
recreate_existing = _w_bool("RECREATE_EXISTING", False)

uc_volume_name_override = _w("UC_VOLUME_NAME", "").strip()
manual_tables_raw = _w("MANUAL_TABLES", "").strip()
discover_init_override = _w("DISCOVER_INIT_OPTIONS_JSON", "").strip()

# COMMAND ----------

# DBTITLE 1,Resolve config
connector_name = str(preset.get("connector_name") or selected).strip()
if not connector_name:
    raise ValueError("preset.connector_name is empty")

generated_source_rel = str(preset.get("generated_source_rel_path") or f"sources/{connector_name}/_generated_{connector_name}_python_source.py").strip()
connector_src_path = project_root / generated_source_rel
if not connector_src_path.exists():
    raise FileNotFoundError(f"Generated connector source not found: {connector_src_path}")

connector_config = preset.get("connector_config")
if not isinstance(connector_config, dict):
    raise ValueError("preset.connector_config must be a JSON object")

csv_cfg = preset.get("csv") or {}
if not isinstance(csv_cfg, dict):
    raise ValueError("preset.csv must be a JSON object")

csv_mode = str(csv_cfg.get("mode") or "static").strip().lower()
if csv_mode_override:
    csv_mode = csv_mode_override
if csv_mode not in ("static", "discover", "manual"):
    raise ValueError(f"Unsupported csv.mode: {csv_mode}")

pipeline_cfg = preset.get("pipeline") or {}
if not isinstance(pipeline_cfg, dict):
    raise ValueError("preset.pipeline must be a JSON object")

development = bool(pipeline_cfg.get("development", False))
continuous = bool(pipeline_cfg.get("continuous", False))
num_workers = int(pipeline_cfg.get("num_workers", 1))

jobs_cfg = preset.get("jobs") or {}
if not isinstance(jobs_cfg, dict):
    raise ValueError("preset.jobs must be a JSON object")
create_jobs = bool(jobs_cfg.get("create", True))
pause_jobs = bool(jobs_cfg.get("pause", True))
timezone_id = str(jobs_cfg.get("timezone_id") or "UTC")

storage_cfg = preset.get("storage") or {}
if not isinstance(storage_cfg, dict):
    raise ValueError("preset.storage must be a JSON object")
uc_volume_name = str(storage_cfg.get("uc_volume_name") or "lakeflow_connectors").strip()
if uc_volume_name_override:
    uc_volume_name = uc_volume_name_override

# Destination catalog/schema must be provided via preset or override
# (We keep it explicit to avoid accidental writes to 'main')
dest_catalog = dest_catalog_override or str(preset.get("dest_catalog") or "").strip()
dest_schema = dest_schema_override or str(preset.get("dest_schema") or "").strip()
if not dest_catalog or not dest_schema:
    raise ValueError("DEST_CATALOG/DEST_SCHEMA must be provided via preset or widget override")

if ws_nb_dir_override:
    workspace_notebooks_dir = ws_nb_dir_override
else:
    if not user:
        raise ValueError("Unable to determine userName; set WORKSPACE_NOTEBOOKS_DIR explicitly")
    workspace_notebooks_dir = f"/Workspace/Users/{user}/{connector_name}_dlt_pipelines"

print("connector:", connector_name)
print("csv_mode:", csv_mode)
print("dest:", f"{dest_catalog}.{dest_schema}")
print("workspace_notebooks_dir:", workspace_notebooks_dir)

# COMMAND ----------

# DBTITLE 1,Build CSV (static/discover/manual)
tmp_dir = Path(tempfile.mkdtemp(prefix=f"{connector_name}_deploy_"))
csv_out_path = tmp_dir / f"{connector_name}.csv"

if csv_mode == "static":
    rel = str(csv_cfg.get("path") or "").strip()
    if not rel:
        raise ValueError("csv.mode=static requires preset.csv.path")
    src = project_root / rel
    if not src.exists():
        raise FileNotFoundError(f"Static CSV not found: {src}")
    csv_out_path.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    print("Using static CSV:", src)

elif csv_mode == "manual":
    if not manual_tables_raw:
        tables = csv_cfg.get("tables") if isinstance(csv_cfg.get("tables"), list) else []
        manual_tables_raw = ",".join([str(x) for x in tables])
    tables = [t.strip() for t in manual_tables_raw.split(",") if t.strip()]
    if not tables:
        raise ValueError("csv.mode=manual requires MANUAL_TABLES widget or preset.csv.tables")
    with csv_out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "source_table",
                "destination_table",
                "pipeline_group",
                "schedule",
                "table_options_json",
                "weight",
            ],
        )
        w.writeheader()
        for t in tables:
            w.writerow(
                {
                    "source_table": t,
                    "destination_table": t,
                    "pipeline_group": "all",
                    "schedule": "",
                    "table_options_json": "{}",
                    "weight": "1",
                }
            )
    print("Generated manual CSV:", csv_out_path)

elif csv_mode == "discover":
    discover_cfg = preset.get("discover") or {}
    if not isinstance(discover_cfg, dict) or not bool(discover_cfg.get("enabled", True)):
        raise ValueError("csv.mode=discover but preset.discover.enabled is false/missing")

    discover_script = project_root / "tools/notebook_based_deployment/discover_and_classify_tables.py"
    if not discover_script.exists():
        raise FileNotFoundError(f"Missing discover script: {discover_script}")

    init_opts = discover_init_override or str(discover_cfg.get("init_options_json") or "").strip()
    group_by = str(discover_cfg.get("group_by") or "ingestion_type")
    best_effort = bool(discover_cfg.get("best_effort", False))

    args = [
        sys.executable,
        str(discover_script),
        "--connector-name",
        connector_name,
        "--output-csv",
        str(csv_out_path),
        "--connection-name",
        "unused_in_notebook_flow",
        "--dest-catalog",
        dest_catalog,
        "--dest-schema",
        dest_schema,
        "--group-by",
        group_by,
    ]
    if init_opts:
        args += ["--init-options-json", init_opts]
    if best_effort:
        args += ["--best-effort"]

    # schedules
    for k in ("schedule_snapshot", "schedule_append", "schedule_unknown"):
        v = str(discover_cfg.get(k) or "").strip()
        if v:
            args += ["--" + k.replace("_", "-"), v]

    # optional category prefix map
    prefix_map_rel = str(discover_cfg.get("category_prefix_map_rel_path") or "").strip()
    if prefix_map_rel:
        pm = project_root / prefix_map_rel
        if pm.exists():
            args += ["--category-prefix-map-json", pm.read_text(encoding="utf-8")]

    print("Running discovery...")
    subprocess.check_call(args)
    print("Generated discovered CSV:", csv_out_path)

# COMMAND ----------

# DBTITLE 1,Write connector source to UC Volume (required)
# Create volume if needed
spark.sql(f"CREATE VOLUME IF NOT EXISTS {dest_catalog}.{dest_schema}.{uc_volume_name}")
connector_fs_dir = f"/Volumes/{dest_catalog}/{dest_schema}/{uc_volume_name}"

# Ensure directory exists (dbutils.fs uses dbfs:/ URIs)
dbutils.fs.mkdirs("dbfs:" + connector_fs_dir)

connector_source_path_for_open = f"{connector_fs_dir}/{connector_name}_generated_source.py"
write_uri = "dbfs:" + connector_source_path_for_open

dbutils.fs.put(write_uri, connector_src_path.read_text(encoding="utf-8"), overwrite=True)
print("Wrote connector source to:", connector_source_path_for_open)

# COMMAND ----------

# DBTITLE 1,Generate DLT notebooks and upload
w = WorkspaceClient()

if purge_ws_nb_dir:
    try:
        print("Purging workspace notebooks dir:", workspace_notebooks_dir)
        w.workspace.delete(path=workspace_notebooks_dir, recursive=True)
    except Exception as e:
        print("(ignore) purge failed:", e)

# Load generator
gen_path = project_root / "tools/notebook_based_deployment/generate_dlt_notebooks_generic.py"
spec = importlib.util.spec_from_file_location("_nbgen", str(gen_path))
if spec is None or spec.loader is None:
    raise ValueError(f"Unable to load generator module: {gen_path}")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore[attr-defined]

# Group rows by pipeline_group
by_group: Dict[str, List[Dict[str, str]]] = defaultdict(list)
with csv_out_path.open("r", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        st = (r.get("source_table") or "").strip()
        if not st:
            continue
        grp = (r.get("pipeline_group") or r.get("category") or "").strip() or "default"
        by_group[grp].append(r)

if not by_group:
    raise ValueError("No tables found after CSV processing")

# Upload notebooks
nb_tmp_dir = Path(tempfile.mkdtemp(prefix=f"{connector_name}_dlt_nbs_"))
uploaded_notebooks: Dict[str, str] = {}

for grp, rows in sorted(by_group.items()):
    out_file = nb_tmp_dir / f"{connector_name}_{grp}.py"
    mod.generate_dlt_notebook(  # type: ignore[attr-defined]
        connector_name,
        connector_config,
        rows,
        out_file,
        connector_fs_dir,
        pipeline_group=grp,
    )

    ws_path = f"{workspace_notebooks_dir}/{connector_name}_{grp}"
    w.workspace.import_(
        path=ws_path,
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=base64.b64encode(out_file.read_bytes()).decode("utf-8"),
        overwrite=True,
    )
    uploaded_notebooks[grp] = ws_path

print("Uploaded notebooks:", len(uploaded_notebooks))

# COMMAND ----------

# DBTITLE 1,Create / update pipelines

def _pipeline_name(connector: str, group: str) -> str:
    return f"{connector.upper()} - {group.replace('_', ' ').title()}"

existing = {p.name: p for p in w.pipelines.list_pipelines()}  # type: ignore[assignment]
created_pipeline_ids: Dict[str, str] = {}

for grp, nb_ws_path in sorted(uploaded_notebooks.items()):
    name = _pipeline_name(connector_name, grp)

    if name in existing and recreate_existing:
        pid = existing[name].pipeline_id
        print("Deleting pipeline:", name, pid)
        w.pipelines.delete(pipeline_id=pid)
        existing.pop(name, None)

    if name in existing:
        pid = existing[name].pipeline_id
        print("Updating pipeline:", name, pid)
        w.pipelines.update(
            pipeline_id=pid,
            name=name,
            catalog=dest_catalog,
            target=dest_schema,
            development=development,
            continuous=continuous,
            libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=nb_ws_path))],
            clusters=[pipelines.PipelineCluster(label="default", num_workers=num_workers)],
        )
        created_pipeline_ids[grp] = pid
    else:
        print("Creating pipeline:", name)
        created = w.pipelines.create(
            name=name,
            catalog=dest_catalog,
            target=dest_schema,
            development=development,
            continuous=continuous,
            libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=nb_ws_path))],
            clusters=[pipelines.PipelineCluster(label="default", num_workers=num_workers)],
        )
        created_pipeline_ids[grp] = created.pipeline_id

print("Pipelines created/updated:", len(created_pipeline_ids))

# COMMAND ----------

# DBTITLE 1,Create / update jobs (optional)
if not create_jobs:
    print("preset.jobs.create is false; skipping jobs")
else:
    # schedules: first non-empty per group from CSV
    schedules: Dict[str, str] = {}
    with csv_out_path.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            grp = (r.get("pipeline_group") or r.get("category") or "").strip() or "default"
            sch = (r.get("schedule") or "").strip()
            if sch and grp not in schedules:
                schedules[grp] = sch

    existing_jobs = {j.settings.name: j for j in w.jobs.list(name=None)}  # type: ignore[assignment]
    created_jobs: Dict[str, int] = {}

    for grp, cron in sorted(schedules.items()):
        if grp not in created_pipeline_ids:
            continue

        pipeline_id = created_pipeline_ids[grp]
        job_name = f"{connector_name.upper()} Scheduler - {grp.replace('_', ' ').title()}"

        if job_name in existing_jobs and recreate_existing:
            jid = existing_jobs[job_name].job_id
            print("Deleting job:", job_name, jid)
            w.jobs.delete(job_id=jid)
            existing_jobs.pop(job_name, None)

        sched = jobs.CronSchedule(
            quartz_cron_expression=_convert_cron_to_quartz(cron),
            timezone_id=timezone_id,
            pause_status=jobs.PauseStatus.PAUSED if pause_jobs else jobs.PauseStatus.UNPAUSED,
        )
        task = jobs.Task(
            task_key=f"run_{connector_name}_{grp}",
            pipeline_task=jobs.PipelineTask(pipeline_id=pipeline_id),
        )

        if job_name in existing_jobs:
            jid = existing_jobs[job_name].job_id
            print("Resetting job:", job_name, jid)
            w.jobs.reset(job_id=jid, new_settings=jobs.JobSettings(name=job_name, schedule=sched, tasks=[task]))
            created_jobs[grp] = jid
        else:
            print("Creating job:", job_name)
            out = w.jobs.create(name=job_name, schedule=sched, tasks=[task])
            created_jobs[grp] = out.job_id

    print("Jobs created/updated:", len(created_jobs))

# COMMAND ----------

print("Done")
