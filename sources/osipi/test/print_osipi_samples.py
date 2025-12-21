"""Print sample rows for OSIPI connector tables.

This is a validation and troubleshooting utility. It uses the connector directly
(no Spark required) and prints N records per table in a readable format.

Typical usage (PI Web API endpoint):
  python3 sources/osipi/test/print_osipi_samples.py \
    --pi-base-url https://<pi-web-api-host> \
    --access-token <BEARER_TOKEN> \
    --verbose \
    --probe \
    --max-records 5

Typical usage (development endpoint):
  python3 sources/osipi/test/print_osipi_samples.py \
    --pi-base-url http://127.0.0.1:8000 \
    --max-records 5 \
    --verbose

Notes:
- No secrets are printed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional

import requests

# Ensure repo root is importable when running as a script (so `import sources...` works).
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from sources.osipi.osipi import LakeflowConnect


def _log(enabled: bool, msg: str) -> None:
    if enabled:
        print(f"[osipi-samples] {msg}")


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "<empty>"
    if len(token) <= 16:
        return "<redacted>"
    return token[:10] + "…" + token[-6:]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _iter_n(it: Iterable[dict], n: int) -> List[dict]:
    out: List[dict] = []
    for i, row in enumerate(it):
        out.append(row)
        if i >= n - 1:
            break
    return out


def _print_table_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _print_records(records: List[dict]) -> None:
    if not records:
        print("(no records)")
        return
    for i, r in enumerate(records, start=1):
        print(f"- record {i}:")
        print(json.dumps(r, indent=2, default=str))


def _probe(base_url: str, access_token: Optional[str], verbose: bool) -> None:
    url = base_url.rstrip("/") + "/piwebapi/dataservers"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    _log(verbose, f"Probe GET {url}")
    t0 = perf_counter()
    r = requests.get(url, headers=headers, timeout=60)
    dt = perf_counter() - t0
    _log(verbose, f"Probe status={r.status_code} ct={r.headers.get('content-type','')} dt={dt:.2f}s")
    # Don't raise here; just a probe.


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pi-base-url", dest="pi_base_url", default=None)
    parser.add_argument("--access-token", dest="access_token", default=None)
    parser.add_argument("--max-records", dest="max_records", type=int, default=5)
    parser.add_argument(
        "--tables",
        dest="tables",
        default=None,
        help="Comma-separated table names (default: all)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print step-by-step telemetry logs")
    parser.add_argument("--probe", action="store_true", help="Probe connectivity before reading")

    args = parser.parse_args()

    parent_dir = Path(__file__).parent.parent
    dev_cfg_path = parent_dir / "configs" / "dev_config.json"
    table_cfg_path = parent_dir / "configs" / "dev_table_config.json"

    dev_cfg: Dict[str, str] = {}
    try:
        dev_cfg = dict(_load_json(dev_cfg_path))
    except FileNotFoundError:
        dev_cfg = {}

    try:
        table_cfg: Dict[str, Dict[str, Any]] = dict(_load_json(table_cfg_path))
    except FileNotFoundError:
        table_cfg = {}

    _log(args.verbose, f"Loaded dev_config.json: {dev_cfg_path}" if dev_cfg else "dev_config.json not found; using CLI arguments only")
    _log(args.verbose, f"Loaded dev_table_config.json: {table_cfg_path}")

    if args.pi_base_url:
        dev_cfg["pi_base_url"] = args.pi_base_url
    if args.access_token is not None:
        dev_cfg["access_token"] = args.access_token

    base_url = (dev_cfg.get("pi_base_url") or "").strip()
    if not base_url:
        raise SystemExit(
            "Missing pi_base_url. Pass --pi-base-url or set it in sources/osipi/configs/dev_config.json"
        )

    access_token = (dev_cfg.get("access_token") or "").strip() or None
    _log(args.verbose, f"Using pi_base_url={base_url}")
    _log(args.verbose, f"Using access_token={_mask_token(access_token)}")

    if args.probe:
        _probe(base_url, access_token, args.verbose)

    _log(args.verbose, "Initializing LakeflowConnect...")
    t0 = perf_counter()
    conn = LakeflowConnect({"pi_base_url": base_url, "access_token": access_token or ""})
    _log(args.verbose, f"Initialized LakeflowConnect dt={perf_counter()-t0:.2f}s")

    tables = conn.list_tables()
    selected = tables
    if args.tables:
        want = {t.strip() for t in args.tables.split(",") if t.strip()}
        selected = [t for t in tables if t in want]

    _print_table_header("OSIPI connector sample output")
    print(f"pi_base_url = {base_url}")
    print(f"tables = {selected}")
    print(f"max_records_per_table = {args.max_records}")

    for t in selected:
        opts = table_cfg.get(t, {})
        _print_table_header(f"table: {t}")
        print(f"table_options = {json.dumps(opts, indent=2, default=str)}")

        try:
            _log(args.verbose, f"Reading table={t} ...")
            t0 = perf_counter()
            it, off = conn.read_table(t, {}, opts)
            rows = _iter_n(it, args.max_records)
            dt = perf_counter() - t0
            _log(
                args.verbose,
                f"Read table={t} ok: sampled={len(rows)} next_offset={off} dt={dt:.2f}s",
            )
            print(f"next_offset = {off}")
            _print_records(rows)
        except Exception as e:
            _log(True, f"ERROR reading table {t}: {type(e).__name__}: {e}")
            if args.verbose:
                print(traceback.format_exc())
            time.sleep(0.5)


if __name__ == "__main__":
    main()
