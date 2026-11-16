#!/usr/bin/env python3
"""Minimal Databricks SQL runner — stdlib only.

Executes .sql files against a workspace via the SQL Statement Execution API.
Each file is split into statements on top-level semicolons (quote-aware).

Env (from .env or environment):
    DATABRICKS_HOST   e.g. https://<workspace>.cloud.databricks.com
    DATABRICKS_TOKEN  PAT

Usage:
    python tools/run_sql.py poc/00_setup.sql poc/01_generate.sql ...
    python tools/run_sql.py --keep-going poc/
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent


def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def api(host, token, method, path, body=None):
    req = urllib.request.Request(
        host.rstrip("/") + path,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(body).encode() if body is not None else None,
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:800]
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {detail}") from e


def split_statements(sql: str):
    """Split on top-level ';', respecting '--' line comments, /* */ block
    comments, and single-quoted strings (with '' escapes)."""
    stmts, buf = [], []
    i, n = 0, len(sql)
    in_str = in_line = in_block = False
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if in_line:
            buf.append(ch)
            if ch == "\n":
                in_line = False
        elif in_block:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 1
                in_block = False
        elif in_str:
            buf.append(ch)
            if ch == "'":
                if nxt == "'":
                    buf.append(nxt)
                    i += 1
                else:
                    in_str = False
        elif ch == "-" and nxt == "-":
            in_line = True
            buf.append(ch)
        elif ch == "/" and nxt == "*":
            in_block = True
            buf.append(ch)
        elif ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == ";":
            stmt = "".join(buf).strip()
            if has_code(stmt):
                stmts.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if has_code(tail):
        stmts.append(tail)
    return stmts


def has_code(stmt: str) -> bool:
    """True if the statement contains anything besides comments/whitespace."""
    s = re.sub(r"/\*.*?\*/", "", stmt, flags=re.S)
    s = re.sub(r"--[^\n]*", "", s)
    return bool(s.strip())


def get_warehouse(host, token):
    whs = api(host, token, "GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not whs:
        raise RuntimeError("No SQL warehouses found in this workspace.")
    wh = whs[0]
    if wh.get("state") == "STOPPED":
        print(f"  starting warehouse '{wh['name']}' ...")
        api(host, token, "POST", f"/api/2.0/sql/warehouses/{wh['id']}/start")
    for _ in range(60):
        wh = api(host, token, "GET", f"/api/2.0/sql/warehouses/{wh['id']}")
        if wh.get("state") == "RUNNING":
            return wh["id"]
        time.sleep(5)
    raise RuntimeError("Warehouse did not reach RUNNING state in time.")


def run_statement(host, token, wh_id, sql):
    res = api(host, token, "POST", "/api/2.0/sql/statements", {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "row_limit": 100,
        "format": "JSON_ARRAY",
    })
    sid = res["statement_id"]
    while res.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(3)
        res = api(host, token, "GET", f"/api/2.0/sql/statements/{sid}")
    state = res.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = res.get("status", {}).get("error", {})
        raise RuntimeError(f"statement failed [{state}]: {err.get('message', res)}")
    result = res.get("result", {})
    return result.get("data_array", [])


def main():
    load_env()
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        sys.exit("Missing DATABRICKS_HOST / DATABRICKS_TOKEN (check .env)")
    # normalize: keep only scheme://netloc — browser URLs carry ?o=<org>
    # and/or paths that break API calls
    p = urlparse(host if "://" in host else f"https://{host}")
    host = f"{p.scheme}://{p.netloc}"

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    keep_going = "--keep-going" in sys.argv
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += sorted(p.glob("*.sql"))
        else:
            files.append(p)

    wh_id = get_warehouse(host, token)
    print(f"warehouse: {wh_id}\n")

    total = failed = 0
    for f in files:
        print(f"=== {f} ===")
        for n, stmt in enumerate(split_statements(f.read_text(encoding="utf-8")), 1):
            head = " ".join(stmt.split())[:80]
            try:
                rows = run_statement(host, token, wh_id, stmt)
                total += 1
                note = f" -> {len(rows)} row(s)" if rows else ""
                print(f"  [{n}] OK{note}  | {head}")
                for r in rows[:10]:
                    print(f"        {r}")
            except Exception as e:
                failed += 1
                print(f"  [{n}] FAIL  | {head}\n        {e}")
                if not keep_going:
                    sys.exit(1)
    print(f"\ndone: {total} ok, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
