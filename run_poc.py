#!/usr/bin/env python3
"""
PLT POC runner: sentinel suite → dbt run

Usage:
  python run_poc.py
"""

import json
import subprocess
import sys
from pathlib import Path

import yaml

FUNCTIONAL = Path("/Users/anushkasingh/Desktop/sentinel-tests/functional")
SUITE      = "suites/post_load_transformations/plt_add_column.yml"
DBT_DIR    = Path(__file__).parent
VARS_FILE  = Path("/tmp/plt_poc_vars.json")

# ── Step 1: Run sentinel suite via manage.py ─────────────────────────────────
VARS_FILE.unlink(missing_ok=True)
subprocess.run(["./manage.py", "run_suite", SUITE], cwd=FUNCTIONAL, check=True)

if not VARS_FILE.exists():
    sys.exit("Tests did not write to /tmp/plt_poc_vars.json — did they pass?")

v = json.loads(VARS_FILE.read_text())

# ── Step 2: Read Snowflake creds from functional/config.yml ──────────────────
sf = yaml.safe_load((FUNCTIONAL / "config.yml").read_text())["local"]["test_dbs"]["snowflake"]
account = sf["host"].removeprefix("https://").removesuffix(".snowflakecomputing.com")

# ── Step 3: Write profiles.yml (auto-generated — not committed) ──────────────
profiles = {
    "plt_poc": {
        "target": "dev",
        "outputs": {
            "dev": {
                "type": "snowflake",
                "account": account,
                "user": sf["root_username"],
                "password": sf["root_password"],
                "warehouse": sf["warehouse"],
                "database": v["k1_db"],
                "schema": "PLT_FINAL",
                "threads": 4,
            }
        },
    }
}
(DBT_DIR / "profiles.yml").write_text(yaml.dump(profiles, default_flow_style=False))

# ── Step 4: Run dbt ──────────────────────────────────────────────────────────
dbt_cmd = ["dbt", "run", "--vars", json.dumps(v)]
print(f"\n{'=' * 60}")
print(f"Running: {' '.join(dbt_cmd)}")
print(f"{'=' * 60}\n")
subprocess.run(dbt_cmd, cwd=DBT_DIR, check=True)
