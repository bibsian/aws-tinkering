"""
benchmark.py
------------
Main orchestrator for the Redshift storage-pattern benchmark.

Schema under test
-----------------
  prop_cd       VARCHAR(20)  — 8,000 distinct values, SORTKEY (leading)
  snapshot_date DATE         — partition key (monthly UNION ALL / Iceberg partition)
  extract_date  DATE         — ETL load date, SORTKEY (trailing)

Volume
------
  num_prop_cds × records_per_prop_cd_per_day × date_range_days rows total.
  At full scale: 8,000 × 1,000 × 30 = 240,000,000 rows.

AGENT USAGE
-----------
  1. Fill in config.yaml (host, s3_bucket, iam_role_arn)
  2. export RS_PASSWORD="..."
  3. Run:  python benchmark.py

Flags
-----
  --config PATH       path to config.yaml (default: ./config.yaml)
  --setup-only        create schema + load data, then stop
  --query-only        skip data load; run queries against existing data
  --teardown          drop all benchmark objects after the run
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import redshift_connector
import yaml

from harness.data_generator import DataGenerator
from harness.query_runner    import QueryRunner
from harness.reporter        import Reporter
from harness.schema_manager  import SchemaManager

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt = "%H:%M:%S",
)
logger = logging.getLogger("benchmark")


def load_config(path: str) -> dict:
    cfg = yaml.safe_load(Path(path).read_text())
    pwd = cfg["redshift"].get("password", "")
    if pwd.startswith("${") and pwd.endswith("}"):
        cfg["redshift"]["password"] = os.environ.get(pwd[2:-1], "")
    return cfg


def connect(cfg: dict) -> redshift_connector.Connection:
    rs = cfg["redshift"]
    logger.info("Connecting → %s / %s", rs["host"], rs["database"])
    conn = redshift_connector.connect(
        host=rs["host"], port=rs.get("port", 5439),
        database=rs["database"], user=rs["user"], password=rs["password"],
    )
    conn.autocommit = False
    return conn


def main(args: argparse.Namespace) -> None:
    cfg = load_config(args.config)

    d = cfg["data"]
    total_rows = d["num_prop_cds"] * d["records_per_prop_cd_per_day"] * d["date_range_days"]
    logger.info(
        "Run config: %d prop_cds × %d rec/day × %d days = %s total rows",
        d["num_prop_cds"], d["records_per_prop_cd_per_day"], d["date_range_days"],
        f"{total_rows:,}",
    )

    schema_mgr = SchemaManager(cfg)
    data_gen   = DataGenerator(cfg)
    runner     = QueryRunner(cfg)
    reporter   = Reporter(cfg)

    conn = connect(cfg)
    try:
        if not args.query_only:
            schema_mgr.setup(conn)
            data_gen.generate_and_load(conn)

        if not args.setup_only:
            results = runner.run_all(conn, total_rows)
            reporter.write(results, total_rows)
            _print_summary(results, total_rows)

        if args.teardown:
            schema_mgr.teardown(conn)

    finally:
        conn.close()
        logger.info("Done.")


def _print_summary(results: list, total_rows: int) -> None:
    print(f"\n{'='*72}")
    print(f"  RESULTS — {total_rows:,} rows")
    print(f"{'='*72}")
    print(f"  {'Query':<40} {'Union ms':>9} {'Iceberg ms':>11} {'Winner':<12}")
    print(f"  {'-'*40} {'-'*9} {'-'*11} {'-'*12}")

    pairs: dict[str, dict] = {}
    for r in results:
        pairs.setdefault(r.query_id, {})[r.pattern] = r

    for pat in pairs.values():
        uv = pat.get("union_view")
        ic = pat.get("iceberg")
        if not uv or not ic:
            continue
        winner = "Iceberg ✓" if ic.mean_ms < uv.mean_ms else "Union  ✓"
        print(f"  {uv.query_label:<40} {uv.mean_ms:>9.1f} {ic.mean_ms:>11.1f} {winner}")
    print()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config",      default="config.yaml")
    p.add_argument("--setup-only",  action="store_true")
    p.add_argument("--query-only",  action="store_true")
    p.add_argument("--teardown",    action="store_true")
    main(p.parse_args())
