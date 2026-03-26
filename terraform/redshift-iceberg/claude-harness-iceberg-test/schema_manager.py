"""
schema_manager.py
-----------------
Creates and drops all objects for both storage patterns.

Union-view pattern  → one Redshift table per calendar month of snapshot data,
                      plus a view that UNIONs them all.
Iceberg pattern     → external schema + Iceberg table in Glue / S3.

Agents: call SchemaManager.setup(conn) before loading data.
        Call SchemaManager.teardown(conn) to clean up after benchmarking.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redshift_connector

logger = logging.getLogger(__name__)

SETUP_DIR = Path(__file__).parent.parent / "setup"


def _month_periods(start_date: date, num_days: int) -> list[str]:
    """
    Returns sorted list of unique 'YYYYMM' strings covered by the date range.
    Example: start=2024-01-15, num_days=50 → ['202401', '202402', '202403']
    """
    months = set()
    for offset in range(num_days):
        d = start_date + timedelta(days=offset)
        months.add(d.strftime("%Y%m"))
    return sorted(months)


class SchemaManager:

    def __init__(self, cfg: dict):
        self.cfg    = cfg
        self.schema = cfg["redshift"]["schema"]
        d = cfg["data"]
        self.start  = date.fromisoformat(d["snapshot_start_date"])
        self.days   = d["date_range_days"]
        self.periods = _month_periods(self.start, self.days)   # e.g. ['202401', '202402']

    # ── setup ─────────────────────────────────────────────────────────────────
    def setup(self, conn: "redshift_connector.Connection") -> None:
        logger.info("Setting up benchmark schema (periods: %s)…", self.periods)
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            self._create_union_tables(cur)
            self._create_union_view(cur)
            self._create_iceberg_schema(cur)
            conn.commit()
        logger.info("Schema setup complete.")

    def _create_union_tables(self, cur: "redshift_connector.Cursor") -> None:
        ddl_template = (SETUP_DIR / "ddl_union_view.sql").read_text()
        # Extract only the CREATE TABLE block (everything before the comment block)
        table_block = ddl_template.split("-- ── Union view")[0]
        for period in self.periods:
            ddl = table_block.replace("{period}", period)
            cur.execute(ddl)
            logger.info("  Created table prop_snapshot_%s", period)

    def _create_union_view(self, cur: "redshift_connector.Cursor") -> None:
        selects = "\n    UNION ALL\n    ".join(
            f"SELECT * FROM {self.schema}.prop_snapshot_{p}"
            for p in self.periods
        )
        sql = (
            f"CREATE OR REPLACE VIEW {self.schema}.prop_snapshot_v AS\n"
            f"    {selects};"
        )
        cur.execute(sql)
        logger.info("  Created view prop_snapshot_v (spans %d partitions)", len(self.periods))

    def _create_iceberg_schema(self, cur: "redshift_connector.Cursor") -> None:
        raw = (SETUP_DIR / "ddl_iceberg.sql").read_text()
        ddl = (
            raw
            .replace("{glue_database}", self.cfg["aws"]["glue_database"])
            .replace("{iam_role_arn}",  self.cfg["aws"]["iam_role_arn"])
            .replace("{s3_bucket}",     self.cfg["aws"]["s3_bucket"])
            .replace("{s3_prefix}",     self.cfg["aws"]["s3_prefix"])
        )
        for stmt in self._split_sql(ddl):
            cur.execute(stmt)
        logger.info("  Created Iceberg external schema and table.")

    # ── teardown ──────────────────────────────────────────────────────────────
    def teardown(self, conn: "redshift_connector.Connection") -> None:
        logger.info("Tearing down benchmark objects…")
        with conn.cursor() as cur:
            cur.execute(f"DROP VIEW IF EXISTS {self.schema}.prop_snapshot_v;")
            for period in self.periods:
                cur.execute(f"DROP TABLE IF EXISTS {self.schema}.prop_snapshot_{period};")
            cur.execute(f"DROP SCHEMA IF EXISTS {self.schema};")
            cur.execute("DROP SCHEMA IF EXISTS benchmark_iceberg;")
            conn.commit()
        logger.info("Teardown complete.")

    # ── helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _split_sql(sql: str) -> list[str]:
        """Split on semicolons after stripping line comments."""
        clean = re.sub(r"--[^\n]*", "", sql)
        return [s.strip() for s in clean.split(";") if s.strip()]
