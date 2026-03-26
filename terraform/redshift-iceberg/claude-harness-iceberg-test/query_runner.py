"""
query_runner.py
---------------
Executes benchmark queries against both storage patterns and collects:
  - wall-clock duration (milliseconds)
  - Redshift query execution plan stats (via SVV_QUERY_SUMMARY)
  - bytes scanned (via STL_SCAN)
  - estimated rows returned

Each query is run `iterations` times; the first `warm_cache_runs` are
discarded as warm-up. The runner captures p50, p95, and mean times.

Agents: call QueryRunner.run_all(conn) → returns list[QueryResult].
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from statistics import mean, median
from typing import TYPE_CHECKING

import numpy as np
import yaml

if TYPE_CHECKING:
    import redshift_connector

logger = logging.getLogger(__name__)

QUERIES_FILE = __import__("pathlib").Path(__file__).parent.parent / "queries" / "benchmark_queries.yaml"

# Placeholder → actual table mapping
PATTERN_TABLES = {
    "union_view": "benchmark.prop_snapshot_v",
    "iceberg":    "benchmark_iceberg.prop_snapshot_iceberg",
}


@dataclass
class QueryResult:
    query_id:    str
    query_label: str
    query_type:  str
    pattern:     str          # "union_view" or "iceberg"
    row_count:   int

    # raw timings in ms (after warm-up runs stripped)
    timings_ms:  list[float] = field(default_factory=list)

    # Redshift system-table stats (per last iteration)
    bytes_scanned: int | None  = None
    rows_scanned:  int | None  = None
    query_id_rs:   int | None  = None   # Redshift internal query ID

    @property
    def mean_ms(self)  -> float: return mean(self.timings_ms)  if self.timings_ms else 0.0
    @property
    def median_ms(self)-> float: return median(self.timings_ms) if self.timings_ms else 0.0
    @property
    def p95_ms(self)   -> float:
        return float(np.percentile(self.timings_ms, 95)) if self.timings_ms else 0.0
    @property
    def min_ms(self)   -> float: return min(self.timings_ms) if self.timings_ms else 0.0
    @property
    def max_ms(self)   -> float: return max(self.timings_ms) if self.timings_ms else 0.0

    def as_dict(self) -> dict:
        return {
            "query_id":     self.query_id,
            "query_label":  self.query_label,
            "query_type":   self.query_type,
            "pattern":      self.pattern,
            "row_count":    self.row_count,
            "mean_ms":      round(self.mean_ms,   1),
            "median_ms":    round(self.median_ms, 1),
            "p95_ms":       round(self.p95_ms,    1),
            "min_ms":       round(self.min_ms,    1),
            "max_ms":       round(self.max_ms,    1),
            "bytes_scanned":self.bytes_scanned,
            "rows_scanned": self.rows_scanned,
        }


class QueryRunner:

    def __init__(self, cfg: dict):
        self.cfg          = cfg
        self.iterations   = cfg["benchmark"]["iterations"]
        self.warm_runs    = cfg["benchmark"]["warm_cache_runs"]
        self.timeout_s    = cfg["benchmark"]["query_timeout_seconds"]
        self.queries      = yaml.safe_load(QUERIES_FILE.read_text())["queries"]

    # ── public ────────────────────────────────────────────────────────────────
    def run_all(
        self,
        conn: "redshift_connector.Connection",
        total_rows: int = 0,
    ) -> list[QueryResult]:
        results = []
        for pattern, table in PATTERN_TABLES.items():
            logger.info("=== Running queries against: %s ===", pattern)
            for qdef in self.queries:
                result = self._run_query(conn, qdef, pattern, table, total_rows)
                results.append(result)
                logger.info(
                    "  [%s/%s] mean=%.1fms  p95=%.1fms  bytes=%s",
                    pattern, qdef["id"],
                    result.mean_ms, result.p95_ms,
                    f"{result.bytes_scanned:,}" if result.bytes_scanned else "n/a",
                )
        return results

    # ── internals ─────────────────────────────────────────────────────────────
    def _run_query(
        self,
        conn: "redshift_connector.Connection",
        qdef: dict,
        pattern: str,
        table: str,
        total_rows: int,
    ) -> QueryResult:
        sql = qdef["sql"].format(table=table)
        result = QueryResult(
            query_id    = qdef["id"],
            query_label = qdef["label"],
            query_type  = qdef["query_type"],
            pattern     = pattern,
            row_count   = total_rows,
        )

        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {self.timeout_s * 1000};")

            # Warm-up runs (discarded)
            for _ in range(self.warm_runs):
                try:
                    cur.execute(sql)
                    cur.fetchall()
                except Exception as exc:
                    logger.warning("Warm-up failed for %s: %s", qdef["id"], exc)

            # Timed runs
            last_rs_query_id: int | None = None
            for i in range(self.iterations):
                t0 = time.perf_counter()
                try:
                    cur.execute(sql)
                    cur.fetchall()
                except Exception as exc:
                    logger.error("Query %s run %d failed: %s", qdef["id"], i, exc)
                    continue
                elapsed_ms = (time.perf_counter() - t0) * 1_000
                result.timings_ms.append(elapsed_ms)

                # Capture the Redshift query ID for the most recent execution
                cur.execute(
                    "SELECT pg_last_query_id();"
                )
                row = cur.fetchone()
                if row:
                    last_rs_query_id = row[0]

            result.query_id_rs = last_rs_query_id

        # Pull system-table stats for the last timed run
        if last_rs_query_id:
            result.bytes_scanned, result.rows_scanned = self._fetch_scan_stats(
                conn, last_rs_query_id
            )

        return result

    @staticmethod
    def _fetch_scan_stats(
        conn: "redshift_connector.Connection",
        rs_query_id: int,
    ) -> tuple[int | None, int | None]:
        """
        Reads bytes and rows scanned from STL_SCAN for the given query.
        STL_SCAN is a leader-node table — accessible via redshift_connector.
        """
        sql = """
            SELECT
                SUM(rows)         AS rows_scanned,
                SUM(bytes)        AS bytes_scanned
            FROM STL_SCAN
            WHERE query = %s
              AND type  = 2       -- type 2 = table scan (not network/sort)
        """
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (rs_query_id,))
                row = cur.fetchone()
                if row:
                    return int(row[1] or 0), int(row[0] or 0)
        except Exception as exc:
            logger.warning("Could not fetch STL_SCAN stats: %s", exc)
        return None, None
