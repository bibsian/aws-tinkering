"""
data_generator.py
-----------------
Generates simulated prop_snapshot data and loads both storage patterns.

Schema
------
  prop_cd       VARCHAR(20)  — 8,000 distinct values (configurable)
  snapshot_date DATE         — partition key; one "day" per loop iteration
  extract_date  DATE         — ETL load date; lags snapshot_date by 0–3 days

Volume
------
  records_per_prop_cd_per_day × num_prop_cds rows per snapshot day.
  At full scale: 1,000 × 8,000 = 8,000,000 rows/day.

Two load paths
--------------
  Union-view → INSERT into benchmark.prop_snapshot_{YYYYMM} tables
  Iceberg    → write Parquet to S3 partitioned by partition_month,
               then MSCK REPAIR to register with Spectrum

Agents: call DataGenerator.generate_and_load(conn) to run both paths.
"""

from __future__ import annotations

import io
import logging
import random
from datetime import date, timedelta
from typing import TYPE_CHECKING

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    import redshift_connector

logger = logging.getLogger(__name__)

# Parquet schema (partition_month excluded — written as directory key)
ARROW_SCHEMA = pa.schema([
    pa.field("prop_cd",       pa.string()),
    pa.field("snapshot_date", pa.date32()),
    pa.field("extract_date",  pa.date32()),
])


def _build_prop_cds(num: int, seed: int) -> list[str]:
    """
    Generates a stable list of prop_cd strings of the form 'P{n:06d}'.
    Deterministic so both patterns receive identical values.
    """
    rng = random.Random(seed)
    # Shuffle so inserts don't walk in sequential order (realistic distribution)
    ids = [f"P{i:06d}" for i in range(1, num + 1)]
    rng.shuffle(ids)
    return ids


class DataGenerator:

    def __init__(self, cfg: dict):
        self.cfg    = cfg
        self.s3     = boto3.client("s3", region_name=cfg["aws"]["region"])
        self.schema = cfg["redshift"]["schema"]

        d = cfg["data"]
        self.num_prop_cds   = d["num_prop_cds"]
        self.records_per_pd = d["records_per_prop_cd_per_day"]
        self.start_date     = date.fromisoformat(d["snapshot_start_date"])
        self.num_days       = d["date_range_days"]
        self.lag_max        = d["extract_lag_days_max"]
        self.seed           = d["seed"]

        self.prop_cds = _build_prop_cds(self.num_prop_cds, self.seed)

    # ── data shape ────────────────────────────────────────────────────────────
    def _iter_days(self):
        """Yields (snapshot_date, partition_month_str) for each day in range."""
        for offset in range(self.num_days):
            snap = self.start_date + timedelta(days=offset)
            yield snap, snap.strftime("%Y-%m")

    def _day_dataframe(self, snapshot_date: date, rng: random.Random) -> pd.DataFrame:
        """
        Generates all rows for a single snapshot day.

        Each prop_cd produces exactly records_per_prop_cd_per_day rows.
        extract_date is snapshot_date + a small random ETL lag (0–lag_max days),
        simulating the real-world delay between the business event and the load.
        """
        rows_per_prop = self.records_per_pd
        records = []
        for prop in self.prop_cds:
            lag = rng.randint(0, self.lag_max)
            extract_dt = snapshot_date + timedelta(days=lag)
            for _ in range(rows_per_prop):
                records.append((prop, snapshot_date, extract_dt))

        return pd.DataFrame(records, columns=["prop_cd", "snapshot_date", "extract_date"])

    # ── union-view load ───────────────────────────────────────────────────────
    def load_union_tables(self, conn: "redshift_connector.Connection") -> None:
        logger.info(
            "Loading union-view tables: %d prop_cds × %d rec/day × %d days = ~%s rows",
            self.num_prop_cds, self.records_per_pd, self.num_days,
            f"{self.num_prop_cds * self.records_per_pd * self.num_days:,}",
        )
        rng = random.Random(self.seed)

        with conn.cursor() as cur:
            for snap, month_str in self._iter_days():
                period = month_str.replace("-", "")   # '202401'
                table  = f"{self.schema}.prop_snapshot_{period}"
                df     = self._day_dataframe(snap, rng)
                logger.info("  Inserting %s rows into %s (snapshot %s)", f"{len(df):,}", table, snap)
                self._bulk_insert(cur, df, table)
            conn.commit()

        logger.info("Union-view load complete.")

    # ── iceberg / S3 load ─────────────────────────────────────────────────────
    def load_iceberg_table(self, conn: "redshift_connector.Connection") -> None:
        bucket = self.cfg["aws"]["s3_bucket"]
        prefix = self.cfg["aws"]["s3_prefix"]
        rng    = random.Random(self.seed)

        logger.info("Writing Parquet to S3 for Iceberg table…")

        # Accumulate one month at a time, then flush to a single Parquet file
        month_buffer: dict[str, list[pd.DataFrame]] = {}
        for snap, month_str in self._iter_days():
            df = self._day_dataframe(snap, rng)
            month_buffer.setdefault(month_str, []).append(df)

        for month_str, frames in month_buffer.items():
            combined = pd.concat(frames, ignore_index=True)
            key = f"{prefix}/partition_month={month_str}/data.parquet"
            table = pa.Table.from_pandas(combined, schema=ARROW_SCHEMA, preserve_index=False)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)
            self.s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            logger.info("  Uploaded s3://%s/%s (%s rows)", bucket, key, f"{len(combined):,}")

        # Register new partitions with Redshift Spectrum
        with conn.cursor() as cur:
            cur.execute("MSCK REPAIR TABLE benchmark_iceberg.prop_snapshot_iceberg;")
            conn.commit()

        logger.info("Iceberg load complete.")

    # ── helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _bulk_insert(
        cur: "redshift_connector.Cursor",
        df: pd.DataFrame,
        table: str,
        batch: int = 5_000,
    ) -> None:
        cols   = ", ".join(df.columns)
        pholds = ", ".join(["%s"] * len(df.columns))
        sql    = f"INSERT INTO {table} ({cols}) VALUES ({pholds})"
        rows   = [tuple(r) for r in df.itertuples(index=False)]
        for i in range(0, len(rows), batch):
            cur.executemany(sql, rows[i : i + batch])

    # ── entry point ───────────────────────────────────────────────────────────
    def generate_and_load(self, conn: "redshift_connector.Connection") -> int:
        """Load identical data into both patterns. Returns total row count."""
        total = self.num_prop_cds * self.records_per_pd * self.num_days
        self.load_union_tables(conn)
        self.load_iceberg_table(conn)
        return total
