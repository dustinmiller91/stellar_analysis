"""
etl.py — Stellar Analysis ETL Pipeline
Downloads, transforms, and loads Hipparcos and Woolley catalog data into SQLite.

Idempotent: safe to run multiple times; all stages use DROP/CREATE or INSERT OR REPLACE.

Usage:
    python etl.py [--db stellar_analysis.db]
"""

import argparse
import csv
import io
import logging
import sqlite3
import urllib.request
from pathlib import Path

import pandas as pd

from stellar_parser import parse_stellar_column

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SOURCES = {
    "hipparcos": {
        "url": (
            "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync"
            "?REQUEST=doQuery&LANG=ADQL&FORMAT=csv"
            '&QUERY=SELECT+*+FROM+"I/239/hip_main"'
        ),
        "bronze_table": "hipparcos_bronze",
        "silver_table": "hipparcos_silver",
        "silver_cols": {
            "ccdm":   "TEXT",
            "plx":    "REAL",
            "e_plx":  "REAL",
            "vmag":   "REAL",
            "b_v":    "REAL",
            "e_b_v":  "REAL",
            "sptype": "TEXT",
        },
    },
    "woolley": {
        "url": (
            "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync"
            "?REQUEST=doQuery&LANG=ADQL&FORMAT=csv"
            '&QUERY=SELECT+*+FROM+"V/32A/catalog"'
        ),
        "bronze_table": "woolley_bronze",
        "silver_table": "woolley_silver",
        "silver_cols": {
            "recno":   "TEXT",
            "plx":     "REAL",
            "e_plx":   "REAL",
            "B_V":     "REAL",
            "Mv":      "REAL",
            "SpType":  "TEXT",
            "LC_Code": "TEXT",
        },
    },
}

# Gold table: union of both silver tables with quality filters applied
SQL_GOLD = """
    CREATE TABLE stars AS
    WITH hip_tmp AS (
        SELECT
             1000 / (plx * 1.0)                          AS dist_pc
            ,vmag - 5 * LOG10(1000 / (plx * 1.0)) + 5   AS abs_mag
            ,b_v
            ,sp_class
            ,lum_class
            ,src
        FROM hipparcos_silver
        WHERE plx  IS NOT NULL AND plx  > 0
          AND vmag IS NOT NULL
          AND b_v  IS NOT NULL
          AND sp_class  IS NOT NULL AND sp_class  != ''
          AND lum_class IS NOT NULL AND lum_class != ''
    ),
    wly_tmp AS (
        SELECT
             1000 / (plx * 1.0)  AS dist_pc
            ,mv                  AS abs_mag
            ,b_v
            ,sp_class
            ,lum_class
            ,src
        FROM woolley_silver
        WHERE plx    IS NOT NULL AND plx    > 0
          AND mv     IS NOT NULL
          AND b_v    IS NOT NULL
          AND sp_class  IS NOT NULL AND sp_class  != ''
          AND lum_class IS NOT NULL AND lum_class != ''
    )
    SELECT * FROM hip_tmp
    UNION ALL
    SELECT * FROM wly_tmp
    ORDER BY dist_pc ASC
"""

# Woolley stores luminosity class as integer code; map to roman numerals
SQL_WLY_LC_UPDATE = """
    UPDATE woolley_silver
    SET lum_class = CASE lc_code
        WHEN '1' THEN 'I'
        WHEN '2' THEN 'II'
        WHEN '3' THEN 'III'
        WHEN '4' THEN 'IV'
        WHEN '5' THEN 'V'
        WHEN '6' THEN 'VI'
        WHEN '7' THEN 'VII'
        ELSE ''
    END
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def download_csv(url: str) -> list[dict]:
    """Download a VizieR TAP CSV response and return parsed rows."""
    log.info("Downloading %s", url[:80])
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")

    lines = [l for l in raw.splitlines() if not l.startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    cols = reader.fieldnames
    expected = len(cols)

    rows = []
    skipped = 0
    for i, row in enumerate(reader):
        row.pop(None, None)
        if len(row) != expected:
            skipped += 1
            continue
        rows.append(row)

    log.info("Parsed %d rows (%d skipped)", len(rows), skipped)
    return rows, cols


# ---------------------------------------------------------------------------
# Bronze stage
# ---------------------------------------------------------------------------

def load_bronze(con: sqlite3.Connection, table: str, rows: list[dict], cols: list[str]) -> None:
    """Drop-and-recreate bronze table; all columns TEXT."""
    col_defs = ", ".join(
        f'"{c.strip().replace("-", "_").lower()}" TEXT' for c in cols
    )
    placeholders = ", ".join("?" * len(cols))

    with con:
        con.execute(f'DROP TABLE IF EXISTS "{table}"')
        con.execute(f'CREATE TABLE "{table}" ({col_defs})')
        con.executemany(
            f'INSERT INTO "{table}" VALUES ({placeholders})',
            [[row[c] for c in cols] for row in rows],
        )

    log.info("Bronze '%s': %d rows loaded", table, len(rows))


# ---------------------------------------------------------------------------
# Silver stage
# ---------------------------------------------------------------------------

def _cast_column(series: pd.Series, dtype: str) -> pd.Series:
    """Cast a string Series to the target SQLite type."""
    if dtype == "REAL":
        extracted = series.astype(str).str.strip().str.extract(r"(-?\d+\.?\d*)")[0]
        return pd.to_numeric(extracted, errors="coerce")
    if dtype == "INTEGER":
        cleaned = series.astype(str).str.strip().str.replace(r"[^\d]", "", regex=True)
        return pd.to_numeric(cleaned, errors="coerce").astype("Int64")
    return series  # TEXT — no conversion


def build_silver(
    con: sqlite3.Connection,
    source_table: str,
    target_table: str,
    col_spec: dict[str, str],
) -> None:
    """
    Read selected columns from a bronze table, cast types, parse spectral
    classification, and write to a silver table. Idempotent via replace.
    """
    src_cols_sql = ", ".join(f'"{c}"' for c in col_spec)
    df = pd.read_sql(f'SELECT {src_cols_sql} FROM "{source_table}"', con)

    # Normalise column names
    df.columns = [c.lower().replace("-", "_") for c in df.columns]
    col_types = {k.lower().replace("-", "_"): v for k, v in col_spec.items()}

    for col, dtype in col_types.items():
        if col in df.columns:
            df[col] = _cast_column(df[col], dtype)

    # Parse raw spectral type string into (sp_class, lum_class)
    parsed = parse_stellar_column(df["sptype"])
    df["sp_class"] = parsed["spectral_type"]
    df["lum_class"] = parsed["lum_class"]

    # Tag source
    src_name = source_table.split("_")[0]
    df["src"] = src_name

    # Sort nearest-first; nulls last
    df.sort_values("plx", ascending=False, na_position="last", inplace=True)

    df.to_sql(target_table, con, if_exists="replace", index=False)

    cur = con.cursor()
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{target_table}_sp "
        f"ON {target_table}(sp_class)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{target_table}_plx "
        f"ON {target_table}(sp_class, plx DESC)"
    )
    con.commit()

    count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {target_table}", con).iloc[0]["n"]
    log.info("Silver '%s': %d rows", target_table, count)


def fix_woolley_lum_class(con: sqlite3.Connection) -> None:
    """Map Woolley integer lc_code -> roman numeral lum_class; drop lc_code."""
    with con:
        con.execute(SQL_WLY_LC_UPDATE)
        try:
            con.execute("ALTER TABLE woolley_silver DROP COLUMN lc_code")
        except Exception:
            pass  # already dropped on re-run


# ---------------------------------------------------------------------------
# Gold stage
# ---------------------------------------------------------------------------

def build_gold(con: sqlite3.Connection) -> None:
    """Create unified analytical stars table from both silver tables."""
    with con:
        con.execute("DROP TABLE IF EXISTS stars")
        con.execute(SQL_GOLD)
        for ddl in [
            "CREATE INDEX IF NOT EXISTS idx_stars_sp      ON stars (sp_class)",
            "CREATE INDEX IF NOT EXISTS idx_stars_lum     ON stars (lum_class)",
            "CREATE INDEX IF NOT EXISTS idx_stars_sp_dist ON stars (sp_class,  dist_pc ASC)",
            "CREATE INDEX IF NOT EXISTS idx_stars_lm_dist ON stars (lum_class, dist_pc ASC)",
        ]:
            con.execute(ddl)

    count = pd.read_sql("SELECT COUNT(*) AS n FROM stars", con).iloc[0]["n"]
    log.info("Gold 'stars': %d rows", count)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(db_path: str) -> None:
    log.info("Database: %s", db_path)
    con = get_connection(db_path)

    for name, cfg in SOURCES.items():
        log.info("=== %s ===", name.upper())
        rows, cols = download_csv(cfg["url"])
        load_bronze(con, cfg["bronze_table"], rows, cols)
        build_silver(con, cfg["bronze_table"], cfg["silver_table"], cfg["silver_cols"])

    fix_woolley_lum_class(con)
    build_gold(con)
    con.close()
    log.info("ETL complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stellar analysis ETL")
    parser.add_argument("--db", default="stellar_analysis.db", help="SQLite database path")
    args = parser.parse_args()
    run(args.db)
