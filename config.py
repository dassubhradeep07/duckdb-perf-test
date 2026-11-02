"""Edit these defaults before starting a benchmark. Paths are relative to this file."""
from pathlib import Path
import os

import duckdb

BASE_DIR = Path(__file__).resolve().parent
STATS_FILE = BASE_DIR / "stats.csv"  # Every size and every command appends here.

ROWS = 100_000
REFERENCE_ROWS = 100_000  # Fixed total reference rows for EVERY input size.
LOAD_TEST_SIZES = (100_000, 1_000_000, 5_000_000, 10_000_000, 20_000_000, 35_000_000)
THREADS = 2
MEMORY_LIMIT = "3GB"
MAX_TEMP_DIRECTORY_SIZE = None  # DuckDB default; override with --spill-limit, e.g. 10000MB.
MATCH_RULE = "email"  # "email" or "name_phone"
SEED = 42
MATCH_RATE = 0.35  # Intended overlap BEFORE validation and reference ambiguity.
QUALITY = {
    "null": 0.01,             # Independently applied to IDs and names.
    "duplicate": 0.01,        # Reuse the preceding customer's ID and identity.
    "spaces": 0.10,
    "mixed_case": 0.20,
    "invalid_email": 0.02,
    "missing_phone": 0.02,
}
REFERENCE_DUPLICATE_RATE = 0.01  # Share matching keys WITHIN the fixed reference total.
SAMPLE_INTERVAL = 1.0
TRACE_PYTHON_MEMORY = True  # Stage-scoped Python allocations; adds tracing overhead.
LOW_RAM_BYTES = 512 * 1024**2
LOW_DISK_BYTES = 2 * 1024**3
HIGH_RAM_PERCENT = 90
CONSOLE_LEVEL = "INFO"

def parse_rows(value):
    """Accept 100000, 100,000 or 100_000 on the command line."""
    rows = int(str(value).replace(",", "").replace("_", ""))
    if rows < 1:
        raise ValueError("Row count must be positive")
    return rows


def select_batch(rows):
    """Both entry points select the SAME folder for the SAME dataset size."""
    rows = parse_rows(rows)
    work = Path(os.environ.get("DUCK_ETL_RUN_DIR", str(BASE_DIR / "batches" / (str(rows) + "_rows")))).resolve()
    paths = dict(WORK_DIR=work, RAW_DIR=work / "data" / "raw",
                 SILVER_DIR=work / "data" / "silver", GOLD_DIR=work / "data" / "gold",
                 LOG_DIR=work / "logs", TEMP_DIR=work / "temp")
    for name, directory, filename in (
        ("RAW", "RAW_DIR", "customers"), ("REFERENCE", "RAW_DIR", "reference"),
        ("VALID", "SILVER_DIR", "valid"), ("REJECTED", "SILVER_DIR", "rejected"),
        ("SILVER", "SILVER_DIR", "customers"), ("CLASSIFIED", "SILVER_DIR", "matching"),
        ("MATCHED", "SILVER_DIR", "matched"), ("UNMATCHED", "SILVER_DIR", "unmatched"),
        ("AMBIGUOUS", "SILVER_DIR", "ambiguous"), ("GOLD", "GOLD_DIR", "customers"),
        ("REVIEW", "GOLD_DIR", "review"),
    ):
        paths[name] = paths[directory] / (filename + ".parquet")
    globals().update(paths)


select_batch(ROWS)


def prepare_directories():
    # Dataset folders are created by write_parquet only when an output is written.
    for path in (LOG_DIR, TEMP_DIR):
        path.mkdir(parents=True, exist_ok=True)


def sql_string(value):
    """Quote SQL string values, including Windows paths and apostrophes."""
    return "'" + str(value).replace("\\", "/").replace("'", "''") + "'"


def parquet(path):
    return "read_parquet({})".format(sql_string(path))


def connect():
    if THREADS < 1:
        raise ValueError("THREADS must be positive")
    if MATCH_RULE not in ("email", "name_phone"):
        raise ValueError("MATCH_RULE must be email or name_phone")
    prepare_directories()
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("SET threads = {}".format(int(THREADS)))
        connection.execute("SET memory_limit = {}".format(sql_string(MEMORY_LIMIT)))
        connection.execute("SET preserve_insertion_order = false")
        connection.execute("SET temp_directory = {}".format(sql_string(TEMP_DIR)))
        if MAX_TEMP_DIRECTORY_SIZE is not None:
            connection.execute("SET max_temp_directory_size = {}".format(sql_string(MAX_TEMP_DIRECTORY_SIZE)))
    except Exception:
        connection.close()
        raise
    return connection
