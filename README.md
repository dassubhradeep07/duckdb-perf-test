# DuckDB + Parquet customer load test

Generate each test size, then execute that same size. Python 3.10+ and DuckDB,
psutil and Rich are required; the local `.venv` is already set up.

## Start with 100,000 records

In the activated virtual environment, execute these commands one at a time:

```powershell
python generate_data.py --rows 100000
python main.py --rows 100000
```

The first command writes incoming customers and reference masters to
`batches/100000_rows/data/raw/`. The second selects that same folder, checks that
its raw file has exactly 100,000 rows, and runs all five ETL stages.
`--rows` selects a dataset folder; it does not sample another dataset.
Missing data produces a message with the required generation command.
Both scripts default to `config.ROWS` (100,000) if `--rows` is omitted.

For the next size, generate and execute it explicitly:

```powershell
python generate_data.py --rows 1000000
python main.py --rows 1000000
```

Use the same pair for 5000000, 10000000, 20000000, or 35000000. Sizes remain
independent. Commands accept 100000, 100,000 or 100_000, but examples use plain digits.
`BENCHMARK_COMMANDS.txt` contains every pair in order and setup/activation steps.
No benchmark runs automatically during setup.

## Minimal output folders

```text
stats.csv                     Shared statistics for EVERY size and command
batches/
    100000_rows/
        data/raw/             Generated customers.parquet and reference.parquet
        data/silver/          Validation, cleaning and matching datasets
        data/gold/            Master customers and ambiguous review records
        logs/<run_id>.log     One log per command invocation
        temp/                 DuckDB spill files
    1000000_rows/
        ...
```

Folders are created when needed. There are no unused top-level data/logs/reports
folders or duplicate JSON, TXT, archive or history reports. All displayed RAM and
storage sizes use decimal MB (1 MB = 1,000,000 bytes).

Generation contributes `generation` and overall `run` rows to the project-level
`stats.csv`. Processing contributes five stage rows and a `run` row to that SAME
file, regardless of dataset size. The `command` column distinguishes generation
from processing. `run_id` connects each
invocation's rows to its log. Reruns append statistics to the common CSV with a new run ID and replace
outputs for that size. After regenerating inputs, rerun downstream processing
before using any previously generated Gold. Do not run two commands concurrently
against the same size's folder.

## What the pipeline does

1. Generate repeatable synthetic customers with configurable nulls, duplicate IDs,
   invalid emails, missing phones, spaces and case differences. Default intended
   reference overlap is 35%, before rejection and reference ambiguity.
2. Inspect Raw counts/schema. Count queries may read Parquet metadata; this is not
   a forced full-column scan or cold-disk benchmark.
3. Validate IDs, nonblank names, email format, and normalized 10-15 digit phones.
   Reject all occurrences of duplicate non-null IDs; retain rejection reasons.
4. Normalize whitespace, uppercase names/locations, lowercase emails and phone digits.
5. Match by the selected email OR name+phone rule. Group reference keys first:
   zero masters is unmatched, one is matched, multiple is ambiguous. Blank/null
   reference keys do not match. Master IDs must be unique and positive.
6. Retain all reference masters in Gold. Matched masters use the newest whole record;
   reference wins equal dates, then lowest incoming customer ID wins. Consolidate
   unmatched customers by the selected key and assign IDs above the maximum
   reference ID. Ambiguous incoming records are excluded and saved for review.

Gold is not automatically promoted to the next run's reference. All databases are
in-memory; permanent datasets are Parquet. SQL operates in DuckDB without Pandas
or collecting customer datasets into Python. Writes replace each Parquet file only
when COPY succeeds; replacement is per file, not a transaction across stages.

## Statistics and resource measurements

The project-level `stats.csv` records volume, quality/matching counts, timing, throughput, raw and
reference MB, retained Parquet MB, stage input/output MB, and observed spill MB.
It also records configured RAM/thread limits, peak process RAM, DuckDB-tracked
memory, Python-traced memory, budget ratios, observed total/active process threads,
CPU capacity percentages, system RAM pressure and available disk/RAM. Versions,
CPU model, matching rule, status/errors and UTC timestamps support comparisons.
Filter `stage=run` for overall results and inspect the matching log for full SQL,
quality counters, memory readings and tracebacks.

DuckDB memory uses `duckdb_memory()` on an independent cursor sharing the database.
It covers the buffer manager, not every native allocation. Python `tracemalloc`
measures allocations during a stage, excluding earlier allocations. These counters
do not add up to resident process RAM. See
[DuckDB memory metadata](https://duckdb.org/docs/1.4/sql/meta/duckdb_table_functions#duckdb_memory)
and [Python tracemalloc](https://docs.python.org/3/library/tracemalloc.html).

Sampling runs approximately once per second plus boundaries; brief peaks and
short-lived threads can be missed. Process thread counts include Python/monitoring
and idle threads. Active threads have increasing CPU time, not necessarily
simultaneous execution. CPU capacity is core-equivalents / configured DuckDB threads
* 100 and may exceed 100%. File footprints are not physical I/O counters. Retained
Parquet MB includes intermediate copies. Warnings display actual available RAM or
free spill-disk space and do not stop processing based on one high reading.

## Settings and other execution options

Edit `config.py`: defaults are two DuckDB threads, a 3GB (=3000 MB) budget, email
matching, and one-second sampling. `TRACE_PYTHON_MEMORY` controls tracing overhead;
keep the same setting when comparing benchmarks. A memory limit is not reserved RAM.

```powershell
# Optional: generate and process one size in a single invocation/log.
python main.py --generate --rows 100000

# Debug a stage for one already generated/processed size.
python main.py --rows 100000 --stage matching

# Optional automatic sequential generation + processing for multiple sizes.
python run_batches.py --rows 100000 1000000
```

The optional multi-size runner uses its own timestamped group and isolated dataset
folders, but appends to the same project-level `stats.csv`. `DUCK_ETL_RUN_DIR` is an
internal runner path override; manual commands do not need it.

## Readable HTML benchmark report

```powershell
python build_benchmark_report.py
```

Open `benchmark_report.html` in a browser. It compares the latest full pipeline
run for each input size using compact counts, rounded MB, readable times, customer
outcomes, resource usage and expandable stage details. A newer generation without
processing is shown as pending. Earlier results remain in `stats.csv`. Rebuild the
HTML after adding new measurements; the report does not modify source statistics.

Business SQL is in `etl.py`. Path selection is in `config.py`, generation in
`generate_data.py`, execution in `main.py`/`run_batches.py`, monitoring/CSV reporting
in `monitoring.py`, and logging in `logger.py`. Setup and every batch's manual
commands are in `BENCHMARK_COMMANDS.txt`.

Reference size is fixed by `REFERENCE_ROWS = 100000` for every input size.
The 100,000 total includes 99,000 distinct customer identities and 1,000 additional
masters sharing matching keys, controlled by `REFERENCE_DUPLICATE_RATE`. Duplicates
are included within the total, not added above it. Master IDs remain unique.
`MATCH_RATE = 0.35` controls incoming identity overlap only, not reference size.
Incoming matching identities reuse reference keys when a large batch needs more
matching records than there are reference customers. Unmatched identities use keys
outside the reference range. Raw customer IDs and all quality defect rules retain
their existing behavior. The same reference contents are regenerated for each
batch with the same settings/seed. Actual file MB is measured in the common CSV.

## Exercise spill-to-disk behavior

Each connection already points `temp_directory` at the selected batch's `temp/`.
Use a smaller per-run memory budget to exercise the pipeline's larger-than-memory
operators without changing validation, transformation, matching or Gold rules:

```powershell
# Generate only if this size is not already available.
python generate_data.py --rows 10000000

# Baseline and constrained run: same input and thread limit.
python main.py --rows 10000000 --memory-limit 3000MB --threads 2 --spill-limit 10000MB
python main.py --rows 10000000 --memory-limit 512MB --threads 2 --spill-limit 10000MB
```

All runs append to `stats.csv`. Compare the same stage's `ram_budget_mb`,
`peak_temp_mb`, duration, throughput and memory. A positive observed `peak_temp_mb`
indicates temporary files in DuckDB's configured spill directory. Files can be
removed before the end of a query/run; a final empty directory does not disprove
spilling. Sampling can miss short-lived files.

The CLI overrides apply only to that process. The default remains 3000 MB and two
threads. `--spill-limit` sets `max_temp_directory_size` and caps temporary storage,
not retained Parquet outputs. It defaults to DuckDB's own limit unless configured.
The same flags work with `run_batches.py`.

Input Parquet size is not the working memory requirement: some queries stream;
others need large intermediate sorts, windows or hash tables. Spilling adds disk
I/O and still requires working RAM. If 512 MB is insufficient, try 768 or 1024 MB;
not every query/operator can succeed with an arbitrarily small budget. See
[DuckDB memory management](https://duckdb.org/2024/07/09/memory-management).
