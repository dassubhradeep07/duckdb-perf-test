"""Run the sequential pipeline, or exactly one stage using existing inputs."""
import argparse
import sys
import time

import config as c
from etl import get_stages, count
from generate_data import generate
from logger import print_summary, setup_logging
from monitoring import memory_label, memory_budget_bytes, run_id, run_stage, system_info, timestamp, write_reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=list(get_stages()), help="Run only this stage; inputs must already exist")
    parser.add_argument("--generate", action="store_true", help="Generate raw/reference, then run the full pipeline")
    parser.add_argument("--rows", type=c.parse_rows, default=c.ROWS,
                        help="Dataset size to process (or generate); defaults to config.ROWS")
    parser.add_argument("--memory-limit", default=c.MEMORY_LIMIT, help="DuckDB RAM budget, e.g. 512MB")
    parser.add_argument("--threads", type=c.parse_rows, default=c.THREADS, help="DuckDB worker limit")
    parser.add_argument("--spill-limit", default=c.MAX_TEMP_DIRECTORY_SIZE,
                        help="Maximum DuckDB temporary disk usage, e.g. 10000MB")
    args = parser.parse_args()
    if args.generate and args.stage:
        parser.error("--generate runs the full pipeline and cannot be combined with --stage")
    c.MEMORY_LIMIT = args.memory_limit
    c.THREADS = args.threads
    c.MAX_TEMP_DIRECTORY_SIZE = args.spill_limit
    c.select_batch(args.rows)
    identifier = run_id()
    log = setup_logging(identifier)
    stages = []
    metadata = dict(system_info(), run_id=identifier, test_dataset_rows=args.rows,
                    started_at=timestamp(), run_status="failed", command="generate_and_etl" if args.generate else args.stage or "all")
    start = time.perf_counter()
    connection = None
    exit_code = 0
    try:
        connection = c.connect()
        log.info("Selected dataset: %s rows | folder: %s", format(args.rows, ","), c.WORK_DIR)
        log.info("Spill directory: %s | maximum spill: %s", c.TEMP_DIR,
                 c.MAX_TEMP_DIRECTORY_SIZE or "DuckDB default")
        if args.generate:
            run_stage(connection, "generation", lambda con: generate(con, args.rows),
                      [], [c.RAW, c.REFERENCE], stages)
        if not c.RAW.is_file():
            raise FileNotFoundError("No raw data for {:,} rows. First run: python generate_data.py --rows {}. Then: python main.py --rows {}".format(args.rows, args.rows, args.rows))
        actual_rows = count(connection, c.RAW)
        if actual_rows != args.rows:
            raise ValueError("Selected {:,} rows but the raw file has {:,}. Regenerate with: python generate_data.py --rows {}".format(args.rows, actual_rows, args.rows))
        log.info("CPU: %s | cores: %s | RAM: %s", metadata["cpu_model"],
                 metadata["physical_cores"], memory_label(metadata["total_system_ram_bytes"]))
        log.info("DuckDB: %s | threads: %s | memory budget: %s | matching: %s",
                 metadata["duckdb_version"], c.THREADS, memory_label(memory_budget_bytes()), c.MATCH_RULE)
        stage_definitions = get_stages()
        selected = [args.stage] if args.stage else list(stage_definitions)
        for name in selected:
            function, inputs, outputs = stage_definitions[name]
            run_stage(connection, name, function, inputs, outputs, stages)
        metadata["run_status"] = "success"
    except (Exception, KeyboardInterrupt) as error:
        metadata["error"] = "{}: {}".format(type(error).__name__, error)
        log.exception("Pipeline stopped")
        exit_code = 1
    finally:
        if connection is not None:
            connection.close()
        metadata.update(ended_at=timestamp(), overall_elapsed_seconds=time.perf_counter() - start,
                        overall_peak_process_rss_bytes=max((s["peak_process_rss_bytes"] for s in stages), default=0),
                        overall_peak_temp_bytes=max((s["peak_temp_bytes"] for s in stages), default=0))
        write_reports(metadata, stages)
        print_summary(stages, metadata["run_status"])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
