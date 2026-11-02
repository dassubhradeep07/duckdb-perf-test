"""Generate and process each selected size: one log per run, one shared stats CSV."""
import argparse
import os
import subprocess
import sys

import config as c
from logger import console
from monitoring import run_id


def positive_rows(value):
    try:
        return c.parse_rows(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=positive_rows, required=True,
                        help="Sizes in execution order, e.g. --rows 100000 1000000")
    parser.add_argument("--memory-limit", default=c.MEMORY_LIMIT, help="DuckDB RAM budget, e.g. 512MB")
    parser.add_argument("--threads", type=positive_rows, default=c.THREADS)
    parser.add_argument("--spill-limit", default=c.MAX_TEMP_DIRECTORY_SIZE, help="Maximum temporary disk, e.g. 10000MB")
    args = parser.parse_args()
    folder = c.BASE_DIR / "batches" / ("benchmark_" + run_id())
    folder.mkdir(parents=True)
    statistics = c.STATS_FILE
    for number, rows in enumerate(args.rows, start=1):
        batch = folder / "{:03d}_{}_rows".format(number, rows)
        batch.mkdir()
        console.print("\nBatch {}/{} | {:,} rows | {}".format(number, len(args.rows), rows, batch),
                      style="bold cyan", soft_wrap=True)
        environment = os.environ.copy()
        environment["DUCK_ETL_RUN_DIR"] = str(batch)
        command = [sys.executable, str(c.BASE_DIR / "main.py"), "--generate", "--rows", str(rows),
                   "--memory-limit", args.memory_limit, "--threads", str(args.threads)]
        if args.spill_limit is not None:
            command.extend(["--spill-limit", args.spill_limit])
        result = subprocess.run(command, cwd=str(c.BASE_DIR), env=environment, check=False)
        if result.returncode:
            console.print("Batch failed. See its log; completed/failed stage statistics: " + str(statistics),
                          style="red", soft_wrap=True)
            return result.returncode
    console.print("\nAll runs completed. Statistics: " + str(statistics), style="bold", soft_wrap=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
