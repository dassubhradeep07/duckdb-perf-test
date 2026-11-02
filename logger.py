"""One readable console log and one detailed, timestamped file per run."""
import logging
import sys

from rich.console import Console
from rich.text import Text

import config

# Windows redirected terminals may default to cp1252, which cannot encode icons.
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")
console = Console()


class ConsoleHandler(logging.Handler):
    """Color complete log lines without Rich's narrow table-column wrapping."""

    def emit(self, record):
        try:
            style = {"WARNING": "yellow", "ERROR": "red", "CRITICAL": "bold red"}.get(record.levelname, "")
            console.print(Text(self.format(record), style=style), soft_wrap=True)
        except Exception:
            self.handleError(record)


def setup_logging(run_id):
    config.prepare_directories()
    log = logging.getLogger("etl")
    log.setLevel(logging.DEBUG)
    log.propagate = False
    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)
    terminal = ConsoleHandler()
    terminal.setLevel(config.CONSOLE_LEVEL)
    terminal.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S"))
    file_handler = logging.FileHandler(config.LOG_DIR / (run_id + ".log"), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(threadName)s | %(message)s"))
    log.addHandler(terminal)
    log.addHandler(file_handler)
    console.rule("DUCKDB ETL PERFORMANCE TEST")
    log.info("Run ID: %s", run_id)
    return log


def print_summary(stages, status):
    console.print("\nExecution summary — " + status, style="bold", soft_wrap=True)
    console.print("Stage          Status       Input   Output  Seconds    Rows/sec   Peak RAM MB   Peak temp MB", soft_wrap=True)
    for row in stages:
        console.print("{:<14} {:<8} {:>9,} {:>8,} {:>8.3f} {:>11,.0f} {:>13.1f} {:>14.1f}".format(
            row["stage"], row["status"], row["input_rows"], row["output_rows"],
            row["elapsed_seconds"], row["rows_per_second"],
            row["peak_process_rss_bytes"] / 1_000_000, row["peak_temp_bytes"] / 1_000_000), soft_wrap=True)
