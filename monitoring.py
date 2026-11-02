"""Measured stage statistics and CSV/JSON reports; one lightweight sampler thread."""
import csv
import json
import logging
import os
import platform
import re
import threading
import time
import tracemalloc
import uuid
from datetime import datetime, timezone

import duckdb
import psutil

import config as c

log = logging.getLogger("etl")


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def run_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]


def directory_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                size += os.stat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                pass  # DuckDB can delete a spill file between listing and stat.
    return size


def file_size(paths):
    return sum(path.stat().st_size for path in paths if path.is_file())


def memory_label(value):
    return "n/a" if value is None else "{:.2f} MB".format(value / 1_000_000)


def mb_statistics(values):
    """Keep internal counters in bytes; present sizes consistently as decimal MB."""
    return {key[:-6] + "_mb" if key.endswith("_bytes") else key:
            round(value / 1_000_000, 3) if key.endswith("_bytes") and value is not None else value
            for key, value in values.items() if key != "duckdb_memory_limit"}


def memory_budget_bytes():
    """Convert common DuckDB limit units. Unknown/unlimited budgets have no ratio."""
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)\s*",
                         c.MEMORY_LIMIT, re.IGNORECASE)
    if not match:
        return None
    units = dict(B=1, KB=1000, MB=1000**2, GB=1000**3, TB=1000**4,
                 KIB=1024, MIB=1024**2, GIB=1024**3, TIB=1024**4)
    return int(float(match[1]) * units[match[2].upper()]) or None


def system_info():
    # Windows often exposes the CPU model here; platform.processor is the fallback.
    cpu_model = os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor() or "unknown"
    if platform.system() == "Windows":
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                               r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
                cpu_model = winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
        except OSError:
            pass
    return dict(duckdb_version=duckdb.__version__, python_version=platform.python_version(),
                os=platform.platform(), cpu_model=cpu_model,
                physical_cores=psutil.cpu_count(logical=False), logical_cpus=psutil.cpu_count(),
                total_system_ram_bytes=psutil.virtual_memory().total,
                configured_threads=c.THREADS, duckdb_memory_limit=c.MEMORY_LIMIT,
                duckdb_memory_budget_bytes=memory_budget_bytes(), work_dir=str(c.WORK_DIR),
                python_memory_tracking_enabled=c.TRACE_PYTHON_MEMORY,
                match_rule=c.MATCH_RULE, sample_interval_seconds=c.SAMPLE_INTERVAL)


def run_stage(connection, name, function, inputs, outputs, reports):
    """The finally block always stops sampling and records failure statistics."""
    process = psutil.Process()
    stop = threading.Event()
    samples = []  # Only scalar resource snapshots, never customer data.
    # Keep running aggregates rather than one snapshot per second on long runs.
    peaks = dict(rss=0, temp=0, system_ram=0, cpu_sum=0.0, samples=0,
                 threads=0, active_threads=0, process_cpu=0.0)
    previous_threads = {}
    previous_cpu = None
    minimums = dict(available_ram=None, free_disk=None)
    budget = memory_budget_bytes()
    warnings_seen = set()
    memory_connection = None
    duckdb_tracking_available = True
    tracing_started = False
    memory_peaks = dict(duckdb=None, python=None)
    start = time.perf_counter()
    cpu_start = process.cpu_times()
    row = dict(stage=name, status="running", started_at=timestamp(), input_rows=0, output_rows=0,
               input_paths=[str(path) for path in inputs], output_paths=[str(path) for path in outputs],
               input_bytes=file_size(inputs), physical_cores=psutil.cpu_count(logical=False),
               logical_cpus=psutil.cpu_count(), duckdb_memory_limit=c.MEMORY_LIMIT,
               duckdb_memory_budget_bytes=budget, configured_threads=c.THREADS,
               duckdb_managed_memory_bytes=None, python_traced_current_bytes=None,
               python_trace_scope="disabled", duckdb_memory_sample_count=0)

    def sample():
        nonlocal previous_cpu, duckdb_tracking_available
        duckdb_bytes = None
        if duckdb_tracking_available:
            try:
                # This independent cursor shares the database's buffer manager.
                # Never execute sampler SQL on the pipeline's active connection.
                duckdb_bytes = memory_connection.execute(
                    "SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory()"
                ).fetchone()[0]
                row["duckdb_memory_sample_count"] += 1
                memory_peaks["duckdb"] = max(memory_peaks["duckdb"] or 0, duckdb_bytes)
            except Exception:
                duckdb_tracking_available = False
                log.warning("[%s] DuckDB memory counter unavailable; reporting n/a", name, exc_info=True)
        python_current = None
        if c.TRACE_PYTHON_MEMORY and tracemalloc.is_tracing():
            python_current, python_peak = tracemalloc.get_traced_memory()
            # An externally started tracer's lifetime peak may precede this stage.
            observed_peak = python_peak if tracing_started else python_current
            memory_peaks["python"] = max(memory_peaks["python"] or 0, observed_peak)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(str(c.TEMP_DIR))
        minimums["available_ram"] = min(minimums["available_ram"], memory.available) if minimums["available_ram"] is not None else memory.available
        minimums["free_disk"] = min(minimums["free_disk"], disk.free) if minimums["free_disk"] is not None else disk.free
        rss = process.memory_info().rss
        temp = directory_size(c.TEMP_DIR)
        cpu = psutil.cpu_percent(interval=None)
        thread_times = {thread.id: thread.user_time + thread.system_time for thread in process.threads()}
        active_threads = sum(value > previous_threads.get(key, 0) for key, value in thread_times.items()) if previous_cpu is not None else 0
        previous_threads.clear()
        previous_threads.update(thread_times)
        now = time.perf_counter()
        process_times = process.cpu_times()
        cpu_total = process_times.user + process_times.system
        interval_cpu = 0.0
        if previous_cpu is not None and now > previous_cpu[0]:
            interval_cpu = 100 * max(0, cpu_total - previous_cpu[1]) / (now - previous_cpu[0])
        previous_cpu = (now, cpu_total)
        peaks["threads"] = max(peaks["threads"], len(thread_times))
        peaks["active_threads"] = max(peaks["active_threads"], active_threads)
        peaks["process_cpu"] = max(peaks["process_cpu"], interval_cpu)
        peaks["rss"] = max(peaks["rss"], rss)
        peaks["temp"] = max(peaks["temp"], temp)
        peaks["system_ram"] = max(peaks["system_ram"], memory.percent)
        peaks["cpu_sum"] += cpu
        peaks["samples"] += 1
        snapshot = dict(process_rss_bytes=rss, duckdb_managed_memory_bytes=duckdb_bytes,
                        python_traced_current_bytes=python_current,
                        temp_bytes=temp, process_thread_count=len(thread_times),
                        active_process_threads=active_threads,
                        total_system_ram_bytes=memory.total, available_system_ram_bytes=memory.available,
                        used_system_ram_bytes=memory.used, system_ram_percent=memory.percent,
                        disk_total_bytes=disk.total, disk_used_bytes=disk.used, disk_free_bytes=disk.free)
        samples[:] = [snapshot]
        conditions = (("low_ram", memory.available < c.LOW_RAM_BYTES,
                       "Available system RAM {:.1f} MB is below {:.1f} MB".format(memory.available / 1_000_000, c.LOW_RAM_BYTES / 1_000_000)),
                      ("high_ram", memory.percent >= c.HIGH_RAM_PERCENT,
                       "System RAM {:.1f}% used; {:.1f} MB available".format(memory.percent, memory.available / 1_000_000)),
                      ("disk", disk.free < c.LOW_DISK_BYTES,
                       "Spill disk free {:.1f} MB is below {:.1f} MB".format(disk.free / 1_000_000, c.LOW_DISK_BYTES / 1_000_000)))
        for key, condition, message in conditions:
            if condition and key not in warnings_seen:
                warnings_seen.add(key)
                log.warning("[%s] %s", name, message)

    def sampler():
        psutil.cpu_percent(interval=None)  # Prime this thread's system CPU counter.
        while not stop.wait(c.SAMPLE_INTERVAL):
            try:
                sample()
                log.info("[%s] working | budget %s | process RSS %.1f MB | DuckDB tracked %s | Python traced %s | temp %.1f MB | %.1fs",
                         name, memory_label(budget), samples[0]["process_rss_bytes"] / 1_000_000,
                         memory_label(samples[0]["duckdb_managed_memory_bytes"]),
                         memory_label(samples[0]["python_traced_current_bytes"]),
                         samples[0]["temp_bytes"] / 1_000_000, time.perf_counter() - start)
            except Exception:
                log.exception("[%s] Resource sampler failed", name)
                break

    log.info("▶ [%s] Started", name)
    log.debug("[%s] Start: %s | Input: %s | Output: %s", name, row["started_at"], row["input_paths"], row["output_paths"])
    for label, paths in (("Input", inputs), ("Output", outputs)):
        if paths:
            log.info("[%s] %s: %s", name, label, ", ".join(
                str(path.relative_to(c.WORK_DIR)) if path.is_relative_to(c.WORK_DIR) else str(path)
                for path in paths))
    thread = threading.Thread(target=sampler, name="resource-sampler", daemon=True)
    try:
        memory_connection = connection.cursor()
        log.debug("[%s] Memory sampling SQL: SELECT sum(memory_usage_bytes) FROM duckdb_memory()", name)
        if c.TRACE_PYTHON_MEMORY:
            if not tracemalloc.is_tracing():
                tracemalloc.start(1)
                tracing_started = True
            row["python_trace_scope"] = "stage_allocations" if tracing_started else "existing_tracer_sampled_during_stage"
        psutil.cpu_percent(interval=None)
        sample()
        thread.start()
        for path in inputs:
            if not path.is_file():
                raise FileNotFoundError("Missing input: {}. Run the preceding stage first.".format(path))
        details = function(connection)
        row.update(details)
        statistics = " | ".join("{}: {}".format(key.replace("_", " "),
                                format(value, ",.2f") if isinstance(value, float) else value)
                                for key, value in details.items()
                                if key not in ("input_rows", "output_rows") and not isinstance(value, (dict, list)))
        if statistics:
            log.info("[%s] %s", name, statistics)
        row["status"] = "success"
    except BaseException as error:
        row.update(status="failed", error="{}: {}".format(type(error).__name__, error))
        log.exception("✗ [%s] Stage failed", name)
        raise
    finally:
        stop.set()
        if thread.ident is not None:
            thread.join()
        try:
            sample()
        except Exception:
            log.exception("[%s] Final resource snapshot failed", name)
        finally:
            if tracing_started:
                tracemalloc.stop()
            if memory_connection is not None:
                memory_connection.close()
        cpu_end = process.cpu_times()
        elapsed = time.perf_counter() - start
        cpu_seconds = cpu_end.user + cpu_end.system - cpu_start.user - cpu_start.system
        stage_cpu_percent = 100 * cpu_seconds / elapsed if elapsed else 0
        # The complete stage is also a measured CPU interval, including short stages.
        peaks["process_cpu"] = max(peaks["process_cpu"], stage_cpu_percent)
        row.update(ended_at=timestamp(), elapsed_seconds=elapsed,
                   rows_per_second=row["input_rows"] / elapsed if elapsed else 0,
                   output_bytes=file_size(outputs), peak_process_rss_bytes=peaks["rss"],
                   peak_temp_bytes=peaks["temp"], peak_system_ram_percent=peaks["system_ram"],
                   system_cpu_percent_sample_mean=peaks["cpu_sum"] / peaks["samples"] if peaks["samples"] else 0,
                   process_cpu_percent=stage_cpu_percent,
                   peak_process_thread_count=peaks["threads"], peak_active_process_threads=peaks["active_threads"],
                   peak_process_cpu_percent=peaks["process_cpu"],
                   peak_cpu_cores_used=peaks["process_cpu"] / 100,
                   peak_configured_thread_capacity_percent=peaks["process_cpu"] / c.THREADS,
                   peak_process_rss_vs_budget_percent=100 * peaks["rss"] / budget if budget else None,
                   min_available_system_ram_bytes=minimums["available_ram"], min_disk_free_bytes=minimums["free_disk"],
                   resource_sample_count=peaks["samples"])
        if samples:
            row.update(samples[0])
        row["average_cpu_cores_used"] = row["process_cpu_percent"] / 100
        row["average_configured_thread_capacity_percent"] = row["process_cpu_percent"] / c.THREADS
        row["process_machine_cpu_percent"] = row["process_cpu_percent"] / (row["logical_cpus"] or 1)
        row["process_rss_vs_budget_percent"] = 100 * row["process_rss_bytes"] / budget if budget and samples else None
        row.update(peak_duckdb_managed_memory_bytes=memory_peaks["duckdb"],
                   peak_python_traced_bytes=memory_peaks["python"],
                   duckdb_managed_vs_budget_percent=100 * row["duckdb_managed_memory_bytes"] / budget
                   if budget and row["duckdb_managed_memory_bytes"] is not None else None,
                   peak_duckdb_managed_vs_budget_percent=100 * memory_peaks["duckdb"] / budget
                   if budget and memory_peaks["duckdb"] is not None else None)
        reports.append(row)
        log.info("%s [%s] %s | %.3fs | %s -> %s rows | %s rows/sec",
                 "✓" if row["status"] == "success" else "✗", name, row["status"],
                 elapsed, row["input_rows"], row["output_rows"], format(row["rows_per_second"], ",.0f"))
        log.debug("[%s] Full measured statistics (MB): %s", name, json.dumps(mb_statistics(row), ensure_ascii=False))
        if samples:
            log.info("[%s] Memory | DuckDB budget: %s | process used (RSS): %.1f MB | peak used: %.1f MB",
                     name, memory_label(budget), row["process_rss_bytes"] / 1_000_000, peaks["rss"] / 1_000_000)
            log.info("[%s] Memory ratio | peak process RSS / DuckDB budget: %s", name,
                     "{:.2f}%".format(row["peak_process_rss_vs_budget_percent"]) if budget else "n/a")
            log.info("[%s] DuckDB tracked | current: %s | observed peak: %s | peak / budget: %s", name,
                     memory_label(row["duckdb_managed_memory_bytes"]), memory_label(memory_peaks["duckdb"]),
                     "{:.2f}%".format(row["peak_duckdb_managed_vs_budget_percent"])
                     if row["peak_duckdb_managed_vs_budget_percent"] is not None else "n/a")
            log.info("[%s] Python traced | current: %s | peak: %s | scope: %s", name,
                     memory_label(row["python_traced_current_bytes"]), memory_label(memory_peaks["python"]),
                     row["python_trace_scope"])
            log.info("[%s] Threads | DuckDB configured: %s | peak process total: %s | peak observed active: %s",
                     name, c.THREADS, peaks["threads"], peaks["active_threads"])
            log.info("[%s] CPU capacity | average: %.2f cores (%.1f%% of configured threads) | peak sampled: %.2f cores (%.1f%%)",
                     name, row["average_cpu_cores_used"], row["average_configured_thread_capacity_percent"],
                     row["peak_cpu_cores_used"], row["peak_configured_thread_capacity_percent"])
            log.info("[%s] System RAM | total: %.1f MB | used: %.1f MB (%.1f%%) | available: %.1f MB",
                     name, row["total_system_ram_bytes"] / 1_000_000, row["used_system_ram_bytes"] / 1_000_000,
                     row["system_ram_percent"], row["available_system_ram_bytes"] / 1_000_000)
            log.info("[%s] CPU | process: %.1f%% | system sample mean: %.1f%%", name,
                     row["process_cpu_percent"], row["system_cpu_percent_sample_mean"])
        log.info("[%s] Disk | input: %.1f MB | output: %.1f MB | peak temp: %.1f MB | free: %.1f MB",
                 name, row["input_bytes"] / 1_000_000, row["output_bytes"] / 1_000_000,
                 peaks["temp"] / 1_000_000, row.get("disk_free_bytes", 0) / 1_000_000)
        if peaks["temp"]:
            log.info("[%s] Temporary files observed; DuckDB used its configured spill directory", name)


def write_reports(metadata, stages):
    """One CSV: stage rows plus one overall run row. No duplicate report formats."""
    def maximum(key):
        return max((s[key] for s in stages if s.get(key) is not None), default=None)

    def minimum(key):
        return min((s[key] for s in stages if s.get(key) is not None), default=None)

    by_stage = {stage["stage"]: stage for stage in stages}
    measured_seconds = sum(stage["elapsed_seconds"] for stage in stages)
    average_cpu = sum(stage["process_cpu_percent"] * stage["elapsed_seconds"] for stage in stages) / measured_seconds if measured_seconds else 0
    overall = dict(stage="run", status=metadata["run_status"], started_at=metadata["started_at"],
                   ended_at=metadata["ended_at"], elapsed_seconds=metadata["overall_elapsed_seconds"],
                   input_rows=metadata["test_dataset_rows"],
                   output_rows=stages[-1]["output_rows"] if stages and metadata["run_status"] == "success" else None,
                   peak_process_rss_bytes=maximum("peak_process_rss_bytes"),
                   peak_duckdb_managed_memory_bytes=maximum("peak_duckdb_managed_memory_bytes"),
                   peak_python_traced_bytes=maximum("peak_python_traced_bytes"),
                   peak_duckdb_managed_vs_budget_percent=maximum("peak_duckdb_managed_vs_budget_percent"),
                   peak_process_rss_vs_budget_percent=maximum("peak_process_rss_vs_budget_percent"),
                   peak_process_thread_count=maximum("peak_process_thread_count"),
                   peak_active_process_threads=maximum("peak_active_process_threads"),
                   average_configured_thread_capacity_percent=average_cpu / c.THREADS if c.THREADS > 0 else None,
                   peak_configured_thread_capacity_percent=maximum("peak_configured_thread_capacity_percent"),
                   peak_system_ram_percent=maximum("peak_system_ram_percent"),
                   min_available_system_ram_bytes=minimum("min_available_system_ram_bytes"),
                   min_disk_free_bytes=minimum("min_disk_free_bytes"), peak_temp_bytes=maximum("peak_temp_bytes"),
                   error=metadata.get("error", ""))
    overall["rows_per_second"] = (metadata["test_dataset_rows"] or 0) / metadata["overall_elapsed_seconds"] if metadata["overall_elapsed_seconds"] else 0
    for stage_name in ("generation", "validation", "matching", "gold"):
        for key in ("reference_rows", "rejected_rows", "matched_rows", "unmatched_rows", "ambiguous_rows", "new_masters_created"):
            if key in by_stage.get(stage_name, {}):
                overall[key] = by_stage[stage_name][key]

    def mb(value):
        return round(value / 1_000_000, 3) if value is not None else None

    rows = []
    size_fields = {
        "input_mb": "input_bytes", "output_mb": "output_bytes",
        "peak_process_ram_mb": "peak_process_rss_bytes",
        "peak_duckdb_tracked_mb": "peak_duckdb_managed_memory_bytes",
        "peak_python_traced_mb": "peak_python_traced_bytes",
        "peak_temp_mb": "peak_temp_bytes", "min_available_system_ram_mb": "min_available_system_ram_bytes",
        "min_free_disk_mb": "min_disk_free_bytes",
    }
    counters = ("input_rows", "output_rows", "reference_rows", "rejected_rows", "matched_rows", "unmatched_rows", "ambiguous_rows", "new_masters_created",
                "rows_per_second", "peak_duckdb_managed_vs_budget_percent", "peak_process_rss_vs_budget_percent",
                "peak_process_thread_count", "peak_active_process_threads", "average_configured_thread_capacity_percent",
                "peak_configured_thread_capacity_percent", "peak_system_ram_percent")
    for item in stages + [overall]:
        is_run = item["stage"] == "run"
        row = dict(run_id=metadata["run_id"], command=metadata["command"], dataset_rows=metadata["test_dataset_rows"],
                   stage=item["stage"], status=item["status"], started_at=item["started_at"], ended_at=item["ended_at"],
                   seconds=round(item["elapsed_seconds"], 3), configured_threads=c.THREADS,
                   ram_budget_mb=mb(memory_budget_bytes()))
        row.update({key: round(item[key], 3) if isinstance(item.get(key), float) else item.get(key) for key in counters})
        row.update({target: mb(item.get(source)) for target, source in size_fields.items()})
        row.update(raw_file_mb=mb(file_size([c.RAW])) if is_run else None,
                   reference_file_mb=mb(file_size([c.REFERENCE])) if is_run else None,
                   retained_parquet_mb=mb(sum(p.stat().st_size for p in (c.WORK_DIR / "data").rglob("*.parquet"))) if is_run else None,
                   system_ram_mb=mb(metadata["total_system_ram_bytes"]), cpu_model=metadata["cpu_model"],
                   logical_cpus=metadata["logical_cpus"], python_version=metadata["python_version"],
                   duckdb_version=metadata["duckdb_version"], match_rule=c.MATCH_RULE,
                   python_tracing=c.TRACE_PYTHON_MEMORY, sample_interval_seconds=c.SAMPLE_INTERVAL,
                   run_folder=str(c.WORK_DIR), error=item.get("error", ""))
        rows.append(row)
    report = c.STATS_FILE
    report.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not report.exists() or report.stat().st_size == 0
    with report.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        if header_needed:
            writer.writeheader()
        writer.writerows(rows)
    log.info("Run %s | %s | %.3fs | raw %s MB | reference %s MB | retained Parquet %s MB",
             metadata["run_id"], metadata["run_status"], metadata["overall_elapsed_seconds"],
             rows[-1]["raw_file_mb"], rows[-1]["reference_file_mb"], rows[-1]["retained_parquet_mb"])
    log.info("Statistics: %s", report)
