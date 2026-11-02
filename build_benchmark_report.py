"""Build a readable, offline HTML report from the common benchmark stats.csv."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent
IST = timezone(timedelta(hours=5, minutes=30))
STAGE_NAMES = {
    "generation": "Generate test data", "raw": "Read input",
    "validation": "Check data quality", "transformation": "Clean and standardize",
    "matching": "Match customers", "gold": "Build master data",
}


def number(row, key):
    value = row.get(key, "") if row else ""
    return float(value) if value not in ("", None) else None


def compact(value):
    if value is None:
        return "—"
    for scale, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1000, "k")):
        if abs(value) >= scale:
            return "{:.1f}".format(value / scale).rstrip("0").rstrip(".") + suffix
    return "{:,.0f}".format(value)


def count(value):
    if value is None:
        return "—"
    return '<span title="{:,.0f} records">{}</span>'.format(value, compact(value))


def mb(value):
    if value is None:
        return "—"
    if 0 < value < 0.1:
        return "&lt;0.1 MB"
    return ("{:.1f}".format(value) if 0 < value < 100 else "{:,.0f}".format(value)) + " MB"


def percent(value):
    if value is None:
        return "—"
    if 0 < value < 0.1:
        return "&lt;0.1%"
    return "{:.1f}%".format(value)


def duration(value):
    if value is None:
        return "—"
    if value < 10:
        return "{:.1f}s".format(value)
    seconds = round(value)
    if seconds < 60:
        return "{}s".format(seconds)
    minutes, seconds = divmod(seconds, 60)
    return "{}m {}s".format(minutes, seconds)


def local_time(value):
    if not value:
        return "Time unavailable"
    return datetime.fromisoformat(value).astimezone(IST).strftime("%d %b %Y, %I:%M %p IST")


def select_batches(rows):
    """Latest full pipeline per size, with the generation that preceded it."""
    run_rows = [row for row in rows if row["stage"] == "run"]
    sizes = sorted({int(row["dataset_rows"]) for row in run_rows if row.get("dataset_rows")})
    batches = []
    for size in sizes:
        candidates = [r for r in run_rows if int(r.get("dataset_rows") or 0) == size]
        pipelines = [r for r in candidates if r.get("command") in ("all", "generate_and_etl")]
        generations = [r for r in candidates if r.get("command") == "generate"]
        pipeline = max(pipelines, key=lambda r: r["started_at"], default=None)
        newest_generation = max(generations, key=lambda r: r["started_at"], default=None)
        if newest_generation and (pipeline is None or newest_generation["started_at"] > pipeline["started_at"]):
            pipeline = None  # New input has not yet been processed; don't show stale success.
        if pipeline is None:
            if newest_generation:
                batches.append(dict(size=size, pipeline=None, generation=newest_generation, stages=[]))
            continue
        matching_generations = [r for r in generations if r["run_folder"] == pipeline["run_folder"]
                                and r["ended_at"] <= pipeline["started_at"]
                                and r.get("reference_rows") == pipeline.get("reference_rows")]
        generation = max(matching_generations, key=lambda r: r["started_at"], default=None)
        stages = [r for r in rows if r["run_id"] == pipeline["run_id"] and r["stage"] != "run"]
        if generation:
            stages = [r for r in rows if r["run_id"] == generation["run_id"] and r["stage"] != "run"] + stages
        batches.append(dict(size=size, pipeline=pipeline, generation=generation, stages=stages))
    return batches


def state(batch):
    pipeline = batch["pipeline"]
    if pipeline:
        return ("passed", "Completed") if pipeline["status"] == "success" else ("failed", "Failed")
    if batch["generation"]["status"] != "success":
        return "failed", "Generation failed"
    return "pending", "Awaiting ETL"


def processing_seconds(batch):
    row = batch["pipeline"]
    if not row:
        return None
    if row["command"] == "all":
        return number(row, "seconds")
    return sum(number(s, "seconds") or 0 for s in batch["stages"] if s["stage"] != "generation")


def generation_seconds(batch):
    if batch["generation"]:
        return number(batch["generation"], "seconds")
    return next((number(s, "seconds") for s in batch["stages"] if s["stage"] == "generation"), None)


def metric(label, value, detail=""):
    return '<div class="metric"><span>{}</span><strong>{}</strong>{}</div>'.format(
        escape(label), value, '<small>{}</small>'.format(escape(detail)) if detail else "")


def overview(batch):
    row = batch["pipeline"]
    status, label = state(batch)
    seconds = processing_seconds(batch)
    speed = None
    if seconds and row["status"] == "success":
        speed = number(row, "rows_per_second") if row["command"] == "all" else batch["size"] / seconds
    return '<tr><td><a href="#batch-{size}">{size_text} rows</a></td><td>{time}</td><td>{speed}</td><td>{ram}</td><td>{budget}</td><td>{disk}</td><td><span class="status {status}">{label}</span></td></tr>'.format(
        size=batch["size"], size_text=compact(batch["size"]), time=duration(seconds),
        speed=(compact(speed) + "/s") if speed is not None else "—",
        ram=mb(number(row, "peak_process_ram_mb")), budget=percent(number(row, "peak_duckdb_managed_vs_budget_percent")),
        disk=mb(number(row, "retained_parquet_mb")), status=status, label=label)


def batch_html(batch, output_folder):
    row = batch["pipeline"]
    source = row or batch["generation"]
    status, label = state(batch)
    seconds = processing_seconds(batch)
    generation_time = generation_seconds(batch)
    budget = number(source, "ram_budget_mb")
    generation_time_text = duration(generation_time)
    if row and row["command"] == "generate_and_etl":
        total = number(row, "seconds")
    else:
        total = seconds + generation_time if seconds is not None and generation_time is not None else None
    timing = metric("Generate data", generation_time_text) + metric("Run pipeline", duration(seconds)) + metric("Combined time", duration(total), "Excludes waiting between commands")
    outcome_html = '<p class="muted">Run the pipeline for this size to see record outcomes.</p>'
    if row:
        outcomes = [("Existing match", "matched_rows", "matched"), ("No existing match", "unmatched_rows", "new"),
                    ("Needs review", "ambiguous_rows", "review"), ("Rejected", "rejected_rows", "rejected")]
        segments, labels = [], []
        for title, key, color in outcomes:
            value = number(row, key)
            share = value / batch["size"] * 100 if value is not None else None
            if share is not None:
                segments.append('<span class="{}" style="width:{:.6f}%" title="{}: {:,.0f} records"></span>'.format(color, share, title, value))
            labels.append('<div><span class="dot {}"></span><span>{}</span><strong>{}</strong><small>{} of input</small></div>'.format(color, title, count(value), percent(share)))
        outcome_html = '<div class="outcome-bar" role="img" aria-label="Input records split into matched, unmatched, review and rejected">{}</div><div class="outcome-labels">{}</div>'.format("".join(segments), "".join(labels))
    resource_html = "".join([
        metric("Peak process RAM", mb(number(row, "peak_process_ram_mb")), "Python + DuckDB + other process memory"),
        metric("Peak DuckDB tracked", mb(number(row, "peak_duckdb_tracked_mb")), "Observed buffer-manager allocations"),
        metric("Peak Python traced", mb(number(row, "peak_python_traced_mb")), "Allocations traced during each stage"),
        metric("Peak temporary disk", mb(number(row, "peak_temp_mb")), "Sampled spill files"),
    ])
    budget_percent = number(row, "peak_duckdb_managed_vs_budget_percent")
    fill = max(0, min(budget_percent or 0, 100))
    stage_rows = []
    for stage in batch["stages"]:
        stage_rows.append('<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
            STAGE_NAMES.get(stage["stage"], escape(stage["stage"])), duration(number(stage, "seconds")),
            count(number(stage, "input_rows")), count(number(stage, "output_rows")),
            mb(number(stage, "peak_process_ram_mb")), mb(number(stage, "peak_duckdb_tracked_mb"))))
    file_metrics = "".join([
        metric("Input file", mb(number(source, "raw_file_mb"))),
        metric("Reference file", mb(number(source, "reference_file_mb"))),
        metric("All saved Parquet", mb(number(row, "retained_parquet_mb")), "Includes input, intermediate and final files"),
        metric("Master output", count(number(row, "output_rows")), "Existing masters + newly resolved customers"),
    ])
    thread_info = '<p class="fine">DuckDB thread limit: <b>{}</b> · Peak total process threads: <b>{}</b> · Peak observed active threads: <b>{}</b> · Average CPU capacity used: <b>{}</b></p>'.format(
        compact(number(source, "configured_threads")), compact(number(row, "peak_process_thread_count")),
        compact(number(row, "peak_active_process_threads")), percent(number(row, "average_configured_thread_capacity_percent")))
    thread_info += '<p class="fine">System RAM peak: <b>{}</b> · Lowest available RAM: <b>{}</b> · Lowest free disk space: <b>{}</b></p>'.format(
        percent(number(row, "peak_system_ram_percent")), mb(number(row, "min_available_system_ram_mb")),
        mb(number(row, "min_free_disk_mb")))
    links = []
    for name, entry in (("Generation log", batch["generation"]), ("Pipeline log", row)):
        if entry:
            log = Path(entry["run_folder"]) / "logs" / (entry["run_id"] + ".log")
            if log.is_file() and log.is_relative_to(output_folder):
                links.append('<a href="{}">{}</a>'.format(quote(log.relative_to(output_folder).as_posix()), name))
    error = '<p class="error">{}</p>'.format(escape(row.get("error", ""))) if row and row.get("error") else ""
    return '''<article class="batch" id="batch-{size}">
      <div class="batch-heading"><div><p class="eyebrow">BATCH · {size_text} INPUT RECORDS</p><h2>{size_text} input / {reference} reference</h2><p class="muted">{when}</p></div><span class="status {status}">{label}</span></div>
      {error}<div class="metrics timing">{timing}</div>
      <h3>Where the records went</h3>{outcomes}
      <h3>Memory and temporary storage</h3><div class="metrics resources">{resources}</div>
      <div class="budget-label"><span>Peak DuckDB tracked memory / {budget} budget</span><strong>{budget_percent}</strong></div>
      <div class="budget-track"><span style="width:{fill}%"></span></div>
      {threads}
      <h3>Data stored and master output</h3><div class="metrics files">{files}</div>
      <details><summary>View stage timings and memory</summary><div class="table-scroll"><table><thead><tr><th>Stage</th><th>Time</th><th>Input rows</th><th>Output rows</th><th>Peak process RAM</th><th>Peak DuckDB tracked</th></tr></thead><tbody>{stage_rows}</tbody></table></div></details>
      <div class="log-links">{links}</div>
    </article>'''.format(size=batch["size"], size_text=compact(batch["size"]), reference=compact(number(source, "reference_rows")),
        when=escape(local_time(source.get("ended_at"))), status=status, label=label, error=error, timing=timing,
        outcomes=outcome_html, resources=resource_html, budget=mb(budget), budget_percent=percent(budget_percent), fill=fill,
        threads=thread_info, files=file_metrics, stage_rows="".join(stage_rows), links=" · ".join(links))


STYLE = '''
:root{--navy:#142e47;--ink:#183047;--muted:#5e7081;--line:#dce5ed;--teal:#087e83;--blue:#4c83c4;--amber:#c59031;--rose:#bf6b76}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#eff3f7;color:var(--ink);font:15px/1.55 'Segoe UI',Arial,sans-serif}
.page{max-width:1220px;margin:32px auto;padding:0 24px}header{background:var(--navy);border-radius:16px;color:white;padding:36px 38px}.eyebrow{font-size:11px;letter-spacing:.12em;font-weight:700;margin:0 0 8px}header .eyebrow{color:#a0dedc}h1{font-size:34px;letter-spacing:-.025em;line-height:1.2;margin:0 0 12px}header p{margin:0;color:#d5e3ed;max-width:790px}h2{font-size:24px;line-height:1.3;margin:0 0 6px}h3{font-size:15px;margin:26px 0 12px}.muted,.fine{color:var(--muted)}.muted{margin:0;font-size:13px}.fine{font-size:12px}
.toolbar{display:flex;justify-content:space-between;align-items:center;gap:15px;flex-wrap:wrap;margin:18px 0}.toolbar a,.toolbar button{display:inline-block;color:var(--ink);background:white;border:1px solid var(--line);padding:8px 14px;border-radius:7px;font:inherit;text-decoration:none;cursor:pointer}.toolbar .caption{font-size:12px;color:var(--muted)}
.headline{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:20px 0}.headline .metric{background:white;border:1px solid var(--line);border-radius:10px;padding:18px 20px}.metric>span{display:block;font-size:12px;color:var(--muted)}.metric>strong{display:block;font-size:25px;line-height:1.3;margin:6px 0;font-weight:650;font-variant-numeric:tabular-nums}.metric small{display:block;color:var(--muted);font-size:11px;line-height:1.4}
.comparison,.batch,.notes{background:white;border:1px solid var(--line);border-radius:12px;padding:25px 28px;margin:20px 0}.comparison h2,.notes h2{font-size:19px}.table-scroll{overflow:auto}table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:13px}th{text-align:left;color:var(--muted);font-size:11px;font-weight:600;padding:12px 10px;background:#f7f9fb}td{padding:13px 10px;border-bottom:1px solid #e9eef3;font-variant-numeric:tabular-nums}tbody tr:last-child td{border-bottom:0}td:first-child,th:first-child{padding-left:0}table a{color:var(--teal);font-weight:650;text-decoration:none}.status{display:inline-block;font-size:11px;font-weight:650;padding:5px 10px;border-radius:20px;white-space:nowrap}.passed{background:#e9f6f0;color:#23654b}.failed{background:#fcecee;color:#a33c4b}.pending{background:#fff4df;color:#936815}
.batch-heading{display:flex;justify-content:space-between;align-items:flex-start;gap:20px}.batch .eyebrow{color:var(--teal)}.batch{scroll-margin-top:20px}.metrics{display:grid;gap:16px}.timing{grid-template-columns:repeat(3,1fr);background:#f5f8fb;border-radius:9px;padding:18px 20px;margin:22px 0}.timing .metric>strong{font-size:24px}.resources,.files{grid-template-columns:repeat(4,1fr)}.resources .metric,.files .metric{border-left:2px solid var(--line);padding-left:12px}.resources .metric>strong,.files .metric>strong{font-size:21px}
.outcome-bar{display:flex;height:12px;background:#edf1f4;border-radius:5px;overflow:hidden}.outcome-bar span{height:100%}.matched{background:var(--teal)}.new{background:var(--blue)}.review{background:var(--amber)}.rejected{background:var(--rose)}.outcome-labels{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}.outcome-labels>div{display:grid;grid-template-columns:10px 1fr;gap:2px 7px;align-items:center}.dot{width:7px;height:7px;border-radius:50%}.outcome-labels span:not(.dot){font-size:11px;color:var(--muted)}.outcome-labels strong{grid-column:2;font-size:22px;font-weight:650;line-height:1.3}.outcome-labels strong span{font-size:inherit!important;color:var(--ink)!important}.outcome-labels small{grid-column:2;font-size:11px;color:var(--muted)}
.budget-label{display:flex;justify-content:space-between;gap:15px;margin:21px 0 7px;font-size:12px;color:var(--muted)}.budget-label strong{color:var(--teal)}.budget-track{height:7px;border-radius:5px;background:#e9f0f4;overflow:hidden}.budget-track span{display:block;background:var(--teal);height:100%}details{margin-top:25px;border-top:1px solid var(--line);padding-top:14px}summary{cursor:pointer;color:var(--teal);font-size:13px;font-weight:600}details table{margin-top:12px}.log-links{font-size:12px;margin-top:14px}.log-links a{color:var(--teal)}.notes p{font-size:13px;color:var(--muted);margin:12px 0}.notes b{color:var(--ink)}.error{background:#fcecee;padding:12px;border-radius:6px;font-size:12px;overflow-wrap:anywhere}footer{font-size:11px;color:var(--muted);padding:4px 4px 24px}code{font-family:Consolas,monospace}
@media(max-width:760px){.page{margin:16px auto;padding:0 14px}header{padding:27px 24px}h1{font-size:28px}.headline,.resources,.files,.outcome-labels{grid-template-columns:repeat(2,1fr)}.comparison,.batch,.notes{padding:22px 19px}.metric>strong{font-size:23px}.timing{gap:10px;padding:15px}.timing .metric>strong{font-size:21px}.batch-heading h2{font-size:21px}}
@media(max-width:420px){.timing{grid-template-columns:1fr}.timing .metric{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap}.timing .metric small{width:100%}.headline{gap:9px}.headline .metric{padding:14px}.batch-heading{flex-wrap:wrap}.budget-label{font-size:11px}}
@media print{body{background:white}.page{max-width:none;margin:0;padding:0}.toolbar,.log-links{display:none}header{background:white;color:var(--ink);padding:15px 0;border-radius:0;border-bottom:2px solid var(--teal)}header p{color:var(--muted)}header .eyebrow{color:var(--teal)}.headline .metric,.comparison,.batch,.notes{box-shadow:none}.batch{break-before:page}.table-scroll{overflow:visible}table{white-space:normal}.metric{break-inside:avoid}*{print-color-adjust:exact;-webkit-print-color-adjust:exact}}
'''


def build(input_path, output_path):
    with input_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    batches = select_batches(rows)
    if not batches:
        raise ValueError("No generation or full pipeline results found in stats.csv")
    completed = [b for b in batches if b["pipeline"] and b["pipeline"]["status"] == "success"]
    largest = max(completed, key=lambda b: b["size"], default=None)
    largest_row = largest["pipeline"] if largest else None
    latest = max((b["pipeline"] or b["generation"])["ended_at"] for b in batches)
    headline = "".join([
        metric("Completed test sizes", "{} / {}".format(len(completed), len(batches))),
        metric("Largest completed input", count(largest["size"]) if largest else "—", "records"),
        metric("Largest batch: pipeline time", duration(processing_seconds(largest)) if largest else "—"),
        metric("Largest batch: peak process RAM", mb(number(largest_row, "peak_process_ram_mb"))),
    ])
    source_link = '<a href="{}" download>Source CSV</a>'.format(quote(input_path.name)) if input_path.parent == output_path.parent else ""
    document = '''<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Customer ETL · Batch Performance Report</title><style>{style}</style></head><body><main class="page">
      <header><p class="eyebrow">DUCKDB · CUSTOMER DATA · LOAD TEST</p><h1>Batch performance report</h1><p>A clear view of how each input size performed: speed, memory, stored data, and customer outcomes.</p></header>
      <div class="toolbar"><span class="caption">Latest results: {latest} · Latest full run per size; newer unprocessed input is marked pending.</span><div>{source_link} <button type="button" onclick="window.print()">Print / save PDF</button></div></div>
      <section class="headline" aria-label="Overall test summary">{headline}</section>
      <section class="comparison"><h2>Compare the batches</h2><p class="fine">Rounded values for readability. Click an input size for its breakdown. All storage and memory use decimal MB.</p><div class="table-scroll"><table><thead><tr><th>Input records</th><th>Pipeline time</th><th>Speed</th><th>Peak process RAM</th><th>DuckDB budget used</th><th>Saved data</th><th>Status</th></tr></thead><tbody>{overview}</tbody></table></div></section>
      {batch_cards}
      <section class="notes"><h2>How to read this report</h2>
      <p><b>Rounded counts:</b> k = thousand; M = million. Hover over a record count for its exact value. Earlier runs, including older reference sizes, remain in the source CSV; this page selects the latest results for each input size.</p>
      <p><b>Outcomes:</b> existing match, no existing match, review and rejected are shares of the original input. Master output retains all reference masters and adds resolved new customers, so it can exceed the input count.</p>
      <p><b>Memory:</b> process RAM covers Python, DuckDB and other resident process allocations. DuckDB tracked memory covers its buffer manager; Python traced memory covers allocations traced during a stage. These values do not add up to total process RAM. The configured budget is a limit, not reserved RAM.</p>
      <p><b>Threads and disk:</b> total/active process threads include Python and monitoring, not just DuckDB workers. CPU capacity is CPU core-equivalents divided by configured threads, so it can exceed 100%. Saved data includes all Parquet inputs, intermediates and outputs. Zero temporary disk means no spill was observed in the samples.</p>
      <p><b>Timing and sampling:</b> separate-command timings use their measured command durations; combined time adds generation and ETL without manual waiting. For a combined command, stage details separate the work and pipeline time sums the five ETL stages. Memory/thread peaks are observed samples and may miss short activity, especially in sub-second stages.</p></section>
      <footer>Source: {source_name} · Static snapshot; refresh with <code>python build_benchmark_report.py</code>. No ETL logic or source statistics are changed.</footer>
    </main></body></html>'''.format(style=STYLE, latest=escape(local_time(latest)), source_link=source_link,
        headline=headline, overview="".join(overview(b) for b in batches),
        batch_cards="".join(batch_html(b, output_path.parent) for b in batches), source_name=escape(input_path.name))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(document, encoding="utf-8")
    return batches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "stats.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "benchmark_report.html")
    args = parser.parse_args()
    batches = build(args.input.resolve(), args.output.resolve())
    print("Created {} with {} dataset sizes.".format(args.output, len(batches)))


if __name__ == "__main__":
    main()
