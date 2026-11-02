"""Generate deterministic synthetic data directly from DuckDB range() to Parquet."""
import argparse
import math
import sys
import time

import config as c
from etl import count, write_parquet
from logger import print_summary, setup_logging
from monitoring import run_id, run_stage, system_info, timestamp, write_reports


def generate(connection, rows):
    if rows < 1:
        raise ValueError("rows must be at least 1")
    for name, rate in dict(c.QUALITY, match=c.MATCH_RATE, reference_duplicate=c.REFERENCE_DUPLICATE_RATE).items():
        if not 0 <= rate <= 1:
            raise ValueError("{} rate must be between 0 and 1".format(name))
    reference_rows = c.parse_rows(c.REFERENCE_ROWS)
    duplicate_reference_rows = min(math.floor(reference_rows * c.REFERENCE_DUPLICATE_RATE), reference_rows - 1)
    unique_reference_rows = reference_rows - duplicate_reference_rows
    intended_match_rows = math.floor(rows * c.MATCH_RATE)

    def issue(name, factor):
        # Integer arithmetic is repeatable; independent multipliers reduce correlation.
        return "((i * {factor} + {seed}) % 1000000) < {threshold}".format(
            factor=factor, seed=c.SEED, threshold=int(c.QUALITY[name] * 1_000_000))

    def identity_query(source, raw=False):
        # Record IDs stay separate from customer matching keys. Many input records
        # can refer to one existing master when input volume exceeds reference size.
        key = "contact_identity"
        return """
            SELECT {row_index} identity,
                   ['AARAV', 'MAYA', 'OLIVER', 'SOFIA', 'LIAM', 'ZARA'][1 + ({key} % 6)]
                   || ' ' || ['SHAH', 'SMITH', 'KHAN', 'BROWN'][1 + ({key} % 4)]
                   || ' ' || {key}::VARCHAR AS customer_name,
                   'customer' || {key}::VARCHAR || '@example.com' AS email,
                   '1' || lpad({key}::VARCHAR, 10, '0') AS phone,
                   ['MUMBAI', 'LONDON', 'TORONTO', 'SYDNEY'][1 + ({key} % 4)] AS city,
                   ['INDIA', 'UK', 'CANADA', 'AUSTRALIA'][1 + ({key} % 4)] AS country
            FROM {source}
        """.format(key=key, source=source, row_index="i," if raw else "")

    write_parquet(connection, """
        WITH reference_ids AS (
            SELECT i AS identity,
                   CASE WHEN i <= {unique_rows} THEN i
                        ELSE 1 + (((i - {unique_rows} - 1) * 141650939 + {seed} * 7) % {unique_rows})
                   END AS contact_identity
            FROM range(1, {reference_end}) t(i)
        ), identities AS ({identities})
            SELECT identity AS master_id, customer_name, email, phone, city, country,
                   'REFERENCE'::VARCHAR AS source_system, DATE '2023-01-01' AS updated_date
            FROM identities
    """.format(identities=identity_query("reference_ids"), reference_end=reference_rows + 1,
               unique_rows=unique_reference_rows, seed=c.SEED), c.REFERENCE)
    write_parquet(connection, """
        WITH row_ids AS (
            SELECT i, CASE WHEN i > 1 AND {duplicate} THEN i - 1 ELSE i END AS identity
            FROM range(1, {end}) t(i)
        ), contact_ids AS (
            SELECT *, CASE WHEN identity <= {intended_match_rows}
                           THEN 1 + ((identity - 1) % {unique_reference_rows})
                           ELSE {reference_rows} + identity END AS contact_identity
            FROM row_ids
        ), identities AS ({identities})
        SELECT CASE WHEN {null_id} THEN NULL ELSE identity END::BIGINT AS customer_id,
               CASE WHEN {null_name} THEN NULL
                    WHEN {spaces} THEN '  ' || d.customer_name || '  '
                    WHEN {mixed_case} THEN lower(d.customer_name) ELSE d.customer_name END AS customer_name,
               CASE WHEN {invalid_email} THEN 'invalid-email-' || i::VARCHAR
                    WHEN {spaces} THEN '  ' || upper(d.email) || '  ' ELSE d.email END AS email,
               CASE WHEN {missing_phone} THEN NULL
                    WHEN {spaces} THEN '+' || substr(d.phone, 1, 1) || ' ' || substr(d.phone, 2)
                    ELSE d.phone END AS phone,
               CASE WHEN {spaces} THEN ' ' || lower(city) || ' ' ELSE city END AS city,
               CASE WHEN {spaces} THEN ' ' || lower(country) || ' ' ELSE country END AS country,
               CASE WHEN i % 2 = 0 THEN 'CRM' ELSE 'WEB' END AS source_system,
               DATE '2024-01-01' + (i % 365)::INTEGER AS updated_date
        FROM identities d
    """.format(end=rows + 1, identities=identity_query("contact_ids", raw=True),
               intended_match_rows=intended_match_rows, unique_reference_rows=unique_reference_rows,
               reference_rows=reference_rows,
               duplicate=issue("duplicate", 15485863), null_id=issue("null", 32452843),
               null_name=issue("null", 49979687), spaces=issue("spaces", 67867967),
               mixed_case=issue("mixed_case", 86028121), invalid_email=issue("invalid_email", 104395301),
               missing_phone=issue("missing_phone", 122949829)), c.RAW)
    actual = count(connection, c.RAW)
    if actual != rows:
        raise ValueError("Generated row count differs from requested count")
    actual_reference_rows = count(connection, c.REFERENCE)
    if actual_reference_rows != reference_rows:
        raise ValueError("Generated reference count differs from REFERENCE_ROWS")
    return dict(input_rows=rows, output_rows=actual, reference_rows=actual_reference_rows,
                reference_unique_customers=unique_reference_rows, reference_duplicate_rows=duplicate_reference_rows,
                intended_match_percent=c.MATCH_RATE * 100, seed=c.SEED, quality_rates=c.QUALITY)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=c.parse_rows, default=c.ROWS)
    args = parser.parse_args()
    c.select_batch(args.rows)
    identifier = run_id()
    log = setup_logging(identifier)
    stages = []
    metadata = dict(system_info(), run_id=identifier, test_dataset_rows=args.rows,
                    started_at=timestamp(), run_status="failed", command="generate")
    start = time.perf_counter()
    connection = None
    exit_code = 0
    try:
        connection = c.connect()
        log.info("Selected dataset: %s rows | folder: %s", format(args.rows, ","), c.WORK_DIR)
        run_stage(connection, "generation", lambda con: generate(con, args.rows), [], [c.RAW, c.REFERENCE], stages)
        metadata["run_status"] = "success"
    except (Exception, KeyboardInterrupt) as error:
        metadata["error"] = "{}: {}".format(type(error).__name__, error)
        log.exception("Generation stopped")
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
