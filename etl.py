"""Readable SQL over Parquet. Only scalar counts are brought into Python."""
import logging

import config as c

log = logging.getLogger("etl")


def execute(connection, sql):
    log.debug("SQL operation:\n%s", sql)
    try:
        return connection.execute(sql)
    except Exception:
        log.exception("SQL operation failed:\n%s", sql)
        raise


def count(connection, path):
    return execute(connection, "SELECT count(*) FROM " + c.parquet(path)).fetchone()[0]


def write_parquet(connection, query, path):
    """Replace one output only after COPY succeeds; never append on a rerun."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.stem + ".partial.parquet")
    try:
        execute(connection, "COPY ({}) TO {} (FORMAT PARQUET, COMPRESSION SNAPPY)".format(
            query, c.sql_string(temporary)))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_query(source, id_column="customer_id"):
    # The same expressions normalize Silver and external reference attributes.
    return r"""
        SELECT {id_column},
               upper(trim(regexp_replace(customer_name, '\s+', ' ', 'g'))) AS customer_name,
               lower(trim(email)) AS email,
               regexp_replace(phone, '[^0-9]', '', 'g') AS phone,
               upper(trim(regexp_replace(city, '\s+', ' ', 'g'))) AS city,
               upper(trim(regexp_replace(country, '\s+', ' ', 'g'))) AS country,
               trim(source_system) AS source_system, updated_date
        FROM {source}
    """.format(id_column=id_column, source=source)


def raw(connection):
    rows = count(connection, c.RAW)
    columns = len(execute(connection, "SELECT * FROM " + c.parquet(c.RAW) + " LIMIT 0").description)
    return dict(input_rows=rows, output_rows=rows, column_count=columns)


def validation(connection):
    execute(connection, r"""
        CREATE OR REPLACE TEMP VIEW checked AS
        WITH duplicates AS (
            SELECT *, count(*) OVER (PARTITION BY customer_id) AS id_count
            FROM {raw}
        )
        SELECT *, concat_ws('; ',
            CASE WHEN customer_id IS NULL THEN 'missing_customer_id' END,
            CASE WHEN customer_name IS NULL OR trim(regexp_replace(customer_name, '\s+', ' ', 'g')) = ''
                 THEN 'missing_customer_name' END,
            CASE WHEN email IS NULL OR NOT regexp_full_match(trim(email), '[^\s@]+@[^\s@.]+\.[^\s@]+')
                 THEN 'invalid_email' END,
            CASE WHEN phone IS NULL OR NOT regexp_full_match(regexp_replace(phone, '[^0-9]', '', 'g'), '[0-9]{{10,15}}')
                 THEN 'invalid_phone' END,
            CASE WHEN customer_id IS NOT NULL AND id_count > 1 THEN 'duplicate_customer_id' END
        ) AS rejection_reason
        FROM duplicates
    """.format(raw=c.parquet(c.RAW)))
    # Reject ALL occurrences of duplicate IDs; no arbitrary record survives.
    write_parquet(connection, "SELECT * EXCLUDE (id_count) FROM checked WHERE rejection_reason <> ''", c.REJECTED)
    write_parquet(connection, "SELECT * EXCLUDE (id_count, rejection_reason) FROM checked WHERE rejection_reason = ''", c.VALID)
    stats = execute(connection, """
        SELECT count(*), count(*) FILTER (WHERE customer_id IS NULL),
               count(*) FILTER (WHERE customer_name IS NULL),
               count(*) FILTER (WHERE email IS NULL), count(*) FILTER (WHERE phone IS NULL),
               count(*) FILTER (WHERE city IS NULL), count(*) FILTER (WHERE country IS NULL),
               count(*) FILTER (WHERE source_system IS NULL), count(*) FILTER (WHERE updated_date IS NULL),
               count(*) FILTER (WHERE customer_id IS NOT NULL AND id_count > 1),
               count(DISTINCT customer_id) FILTER (WHERE customer_id IS NOT NULL AND id_count > 1)
        FROM checked
    """).fetchone()
    valid, rejected = count(connection, c.VALID), count(connection, c.REJECTED)
    if stats[0] != valid + rejected:
        raise ValueError("Validation counts do not reconcile")
    return dict(input_rows=stats[0], output_rows=valid, rejected_rows=rejected,
                null_counts=dict(zip(("customer_id", "customer_name", "email", "phone", "city", "country", "source_system", "updated_date"), stats[1:9])),
                duplicate_rows=stats[9], duplicate_ids=stats[10],
                validation_percent=100 * valid / stats[0] if stats[0] else 0)


def transformation(connection):
    rows = count(connection, c.VALID)
    write_parquet(connection, normalize_query(c.parquet(c.VALID)), c.SILVER)
    output = count(connection, c.SILVER)
    if rows != output:
        raise ValueError("Transformation changed row count")
    return dict(input_rows=rows, output_rows=output)


def reference_view(connection):
    execute(connection, "CREATE OR REPLACE TEMP VIEW reference_clean AS " +
            normalize_query(c.parquet(c.REFERENCE), "master_id"))
    total, unique, missing = execute(connection, """
        SELECT count(*), count(DISTINCT master_id),
               count(*) FILTER (WHERE master_id IS NULL OR master_id < 1)
        FROM reference_clean
    """).fetchone()
    if missing or unique != total:
        raise ValueError("Reference master_id must be positive, non-null and unique")
    return total


def matching_keys():
    return ["email"] if c.MATCH_RULE == "email" else ["customer_name", "phone"]


def matching(connection):
    reference_rows = reference_view(connection)
    keys = matching_keys()
    key_list = ", ".join(keys)
    usable = " AND ".join("{0} IS NOT NULL AND {0} <> ''".format(key) for key in keys)
    execute(connection, """
        CREATE OR REPLACE TEMP VIEW reference_keys AS
        SELECT {keys}, count(*) AS reference_count, min(master_id) AS candidate_master_id
        FROM reference_clean WHERE {usable} GROUP BY {keys}
    """.format(keys=key_list, usable=usable))
    duplicate_keys = execute(connection, "SELECT count(*) FROM reference_keys WHERE reference_count > 1").fetchone()[0]
    if duplicate_keys:
        log.warning("⚠ %s duplicate reference keys will require review", duplicate_keys)
    join = " AND ".join("s.{0} = r.{0}".format(key) for key in keys)
    write_parquet(connection, """
        SELECT s.*, coalesce(r.reference_count, 0) AS reference_count,
               CASE WHEN r.reference_count = 1 THEN r.candidate_master_id END AS matched_master_id,
               CASE WHEN r.reference_count > 1 THEN 'ambiguous'
                    WHEN r.reference_count = 1 THEN 'matched' ELSE 'unmatched' END AS match_status
        FROM {silver} s LEFT JOIN reference_keys r ON {join}
    """.format(silver=c.parquet(c.SILVER), join=join), c.CLASSIFIED)
    counts = {}
    for status, path in (("matched", c.MATCHED), ("unmatched", c.UNMATCHED), ("ambiguous", c.AMBIGUOUS)):
        write_parquet(connection, "SELECT * FROM {} WHERE match_status = '{}'".format(c.parquet(c.CLASSIFIED), status), path)
        counts[status + "_rows"] = count(connection, path)
    rows = count(connection, c.SILVER)
    if rows != sum(counts.values()):
        raise ValueError("Matching counts do not reconcile")
    return dict(input_rows=rows, output_rows=rows, reference_rows=reference_rows,
                duplicate_reference_keys=duplicate_keys, match_rule=c.MATCH_RULE,
                match_percent=100 * counts["matched_rows"] / rows if rows else 0, **counts)


def gold(connection):
    reference_rows = reference_view(connection)
    # A standalone gold run requires all three matching outputs from a successful matching run.
    matched, unmatched, ambiguous = (count(connection, path) for path in (c.MATCHED, c.UNMATCHED, c.AMBIGUOUS))
    keys = ", ".join(matching_keys())
    attributes = "customer_name, email, phone, city, country, source_system, updated_date"
    write_parquet(connection, """
        WITH existing_candidates AS (
            SELECT master_id, {attributes}, 0 AS priority, 0::BIGINT AS customer_id FROM reference_clean
            UNION ALL
            SELECT matched_master_id AS master_id, {attributes}, 1 AS priority, customer_id FROM {matched}
        ), existing_winners AS (
            SELECT master_id, {attributes} FROM existing_candidates
            QUALIFY row_number() OVER (
                PARTITION BY master_id ORDER BY updated_date DESC NULLS LAST, priority, customer_id
            ) = 1
        ), new_winners AS (
            SELECT * FROM {unmatched}
            QUALIFY row_number() OVER (
                PARTITION BY {keys} ORDER BY updated_date DESC NULLS LAST, customer_id
            ) = 1
        ), new_masters AS (
            SELECT (SELECT coalesce(max(master_id), 0) FROM reference_clean)
                   + row_number() OVER (ORDER BY customer_id) AS master_id, {attributes}
            FROM new_winners
        )
        SELECT * FROM existing_winners UNION ALL SELECT * FROM new_masters
    """.format(attributes=attributes, matched=c.parquet(c.MATCHED), unmatched=c.parquet(c.UNMATCHED), keys=keys), c.GOLD)
    write_parquet(connection, "SELECT * FROM " + c.parquet(c.AMBIGUOUS), c.REVIEW)
    total, unique = execute(connection, "SELECT count(*), count(DISTINCT master_id) FROM " + c.parquet(c.GOLD)).fetchone()
    if total != unique or total < reference_rows:
        raise ValueError("Gold master counts do not reconcile")
    return dict(input_rows=matched + unmatched + ambiguous, output_rows=total,
                existing_masters_retained=reference_rows, new_masters_created=total - reference_rows,
                ambiguous_excluded=ambiguous, unmatched_rows_consolidated=unmatched - (total - reference_rows))


def get_stages():
    # Resolve paths after the CLI has selected its dataset size.
    return {
        "raw": (raw, [c.RAW], []),
        "validation": (validation, [c.RAW], [c.VALID, c.REJECTED]),
        "transformation": (transformation, [c.VALID], [c.SILVER]),
        "matching": (matching, [c.SILVER, c.REFERENCE], [c.CLASSIFIED, c.MATCHED, c.UNMATCHED, c.AMBIGUOUS]),
        "gold": (gold, [c.REFERENCE, c.MATCHED, c.UNMATCHED, c.AMBIGUOUS], [c.GOLD, c.REVIEW]),
    }
