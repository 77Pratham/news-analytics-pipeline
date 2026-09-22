"""
Quality gate, run right after ingestion and before enrichment.

Uses Great Expectations' pandas dataset API to validate a batch of newly
ingested rows. Anything that fails is written to quarantined_articles with
the specific failed expectations attached (as JSON) so a human can inspect
*why* it was rejected -- rows are never silently dropped.
"""
from __future__ import annotations

import json
import logging

import great_expectations as ge
import pandas as pd
from psycopg2.extras import Json, execute_values

logger = logging.getLogger(__name__)

EXPECTATION_SUITE = [
    # (expectation method name, kwargs)
    ("expect_column_values_to_not_be_null", {"column": "title"}),
    ("expect_column_value_lengths_to_be_between", {"column": "title", "min_value": 5, "max_value": 500}),
    ("expect_column_values_to_not_be_null", {"column": "url"}),
    ("expect_column_values_to_match_regex", {"column": "url", "regex": r"^https?://"}),
    ("expect_column_value_lengths_to_be_between", {"column": "body", "min_value": 30, "max_value": None}),
]


def fetch_unvalidated_batch(conn, limit: int = 500) -> pd.DataFrame:
    """Rows that are in raw_articles but not yet in articles or quarantined_articles."""
    query = """
        SELECT r.id, r.source, r.url, r.title, r.body, r.published_at
        FROM raw_articles r
        LEFT JOIN articles a ON a.id = r.id
        LEFT JOIN quarantined_articles q ON q.raw_article_id = r.id
        WHERE a.id IS NULL AND q.raw_article_id IS NULL
        ORDER BY r.id
        LIMIT %s
    """
    return pd.read_sql(query, conn, params=(limit,))


def validate_batch(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[int, list[str]]]:
    """Returns (passing_df, {row_id: [failed_expectation_names]})."""
    if df.empty:
        return df, {}

    gdf = ge.from_pandas(df)
    failures: dict[int, set[str]] = {}

    for expectation_name, kwargs in EXPECTATION_SUITE:
        result = getattr(gdf, expectation_name)(**kwargs, result_format="COMPLETE")
        if result["success"]:
            continue
        unexpected_index_list = result["result"].get("unexpected_index_list", [])
        for idx in unexpected_index_list:
            row_id = int(df.loc[idx, "id"])
            failures.setdefault(row_id, set()).add(expectation_name)

    failing_ids = set(failures.keys())
    passing_df = df[~df["id"].isin(failing_ids)]
    return passing_df, {k: sorted(v) for k, v in failures.items()}


def quarantine_failures(conn, failures: dict[int, list[str]]) -> int:
    if not failures:
        return 0
    rows = [(raw_id, Json({"failed_checks": checks})) for raw_id, checks in failures.items()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO quarantined_articles (raw_article_id, failed_checks)
            VALUES %s
            """,
            rows,
        )
    conn.commit()
    logger.warning("Quarantined %d articles that failed validation", len(rows))
    return len(rows)


def run_quality_gate(conn, limit: int = 500) -> pd.DataFrame:
    """Fetches the unvalidated batch, quarantines failures, returns the
    passing rows ready for the enrichment stage."""
    batch = fetch_unvalidated_batch(conn, limit=limit)
    passing_df, failures = validate_batch(batch)
    quarantine_failures(conn, failures)
    logger.info(
        "Quality gate: %d in, %d passed, %d quarantined",
        len(batch), len(passing_df), len(failures),
    )
    return passing_df
