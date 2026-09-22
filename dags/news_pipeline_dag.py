"""
Main DAG for the news analytics pipeline.

Stages: ingest -> quality gate -> enrich (sentiment + NER + embeddings +
dedup) -> cluster topics -> generate daily digest.

IMPORTANT (native/non-Docker setup): Airflow lives in its own venv
(~/airflow-venv) and never imports torch/spaCy/sentence-transformers
directly -- those heavy ML libraries live in a SEPARATE venv
(~/news-pipeline-venv) to avoid a dependency conflict between Airflow's
pinned typing-extensions and the newer pydantic/torch stack.

Each task runs via ExternalPythonOperator, which executes the callable
as a subprocess using the interpreter at EXTERNAL_PYTHON below, instead
of importing anything into Airflow's own process. Because of that, every
import the task needs must happen INSIDE the function body, not at the
top of this file -- the subprocess starts fresh and doesn't inherit this
module's global imports.

Update EXTERNAL_PYTHON to match your actual venv path.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

# --- EDIT THIS: absolute path to the python inside your ML/enrichment venv ---
EXTERNAL_PYTHON = os.environ.get(
    "PIPELINE_EXTERNAL_PYTHON",
    "/home/pratham_r/news-pipeline-venv/bin/python",
)

# Absolute path to the project root, so the subprocess can find enrichment/,
# ingestion/, quality/ as importable packages even though it starts fresh.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

default_args = {
    "owner": "pratham",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_ingest(**context):
    import sys
    import os as _os
    project_root = _os.environ.get(
        "PIPELINE_PROJECT_ROOT",
        "/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2",
    )
    sys.path.append(project_root)
    import psycopg2
    from ingestion.fetch_news import run_ingestion

    dsn = _os.environ.get(
        "PIPELINE_DB_DSN",
        "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
    )
    conn = psycopg2.connect(dsn)
    try:
        inserted = run_ingestion(conn, dag_run_id=context["run_id"])
        print(f"Ingested {inserted} new articles")
        return inserted
    finally:
        conn.close()


def task_quality_gate(**context):
    import sys
    import os as _os
    project_root = _os.environ.get(
        "PIPELINE_PROJECT_ROOT",
        "/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2",
    )
    sys.path.append(project_root)
    import psycopg2
    from quality.ge_checks import run_quality_gate

    dsn = _os.environ.get(
        "PIPELINE_DB_DSN",
        "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
    )
    conn = psycopg2.connect(dsn)
    try:
        passing_df = run_quality_gate(conn)
        print(f"Quality gate: {len(passing_df)} articles passed")
        return len(passing_df)
    finally:
        conn.close()


def task_enrich(**context):
    import sys
    import os as _os
    project_root = _os.environ.get(
        "PIPELINE_PROJECT_ROOT",
        "/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2",
    )
    sys.path.append(project_root)
    import json
    import psycopg2
    import pandas as pd
    from enrichment.embeddings import embed_texts, find_near_duplicate
    from enrichment.ner import extract_entities
    from enrichment.sentiment import score_sentiment

    dsn = _os.environ.get(
        "PIPELINE_DB_DSN",
        "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
    )
    conn = psycopg2.connect(dsn)
    try:
        # An article is eligible for enrichment if quality_gate did NOT
        # quarantine it, and it hasn't already been enriched in a prior run.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.source, r.url, r.title, r.body, r.published_at
                FROM raw_articles r
                WHERE NOT EXISTS (SELECT 1 FROM quarantined_articles q WHERE q.raw_article_id = r.id)
                  AND NOT EXISTS (SELECT 1 FROM articles a WHERE a.id = r.id)
                """
            )
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=["id", "source", "url", "title", "body", "published_at"])
        if df.empty:
            print("No articles passed quality gate, nothing to enrich")
            return

        texts = (df["title"].fillna("") + ". " + df["body"].fillna("")).tolist()
        embeddings = embed_texts(texts)

        inserted, deduped = 0, 0
        with conn.cursor() as cur:
            for i, row in df.iterrows():
                embedding = embeddings[i]
                dup_id = find_near_duplicate(conn, embedding)
                if dup_id is not None:
                    deduped += 1
                    continue

                label, score = score_sentiment(row["title"])
                entities = extract_entities(row["title"] + " " + (row["body"] or ""))
                vector_literal = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"

                cur.execute(
                    """
                    INSERT INTO articles
                        (id, source, url, title, body, published_at,
                         sentiment_label, sentiment_score, entities, embedding)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        int(row["id"]), row["source"], row["url"], row["title"], row["body"],
                        row["published_at"], label, score,
                        json.dumps(entities), vector_literal,
                    ),
                )
                inserted += 1
        conn.commit()
        print(f"Enrichment: {inserted} inserted, {deduped} skipped as near-duplicates")
    finally:
        conn.close()


def task_cluster_topics(**context):
    import sys
    import os as _os
    project_root = _os.environ.get(
        "PIPELINE_PROJECT_ROOT",
        "/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2",
    )
    sys.path.append(project_root)
    import psycopg2
    import pandas as pd
    import numpy as np
    from pgvector.psycopg2 import register_vector
    from enrichment.topics import cluster_topics, label_clusters

    dsn = _os.environ.get(
        "PIPELINE_DB_DSN",
        "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
    )
    conn = psycopg2.connect(dsn)
    register_vector(conn)  # without this, psycopg2 returns vector columns as raw text strings
    try:
        df = pd.read_sql(
            "SELECT id, title, embedding FROM articles WHERE topic_id IS NULL AND embedding IS NOT NULL",
            conn,
        )
        if df.empty:
            print("No new articles to cluster")
            return

        embeddings = np.array([np.asarray(e, dtype=float) for e in df["embedding"]])
        # min_cluster_size lowered from the module default (5) to 3 -- with only
        # a few dozen articles from a handful of RSS feeds, 5+ similar articles
        # rarely exist yet. Raise this back toward 5+ once article volume grows
        # from repeated scheduled runs.
        cluster_ids = cluster_topics(embeddings, min_cluster_size=3)
        labels = label_clusters(df["title"].tolist(), cluster_ids)

        with conn.cursor() as cur:
            for article_id, cluster_id in zip(df["id"], cluster_ids):
                cur.execute(
                    "UPDATE articles SET topic_id = %s WHERE id = %s",
                    (int(cluster_id), int(article_id)),
                )
            for cluster_id, label in labels.items():
                count = int((cluster_ids == cluster_id).sum())
                cur.execute(
                    """
                    INSERT INTO topics (topic_id, label, article_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (topic_id) DO UPDATE
                        SET label = EXCLUDED.label, article_count = EXCLUDED.article_count, computed_at = now()
                    """,
                    (int(cluster_id), label, count),
                )
        conn.commit()
        print(f"Clustered {len(df)} articles into {len(labels)} topics")
    finally:
        conn.close()


def task_generate_digest(**context):
    import sys
    import os as _os
    project_root = _os.environ.get(
        "PIPELINE_PROJECT_ROOT",
        "/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2",
    )
    sys.path.append(project_root)
    import psycopg2
    import pandas as pd
    from enrichment.digest import summarize_cluster

    dsn = _os.environ.get(
        "PIPELINE_DB_DSN",
        "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
    )
    conn = psycopg2.connect(dsn)
    try:
        # Widened from "= current_date" to a 3-day window: this project
        # accumulates a backlog rather than only ever processing same-day
        # articles, so a strict same-day filter was excluding real clustered
        # topics whose articles happened to be a day or two old.
        df = pd.read_sql(
            """
            SELECT a.topic_id, t.label, a.title
            FROM articles a
            JOIN topics t ON t.topic_id = a.topic_id
            WHERE a.published_at >= current_date - interval '3 days'
              AND a.topic_id != -1
            """,
            conn,
        )
        if df.empty:
            print("No articles today to digest")
            return

        with conn.cursor() as cur:
            for topic_id, group in df.groupby("topic_id"):
                label = group["label"].iloc[0]
                summary = summarize_cluster(label, group["title"].tolist())
                if not summary:
                    continue
                cur.execute(
                    """
                    INSERT INTO daily_digests (digest_date, topic_id, summary, article_count)
                    VALUES (current_date, %s, %s, %s)
                    ON CONFLICT (digest_date, topic_id) DO UPDATE
                        SET summary = EXCLUDED.summary, article_count = EXCLUDED.article_count, generated_at = now()
                    """,
                    (int(topic_id), summary, len(group)),
                )
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="news_analytics_pipeline_v2",
    default_args=default_args,
    description="Ingest -> quality gate -> NLP enrichment -> topic clustering -> daily digest",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["news", "nlp", "portfolio"],
) as dag:

    ingest = ExternalPythonOperator(
        task_id="ingest", python=EXTERNAL_PYTHON, python_callable=task_ingest
    )
    quality_gate = ExternalPythonOperator(
        task_id="quality_gate", python=EXTERNAL_PYTHON, python_callable=task_quality_gate
    )
    enrich = ExternalPythonOperator(
        task_id="enrich", python=EXTERNAL_PYTHON, python_callable=task_enrich
    )
    cluster = ExternalPythonOperator(
        task_id="cluster_topics", python=EXTERNAL_PYTHON, python_callable=task_cluster_topics
    )
    digest = ExternalPythonOperator(
        task_id="generate_digest", python=EXTERNAL_PYTHON, python_callable=task_generate_digest
    )

    ingest >> quality_gate >> enrich >> cluster >> digest
