"""
FastAPI serving layer.

The Streamlit dashboard talks to this API instead of hitting Postgres
directly -- gives the project a real service boundary and means the same
data is reusable behind any other frontend later.
"""
from __future__ import annotations

from datetime import date

from fastapi import FastAPI, HTTPException, Query

from api.db import get_conn, get_dict_cursor
from enrichment.embeddings import embed_texts

app = FastAPI(
    title="News Analytics Pipeline API",
    description="Serves enriched articles, topic trends, and daily digests.",
    version="2.0.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/articles")
def list_articles(
    topic_id: int | None = None,
    sentiment: str | None = Query(None, pattern="^(positive|neutral|negative)$"),
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    conditions, params = [], []
    if topic_id is not None:
        conditions.append("topic_id = %s")
        params.append(topic_id)
    if sentiment is not None:
        conditions.append("sentiment_label = %s")
        params.append(sentiment)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([limit, offset])

    with get_conn() as conn, get_dict_cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT id, source, url, title, published_at, sentiment_label,
                   sentiment_score, entities, topic_id
            FROM articles
            {where_clause}
            ORDER BY published_at DESC
            LIMIT %s OFFSET %s
            """,
            params,
        )
        return cur.fetchall()


@app.get("/trends")
def get_trends(as_of: date | None = None):
    """Topic distribution + article counts, for the dashboard's trend chart."""
    with get_conn() as conn, get_dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT t.topic_id, t.label, t.article_count, t.computed_at
            FROM topics t
            WHERE t.topic_id != -1
            ORDER BY t.article_count DESC
            LIMIT 25
            """
        )
        return cur.fetchall()


@app.get("/digest")
def get_digest(digest_date: date = Query(default_factory=date.today)):
    with get_conn() as conn, get_dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT d.topic_id, t.label, d.summary, d.article_count
            FROM daily_digests d
            JOIN topics t ON t.topic_id = d.topic_id
            WHERE d.digest_date = %s
            ORDER BY d.article_count DESC
            """,
            (digest_date,),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No digest generated for {digest_date}")
    return rows


@app.get("/search")
def semantic_search(q: str, limit: int = Query(10, le=50)):
    """Semantic search over article embeddings via pgvector cosine distance."""
    embedding = embed_texts([q])[0]
    vector_literal = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"

    with get_conn() as conn, get_dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, title, url, source, published_at,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM articles
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vector_literal, vector_literal, limit),
        )
        return cur.fetchall()
