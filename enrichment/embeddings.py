"""
Embeds article text with SBERT (all-MiniLM-L6-v2, 384-dim -- matches the
`vector(384)` column in schema.sql) and provides a near-duplicate check
against existing rows via pgvector cosine distance. This is the same
SBERT-based approach used in the audience-matching embedding pipeline,
reused here for a different purpose: dedup and semantic search instead of
audience-content matching.
"""
from __future__ import annotations

import logging

import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

_MODEL_NAME = "all-MiniLM-L6-v2"
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def embed_texts(texts: list[str]) -> np.ndarray:
    model = get_model()
    return model.encode(texts, show_progress_bar=False, normalize_embeddings=True)


def find_near_duplicate(conn, embedding: np.ndarray, threshold: float = 0.92) -> int | None:
    """Returns the id of an existing article whose embedding is within
    `threshold` cosine similarity, or None if this article looks novel.
    Cosine distance in pgvector is `1 - similarity`, so we filter on
    distance < (1 - threshold)."""
    max_distance = 1 - threshold
    vector_literal = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM articles
            WHERE embedding <=> %s::vector < %s
            ORDER BY embedding <=> %s::vector
            LIMIT 1
            """,
            (vector_literal, max_distance, vector_literal),
        )
        row = cur.fetchone()
    return row[0] if row else None
