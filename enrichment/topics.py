"""
Topic clustering over a day's (or a batch's) articles.

Reuses the SBERT embeddings already computed for dedup/search and clusters
them with HDBSCAN, which -- unlike k-means -- doesn't require picking a
fixed number of topics in advance and naturally labels off-topic articles
as noise (cluster id -1) instead of forcing them into a bucket.
"""
from __future__ import annotations

from collections import Counter

import hdbscan
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS


def cluster_topics(embeddings: np.ndarray, min_cluster_size: int = 5) -> np.ndarray:
    """Returns a cluster-id array aligned with the input embeddings.
    -1 means "no clear topic" (HDBSCAN noise point)."""
    if len(embeddings) < min_cluster_size:
        return np.full(len(embeddings), -1)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric="euclidean")
    return clusterer.fit_predict(embeddings)


def label_clusters(titles: list[str], cluster_ids: np.ndarray, top_n_words: int = 3) -> dict[int, str]:
    """Cheap keyword-based label per cluster: most common non-stopword
    tokens across that cluster's titles. Good enough for a dashboard label;
    swap for an LLM-generated label (see digest.py) if you want something
    more readable."""
    labels: dict[int, str] = {}
    df = pd.DataFrame({"title": titles, "cluster": cluster_ids})

    for cluster_id, group in df.groupby("cluster"):
        if cluster_id == -1:
            labels[cluster_id] = "uncategorized"
            continue
        words = []
        for title in group["title"]:
            words.extend(
                w.lower().strip(".,!?\"'")
                for w in title.split()
                if w.lower() not in ENGLISH_STOP_WORDS and len(w) > 2
            )
        top_words = [w for w, _ in Counter(words).most_common(top_n_words)]
        labels[cluster_id] = ", ".join(top_words) if top_words else f"topic {cluster_id}"

    return labels
