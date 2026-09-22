"""
Sentiment scoring.

Deliberately kept lightweight (VADER, a lexicon/rule-based scorer) rather
than a transformer -- news headlines are short and VADER is fast enough to
run per-article inline in the Airflow task without a GPU or an extra model
download. Swap in a transformer pipeline later if the accuracy matters more
than throughput.
"""
from __future__ import annotations

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()


def score_sentiment(text: str) -> tuple[str, float]:
    """Returns (label, compound_score) where label is one of
    'positive' / 'neutral' / 'negative'."""
    if not text:
        return "neutral", 0.0

    scores = _analyzer.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return label, compound
