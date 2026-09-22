"""
Ingestion stage.

Pulls articles from NewsAPI and a configurable list of RSS feeds, normalizes
them into a common shape, and upserts into raw_articles. Dedup is handled at
the database level via a UNIQUE constraint on url_hash -- this function is
safe to re-run (idempotent), which is what lets the Airflow DAG do backfills
without creating duplicate rows.
"""
from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import feedparser
import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
NEWSAPI_URL = "https://newsapi.org/v2/top-headlines"

DEFAULT_RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.reuters.com/rssFeed/worldNews",
    "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
]


@dataclass
class RawArticle:
    source: str
    url: str
    title: str
    body: str
    published_at: datetime | None

    @property
    def url_hash(self) -> str:
        return hashlib.sha256(self.url.encode("utf-8")).hexdigest()


def fetch_from_newsapi(query: str = "", page_size: int = 100) -> list[RawArticle]:
    if not NEWSAPI_KEY:
        logger.warning("NEWSAPI_KEY not set, skipping NewsAPI fetch")
        return []

    params = {
        "apiKey": NEWSAPI_KEY,
        "language": "en",
        "pageSize": page_size,
    }
    if query:
        params["q"] = query
    else:
        params["country"] = "us"

    resp = requests.get(NEWSAPI_URL, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    articles = []
    for item in payload.get("articles", []):
        published = _parse_datetime(item.get("publishedAt"))
        articles.append(
            RawArticle(
                source=item.get("source", {}).get("name", "newsapi"),
                url=item["url"],
                title=item.get("title") or "",
                body=item.get("content") or item.get("description") or "",
                published_at=published,
            )
        )
    return articles


def fetch_from_rss(feed_urls: list[str] | None = None) -> list[RawArticle]:
    feed_urls = feed_urls or DEFAULT_RSS_FEEDS
    articles: list[RawArticle] = []
    for feed_url in feed_urls:
        try:
            parsed = feedparser.parse(feed_url)
        except Exception as exc:  # noqa: BLE001 - one bad feed shouldn't kill the run
            logger.warning("Failed to parse RSS feed %s: %s", feed_url, exc)
            continue

        source_name = parsed.feed.get("title", feed_url)
        for entry in parsed.entries:
            published = None
            if getattr(entry, "published_parsed", None):
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            articles.append(
                RawArticle(
                    source=source_name,
                    url=entry.link,
                    title=entry.get("title", ""),
                    body=entry.get("summary", ""),
                    published_at=published,
                )
            )
    return articles


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def upsert_raw_articles(conn, articles: list[RawArticle], dag_run_id: str = "") -> int:
    """Insert articles, skipping ones already seen (same url_hash). Returns
    the number of NEW rows inserted."""
    if not articles:
        return 0

    rows = [
        (
            a.source,
            a.url,
            a.url_hash,
            a.title,
            a.body,
            a.published_at,
            dag_run_id,
        )
        for a in articles
    ]

    with conn.cursor() as cur:
        result = execute_values(
            cur,
            """
            INSERT INTO raw_articles (source, url, url_hash, title, body, published_at, dag_run_id)
            VALUES %s
            ON CONFLICT (url_hash) DO NOTHING
            RETURNING id
            """,
            rows,
            fetch=True,
        )
    conn.commit()
    return len(result)


def run_ingestion(conn, dag_run_id: str = "", newsapi_query: str = "") -> int:
    articles = fetch_from_newsapi(newsapi_query) + fetch_from_rss()
    inserted = upsert_raw_articles(conn, articles, dag_run_id=dag_run_id)
    logger.info("Ingestion: fetched %d, inserted %d new rows", len(articles), inserted)
    return inserted
