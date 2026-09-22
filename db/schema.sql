-- News Analytics Pipeline v2 schema
-- Requires the pgvector extension (bundled in the ankane/pgvector docker image)

CREATE EXTENSION IF NOT EXISTS vector;

-- Raw landing zone: everything ingestion writes, before quality checks
CREATE TABLE IF NOT EXISTS raw_articles (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    url             TEXT NOT NULL,
    url_hash        TEXT NOT NULL UNIQUE,      -- sha256(url), used for dedup
    title           TEXT,
    body            TEXT,
    published_at    TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_run_id      TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_articles_published_at ON raw_articles (published_at);

-- Records that fail the Great Expectations gate land here instead of being
-- silently dropped or failing the whole DAG run.
CREATE TABLE IF NOT EXISTS quarantined_articles (
    id              BIGSERIAL PRIMARY KEY,
    raw_article_id  BIGINT REFERENCES raw_articles(id),
    failed_checks   JSONB NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Articles that passed validation, enriched with NLP outputs + embedding
CREATE TABLE IF NOT EXISTS articles (
    id              BIGINT PRIMARY KEY REFERENCES raw_articles(id),
    source          TEXT NOT NULL,
    url             TEXT NOT NULL,
    title           TEXT NOT NULL,
    body            TEXT,
    published_at    TIMESTAMPTZ,
    sentiment_label TEXT,           -- positive / neutral / negative
    sentiment_score REAL,
    entities        JSONB,          -- [{"text": "...", "label": "ORG"}, ...]
    topic_id        INTEGER,        -- HDBSCAN cluster id, -1 = noise
    embedding       vector(384),    -- all-MiniLM-L6-v2 output dim
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ANN index for semantic search / near-duplicate detection
CREATE INDEX IF NOT EXISTS idx_articles_embedding
    ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS topics (
    topic_id        INTEGER PRIMARY KEY,
    label           TEXT,           -- short human-readable label (top keywords)
    article_count   INTEGER,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_digests (
    digest_date     DATE NOT NULL,
    topic_id        INTEGER NOT NULL,
    summary         TEXT NOT NULL,
    article_count   INTEGER,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (digest_date, topic_id)
);
