from ingestion.fetch_news import RawArticle

def test_url_hash_deterministic():
    a1 = RawArticle(source="s", url="https://x.com/a", title="t", body="b", published_at=None)
    a2 = RawArticle(source="s2", url="https://x.com/a", title="t2", body="b2", published_at=None)
    assert a1.url_hash == a2.url_hash

def test_url_hash_differs():
    a1 = RawArticle(source="s", url="https://x.com/a", title="t", body="b", published_at=None)
    a2 = RawArticle(source="s", url="https://x.com/b", title="t", body="b", published_at=None)
    assert a1.url_hash != a2.url_hash
