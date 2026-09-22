from enrichment.sentiment import score_sentiment

def test_positive():
    label, score = score_sentiment("Scientists celebrate breakthrough cure, community overjoyed")
    assert label == "positive"
    assert score > 0

def test_negative():
    label, score = score_sentiment("Disaster kills dozens, officials warn of worsening crisis")
    assert label == "negative"
    assert score < 0

def test_empty():
    label, score = score_sentiment("")
    assert label == "neutral"
    assert score == 0.0
