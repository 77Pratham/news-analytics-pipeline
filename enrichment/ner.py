"""
Named entity extraction with spaCy. Used to power an "entities trending
today" view -- which people, organizations, and places are showing up most
across the day's ingested articles.
"""
from __future__ import annotations

import spacy

_KEEP_LABELS = {"PERSON", "ORG", "GPE", "EVENT", "PRODUCT"}
_nlp = None


def get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm", disable=["parser", "lemmatizer"])
    return _nlp


def extract_entities(text: str) -> list[dict]:
    if not text:
        return []
    nlp = get_nlp()
    doc = nlp(text)
    seen = set()
    entities = []
    for ent in doc.ents:
        if ent.label_ not in _KEEP_LABELS:
            continue
        key = (ent.text.strip().lower(), ent.label_)
        if key in seen:
            continue
        seen.add(key)
        entities.append({"text": ent.text.strip(), "label": ent.label_})
    return entities
