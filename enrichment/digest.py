"""
Daily digest generation.

For each topic cluster with enough articles, asks a local Ollama model for a
2-3 sentence summary of what happened. Same zero-API-cost pattern as the
Research Paper Intelligence System (local model via Ollama, no external LLM
bill) -- swap OLLAMA_MODEL for a hosted model (e.g. Groq/Llama) if you need
higher quality summaries and are fine paying per token.
"""
from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "phi3:mini")

PROMPT_TEMPLATE = """You are summarizing today's news for a topic cluster labeled "{label}".
Here are the headlines in this cluster:

{headlines}

Write a 2-3 sentence neutral summary of what's happening in this cluster. No preamble."""


def summarize_cluster(label: str, headlines: list[str]) -> str:
    if not headlines:
        return ""

    prompt = PROMPT_TEMPLATE.format(label=label, headlines="\n".join(f"- {h}" for h in headlines[:20]))

    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except requests.RequestException as exc:
        logger.warning("Digest generation failed for cluster '%s': %s", label, exc)
        return ""
