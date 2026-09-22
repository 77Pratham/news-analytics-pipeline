"""
Streamlit dashboard. Talks to the FastAPI service only -- no direct DB
access from here, so the dashboard stays a thin client over the API.
"""
import os
from datetime import date

import pandas as pd
import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(page_title="News Analytics Pipeline v2", layout="wide")
st.title("News analytics pipeline")

tab_trends, tab_articles, tab_digest, tab_search = st.tabs(
    ["Topic trends", "Articles", "Daily digest", "Semantic search"]
)

with tab_trends:
    st.subheader("Topics by article count")
    try:
        trends = requests.get(f"{API_URL}/trends", timeout=10).json()
        if trends:
            df = pd.DataFrame(trends)
            st.bar_chart(df.set_index("label")["article_count"])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No clustered topics yet -- run the pipeline first.")
    except requests.RequestException as exc:
        st.error(f"Could not reach API: {exc}")

with tab_articles:
    st.subheader("Recent articles")
    col1, col2 = st.columns(2)
    sentiment_filter = col1.selectbox("Sentiment", [None, "positive", "neutral", "negative"])
    limit = col2.slider("How many", 10, 200, 50)

    params = {"limit": limit}
    if sentiment_filter:
        params["sentiment"] = sentiment_filter

    try:
        articles = requests.get(f"{API_URL}/articles", params=params, timeout=10).json()
        st.dataframe(pd.DataFrame(articles), use_container_width=True)
    except requests.RequestException as exc:
        st.error(f"Could not reach API: {exc}")

with tab_digest:
    st.subheader("Daily digest")
    selected_date = st.date_input("Date", value=date.today())
    try:
        resp = requests.get(f"{API_URL}/digest", params={"digest_date": str(selected_date)}, timeout=10)
        if resp.status_code == 404:
            st.info("No digest generated for this date yet.")
        else:
            for row in resp.json():
                with st.container(border=True):
                    st.markdown(f"**{row['label']}** ({row['article_count']} articles)")
                    st.write(row["summary"])
    except requests.RequestException as exc:
        st.error(f"Could not reach API: {exc}")

with tab_search:
    st.subheader("Semantic search")
    query = st.text_input("Search articles by meaning, not just keywords")
    if query:
        try:
            results = requests.get(f"{API_URL}/search", params={"q": query}, timeout=15).json()
            for r in results:
                st.markdown(f"[{r['title']}]({r['url']}) — {r['source']} (similarity {r['similarity']:.2f})")
        except requests.RequestException as exc:
            st.error(f"Could not reach API: {exc}")
