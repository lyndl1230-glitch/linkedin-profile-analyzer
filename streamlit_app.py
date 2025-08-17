import os
import re
import csv
import io
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple

import requests
import streamlit as st
from dateutil import parser as dateparser

APIFY_ACTOR_ENDPOINT = "https://api.apify.com/v2/acts/apimaestro~linkedin-profile-posts/run-sync-get-dataset-items"


def get_apify_token() -> Optional[str]:
    try:
        token = st.secrets.get("APIFY_TOKEN")  # type: ignore[attr-defined]
    except Exception:
        token = None
    if not token:
        token = os.getenv("APIFY_TOKEN")
    return token


def extract_username(linkedin_url: str) -> str:
    m = re.search(r"linkedin\.com\/in\/([^\/?#]+)", linkedin_url)
    return m.group(1) if m else linkedin_url.strip()


def parse_post_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return dateparser.parse(str(value))
    except Exception:
        return None


def fetch_posts_bulk(username: str, token: str, start: datetime, end: datetime, target_total: int = 5000, per_page: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    def run_fetch(total_posts: int) -> List[Dict[str, Any]]:
        resp = requests.post(
            f"{APIFY_ACTOR_ENDPOINT}?token={token}",
            json={
                "username": username,
                "limit": per_page,         # items per page
                "total_posts": total_posts # ask actor to auto-paginate
            },
            timeout=300,
        )
        if not resp.ok:
            raise RuntimeError(f"Apify error {resp.status_code}: {resp.text}")
        data = resp.json()
        return data if isinstance(data, list) else []

    # First attempt
    all_items = run_fetch(target_total)
    # If we still didn't reach start date and items are non-empty, try a bigger cap once
    if all_items:
        parsed_dates = [parse_post_date(i.get("posted_at", {}).get("date")) for i in all_items]
        valid_dates = [d for d in parsed_dates if d]
        min_dt = min(valid_dates) if valid_dates else None
        if not min_dt or (min_dt and min_dt > start and len(all_items) >= min(target_total, 1000)):
            # Retry with higher ceiling
            all_items = run_fetch(max(10000, target_total * 2))

    # Filter by timeframe
    in_range = []
    for p in all_items:
        dt = parse_post_date(p.get("posted_at", {}).get("date"))
        if dt and start <= dt <= end:
            in_range.append(p)

    # Sort newest first for in-range
    in_range.sort(key=lambda p: parse_post_date(p.get("posted_at", {}).get("date")) or datetime.min, reverse=True)
    return all_items, in_range


def posts_to_csv(posts: List[Dict[str, Any]]) -> bytes:
    headers = [
        "Post Date",
        "posted_at/relative",
        "url",
        "Post Type",
        "Post Content",
        "total reactions",
        "like",
        "support",
        "love",
        "insightful",
        "celebrate",
        "comments",
        "reposts",
        "funny",
        "Media Type"
    ]

    def get_path(obj: Dict[str, Any], path: str) -> Any:
        cur: Any = obj
        for part in path.split('.'):
            if not isinstance(cur, dict) or part not in cur:
                return ""
            cur = cur[part]
        return cur

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    for p in posts:
        row = [
            get_path(p, "posted_at.date"),
            get_path(p, "posted_at.relative"),
            p.get("url", ""),
            p.get("post_type", ""),
            p.get("text", ""),
            get_path(p, "stats.total_reactions"),
            get_path(p, "stats.like"),
            get_path(p, "stats.support"),
            get_path(p, "stats.love"),
            get_path(p, "stats.insight"),
            get_path(p, "stats.celebrate"),
            get_path(p, "stats.comments"),
            get_path(p, "stats.reposts"),
            get_path(p, "stats.funny"),
            get_path(p, "media.type")
        ]
        writer.writerow(row)

    return buf.getvalue().encode("utf-8")


# UI
st.set_page_config(page_title="LinkedIn Report Exporter", page_icon="📄", layout="centered")
st.title("LinkedIn Report Exporter")

profile_url = st.text_input("LinkedIn profile URL", placeholder="https://www.linkedin.com/in/username")

# Advanced/tunable parameters
colA, colB, colC = st.columns([2, 1, 1])
with colA:
    token_input = st.text_input(
        "Apify API token (must enter)",
        value=(get_apify_token()),
        help="Leave empty to use the default token in this app."
    )
with colB:
    per_page_input = st.number_input(
        "Per page (limit)", min_value=1, max_value=100, value=100, step=1,
        help="Items per page for the actor (max 100)."
    )
with colC:
    target_total_input = st.number_input(
        "Target total (total_posts)", min_value=1, max_value=20000, value=100, step=100,
        help="Overall cap the actor will try to fetch across pages."
    )

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("From", value=date(2025, 1, 1))
with col2:
    end_date = st.date_input("To", value=date.today())

run = st.button("Generate CSV", type="primary")

if run:
    # Resolve token: prefer provided; fallback to default
    token = (token_input or "").strip()

    if not profile_url:
        st.error("Please enter a LinkedIn profile URL.")
        st.stop()

    try:
        username = extract_username(profile_url)
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time())

        with st.spinner("Fetching posts from Apify..."):
            all_items, posts = fetch_posts_bulk(
                username,
                token,
                start_dt,
                end_dt,
                target_total=int(target_total_input),
                per_page=int(per_page_input),
            )

        st.success(
            f"Retrieved {len(all_items)} total posts from Apify; {len(posts)} within selected range"
        )
        csv_bytes = posts_to_csv(posts)
        filename = f"linkedin_posts_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
        st.download_button("Download CSV", data=csv_bytes, file_name=filename, mime="text/csv")

        with st.expander("Preview JSON (first 3 in range)"):
            st.json(posts[:3])
    except Exception as e:
        st.error(f"Error: {e}") 
