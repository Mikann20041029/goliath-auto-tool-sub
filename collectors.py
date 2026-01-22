import os
import re
import time
import datetime
from typing import List, Dict, Any

import requests

UA = "goliath-collector/1.0"
TIMEOUT = 20

DEFAULT_QUERIES = [
    "how do i", "how to", "error", "issue", "problem", "can't", "doesn't work",
    "convert", "calculator", "compare", "template", "timezone", "subscription",
]


def _days_ago_ts(days: int) -> int:
    dt = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    return int(dt.timestamp())


def _dedup(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for it in items:
        text = (it.get("text") or "").strip()
        url = (it.get("url") or "").strip()
        if not text or not url:
            continue
        key = url + "|" + text[:160]
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def collect_hn(queries: List[str], days_back: int, limit_per_query: int) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    min_ts = _days_ago_ts(days_back)

    out: List[Dict[str, str]] = []
    api = "https://hn.algolia.com/api/v1/search_by_date"

    for q in queries:
        params = {
            "query": q,
            "tags": "(story,comment)",
            "numericFilters": f"created_at_i>{min_ts}",
            "hitsPerPage": str(limit_per_query),
        }
        try:
            r = session.get(api, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for h in (data.get("hits") or []):
            title = (h.get("title") or "").strip()
            story_title = (h.get("story_title") or "").strip()
            comment_text = (h.get("comment_text") or "").strip()
            text = title or story_title or comment_text
            if not text:
                continue

            object_id = h.get("objectID")
            if not object_id:
                continue

            hn_url = f"https://news.ycombinator.com/item?id={object_id}"
            out.append({"text": text, "url": hn_url, "platform": "hn"})

        time.sleep(0.2)

    return _dedup(out)


def collect_bluesky(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    base = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    out: List[Dict[str, str]] = []

    for q in queries:
        params = {"q": q, "limit": str(limit_per_query)}
        try:
            r = session.get(base, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for p in (data.get("posts") or []):
            record = p.get("record") or {}
            text = str(record.get("text") or "").strip()
            if not text:
                continue

            uri = p.get("uri") or ""
            author = p.get("author") or {}
            handle = author.get("handle") or ""
            rkey = ""
            if uri and "/app.bsky.feed.post/" in uri:
                rkey = uri.split("/app.bsky.feed.post/")[-1]

            if handle and rkey:
                url = f"https://bsky.app/profile/{handle}/post/{rkey}"
            else:
                url = uri or "https://bsky.app/"

            out.append({"text": text, "url": url, "platform": "bluesky"})

        time.sleep(0.2)

    return _dedup(out)


def collect_mastodon(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    api_base = (os.getenv("MASTODON_API_BASE") or "").strip().rstrip("/")
    token = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()
    if not api_base or not token:
        return []

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Authorization": f"Bearer {token}"})

    out: List[Dict[str, str]] = []

    for q in queries:
        url = f"{api_base}/api/v2/search"
        params = {"q": q, "type": "statuses", "limit": str(limit_per_query), "resolve": "false"}
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for s in (data.get("statuses") or []):
            content = (s.get("content") or "").strip()
            if not content:
                continue

            txt = content.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
            txt = re.sub(r"<[^>]+>", "", txt).strip()
            if not txt:
                continue

            url2 = (s.get("url") or "").strip()
            if not url2:
                continue

            out.append({"text": txt, "url": url2, "platform": "mastodon"})

        time.sleep(0.2)

    return _dedup(out)


def collect_x(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    bearer = (os.getenv("X_BEARER_TOKEN") or "").strip()
    if not bearer:
        return []

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Authorization": f"Bearer {bearer}"})

    out: List[Dict[str, str]] = []
    api = "https://api.x.com/2/tweets/search/recent"

    for q in queries:
        params = {"query": q, "max_results": str(min(limit_per_query, 100)), "tweet.fields": "created_at"}
        try:
            r = session.get(api, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for t in (data.get("data") or []):
            tid = t.get("id")
            text = (t.get("text") or "").strip()
            if not tid or not text:
                continue
            out.append({"text": text, "url": f"https://x.com/i/web/status/{tid}", "platform": "x"})

        time.sleep(0.2)

    return _dedup(out)


def collect_items(days_back: int = 365, total_limit: int = 60, per_query: int = 15) -> List[Dict[str, str]]:
    """
    返す形式:
      [{"text": "...problem...", "url": "https://...", "platform": "hn|bluesky|mastodon|x"}, ...]
    設定:
      - COLLECT_SOURCES = "hn,bluesky,mastodon,x"
      - COLLECT_QUERIES = "how to,error,issue,can't"
      - X_BEARER_TOKEN が無ければ X は自動スキップ
      - Mastodon は MASTODON_API_BASE / MASTODON_ACCESS_TOKEN が無ければ自動スキップ
    """
    srcs = (os.getenv("COLLECT_SOURCES") or "hn,bluesky,mastodon,x").lower().split(",")
    srcs = [s.strip() for s in srcs if s.strip()]

    qenv = (os.getenv("COLLECT_QUERIES") or "").strip()
    queries = [x.strip() for x in qenv.split(",") if x.strip()] if qenv else DEFAULT_QUERIES

    items: List[Dict[str, str]] = []
    if "hn" in srcs:
        items += collect_hn(queries, days_back=days_back, limit_per_query=per_query)
    if "bluesky" in srcs:
        items += collect_bluesky(queries, limit_per_query=per_query)
    if "mastodon" in srcs:
        items += collect_mastodon(queries, limit_per_query=per_query)
    if "x" in srcs:
        items += collect_x(queries, limit_per_query=per_query)

    items = _dedup(items)
    return items[:total_limit]
