import os
import time
import datetime
from typing import List, Dict, Any, Optional
import requests

UA = "goliath-collector/1.0 (+https://example.invalid)"
TIMEOUT = 20

DEFAULT_QUERIES = [
    # 悩みっぽい投稿が当たりやすい雑クエリ（あとであなたの方針で増やしてOK）
    "how do i",
    "how to",
    "error",
    "issue",
    "problem",
    "can't",
    "doesn't work",
    "convert",
    "calculator",
    "compare",
    "template",
    "timezone",
    "subscription",
]

def _now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def _days_ago_ts(days: int) -> int:
    dt = _now_utc() - datetime.timedelta(days=days)
    return int(dt.timestamp())

def _dedup(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for it in items:
        u = (it.get("url") or "").strip()
        t = (it.get("text") or "").strip()
        key = (u or "") + "|" + (t[:120] if t else "")
        if not u or not t:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def collect_hn(queries: List[str], days_back: int, limit_per_query: int = 20) -> List[Dict[str, str]]:
    """
    Hacker News(Algolia) から直近days_back日以内の投稿/コメントを収集。
    返す形式: [{"text": "...", "url": "...", "source":"hn", "created_at":"..."}]
    """
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    min_ts = _days_ago_ts(days_back)

    out: List[Dict[str, str]] = []
    for q in queries:
        # Algolia HN Search API: search_by_date を使用（新しい順）
        # numericFilters=created_at_i>min_ts で期間フィルタ
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {
            "query": q,
            "tags": "(story,comment)",
            "numericFilters": f"created_at_i>{min_ts}",
            "hitsPerPage": str(limit_per_query),
        }
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        hits = data.get("hits") or []
        for h in hits:
            created_at = h.get("created_at") or ""
            title = (h.get("title") or "").strip()
            story_title = (h.get("story_title") or "").strip()
            comment_text = (h.get("comment_text") or "").strip()
            # textは「タイトル優先、なければコメント」
            text = title or story_title or comment_text
            if not text:
                continue

            # URLをそれっぽく統一
            object_id = h.get("objectID")
            if not object_id:
                continue
            hn_url = f"https://news.ycombinator.com/item?id={object_id}"

            out.append({
                "text": text,
                "url": hn_url,
                "source": "hn",
                "created_at": created_at,
            })
        time.sleep(0.2)
    return _dedup(out)

def collect_bluesky(queries: List[str], days_back: int, limit_per_query: int = 20) -> List[Dict[str, str]]:
    """
    Bluesky: public API で searchPosts を叩いて収集（認証なし）。
    返す形式: [{"text": "...", "url": "...", "source":"bluesky", "created_at":"..."}]
    """
    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    out: List[Dict[str, str]] = []
    # public endpoint
    base = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    for q in queries:
        params = {"q": q, "limit": str(limit_per_query)}
        try:
            r = session.get(base, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        posts = data.get("posts") or []
        for p in posts:
            text = (p.get("record") or {}).get("text") or ""
            text = str(text).strip()
            if not text:
                continue

            uri = p.get("uri") or ""
            created_at = (p.get("record") or {}).get("createdAt") or ""

            # URIからブラウザURLを組み立て（標準の bsky.app プロフィール経由）
            # at://did:plc.../app.bsky.feed.post/xxxxx
            author = (p.get("author") or {})
            handle = author.get("handle") or ""
            rkey = ""
            if uri and "/app.bsky.feed.post/" in uri:
                rkey = uri.split("/app.bsky.feed.post/")[-1]
            if handle and rkey:
                url = f"https://bsky.app/profile/{handle}/post/{rkey}"
            else:
                # 取れない場合はURIをURL欄に入れる（最低限追跡可能にする）
                url = uri or "https://bsky.app/"

            out.append({
                "text": text,
                "url": url,
                "source": "bluesky",
                "created_at": created_at,
            })
        time.sleep(0.2)
    return _dedup(out)

def collect_mastodon(queries: List[str], limit_per_query: int = 20) -> List[Dict[str, str]]:
    """
    Mastodon: /api/v2/search を使って statuses を拾う。
    注意: インスタンスによっては検索にトークン必須。無ければ黙ってスキップ。
    """
    api_base = (os.getenv("MASTODON_API_BASE") or "").strip().rstrip("/")
    token = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()
    if not api_base or not token:
        return []

    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Authorization": f"Bearer {token}",
    })

    out: List[Dict[str, str]] = []
    for q in queries:
        url = f"{api_base}/api/v2/search"
        params = {
            "q": q,
            "type": "statuses",
            "limit": str(limit_per_query),
            "resolve": "false",
        }
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        statuses = data.get("statuses") or []
        for s in statuses:
            content = (s.get("content") or "").strip()
            # contentはHTMLなので雑にタグ除去（最低限）
            text = content.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
            # 超雑なタグ落とし
            import re
            text = re.sub(r"<[^>]+>", "", text).strip()
            if not text:
                continue

            url2 = (s.get("url") or "").strip()
            created_at = (s.get("created_at") or "").strip()
            if not url2:
                continue

            out.append({
                "text": text,
                "url": url2,
                "source": "mastodon",
                "created_at": created_at,
            })
        time.sleep(0.2)
    return _dedup(out)

def collect_x(queries: List[str], days_back: int, limit_per_query: int = 20) -> List[Dict[str, str]]:
    """
    X(Twitter) API v2 recent search。Bearerが無ければ黙ってスキップ。
    ※契約/権限不足だと403等が出るので、その場合も黙って空で返す。
    """
    bearer = (os.getenv("X_BEARER_TOKEN") or "").strip()
    if not bearer:
        return []
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Authorization": f"Bearer {bearer}"})

    out: List[Dict[str, str]] = []
    min_ts = _days_ago_ts(days_back)
    # v2 recent search は直近7日制限のことが多いので、days_backが大きくても実質はAPI側依存
    url = "https://api.x.com/2/tweets/search/recent"
    for q in queries:
        params = {
            "query": q,
            "max_results": str(min(limit_per_query, 100)),
            "tweet.fields": "created_at",
        }
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        tweets = data.get("data") or []
        for t in tweets:
            tid = t.get("id")
            text = (t.get("text") or "").strip()
            created_at = (t.get("created_at") or "").strip()
            if not tid or not text:
                continue
            # ざっくり期間フィルタ（created_atが無いケースは通す）
            if created_at:
                try:
                    dt = datetime.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    if int(dt.timestamp()) < min_ts:
                        continue
                except Exception:
                    pass
            out.append({
                "text": text,
                "url": f"https://x.com/i/web/status/{tid}",
                "source": "x",
                "created_at": created_at,
            })
        time.sleep(0.2)
    return _dedup(out)

def collect_items(
    days_back: int = 365,
    total_limit: int = 60,
    per_query: int = 15,
    queries: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """
    env:
      COLLECT_SOURCES="hn,bluesky,mastodon,x"
      COLLECT_QUERIES="how to,error,can't,convert"
    """
    srcs = (os.getenv("COLLECT_SOURCES") or "hn,bluesky,mastodon").lower().split(",")
    srcs = [s.strip() for s in srcs if s.strip()]
    qenv = (os.getenv("COLLECT_QUERIES") or "").strip()
    if queries is None:
        if qenv:
            queries = [x.strip() for x in qenv.split(",") if x.strip()]
        else:
            queries = DEFAULT_QUERIES

    out: List[Dict[str, str]] = []
    if "hn" in srcs:
        out += collect_hn(queries, days_back=days_back, limit_per_query=per_query)
    if "bluesky" in srcs:
        out += collect_bluesky(queries, days_back=days_back, limit_per_query=per_query)
    if "mastodon" in srcs:
        out += collect_mastodon(queries, limit_per_query=per_query)
    if "x" in srcs:
        out += collect_x(queries, days_back=days_back, limit_per_query=per_query)

    out = _dedup(out)
    return out[:total_limit]
