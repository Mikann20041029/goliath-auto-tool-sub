import os
import re
import json
import time
import datetime
from typing import Dict, Any, List, Tuple, Optional

import requests
from openai import OpenAI

# Bluesky
try:
    from atproto import Client as BskyClient
except Exception:
    BskyClient = None

# Mastodon
try:
    from mastodon import Mastodon
except Exception:
    Mastodon = None

# X (Twitter)
try:
    import tweepy
except Exception:
    tweepy = None


ROOT = "goliath"
DB_PATH = f"{ROOT}/db.json"
STATE_PATH = f"{ROOT}/outreach_state.json"


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def norm_words(text: str) -> List[str]:
    t = (text or "").lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[^a-z0-9\s\-_/]", " ", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    words = [w for w in t.split(" ") if 3 <= len(w) <= 30]
    stop = {
        "the","and","for","with","from","this","that","have","need","help","please","anyone","what",
        "how","can","could","should","would","tool","tools","free","best","good","looking"
    }
    out = []
    seen = set()
    for w in words:
        if w in stop:
            continue
        if w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out[:80]


def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def pick_best_tool(db: List[Dict[str, Any]], text: str) -> Tuple[Optional[Dict[str, Any]], float]:
    q = norm_words(text)
    best = None
    best_score = 0.0
    for e in db[:200]:  # 上から新しい順に想定
        title = e.get("title", "")
        tags = e.get("tags", []) or []
        cand = norm_words(title) + [str(t).lower() for t in tags]
        score = jaccard(q, cand)
        if score > best_score:
            best_score = score
            best = e
    return best, best_score


def openai_reply_text(client: OpenAI, platform: str, post_text: str, tool_title: str, tool_url: str) -> str:
    # 「疑問文に適した優しい口調で違和感ない言葉に続けてURLを添える」固定
    prompt = f"""
You are writing a short public reply on {platform}.
Goal: help the user, not advertise.
Rules:
- Write in natural English.
- Must sound friendly and non-salesy.
- Must include exactly ONE question mark in the whole reply.
- Must include the tool URL exactly once: {tool_url}
- Keep under 320 characters if platform is X, otherwise under 450 characters.
- Mention the tool name naturally: "{tool_title}"
Context (their post):
{post_text}
Return ONLY the reply text.
""".strip()

    r = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[{"role": "user", "content": prompt}],
    )
    out = (r.choices[0].message.content or "").strip()
    # 最終ガード：URL 1回だけ
    out = re.sub(r"\s+", " ", out).strip()
    if out.count(tool_url) != 1:
        out = re.sub(re.escape(tool_url), "", out).strip()
        out = f"{out} {tool_url}".strip()
    # '?' がなければ末尾に付ける（ただし2個以上は削る）
    if "?" not in out:
        out = out.rstrip(".!") + "?"
    # '?' が複数なら最初の1個だけ残す
    if out.count("?") > 1:
        first = out.find("?")
        out = out[:first+1] + out[first+1:].replace("?", "")
    return out


# -------------------------------
# Collector: HN / Bluesky / X / Mastodon から「悩みっぽい投稿」を集める
# -------------------------------

def hn_search(query: str, days: int = 365, max_hits: int = 30) -> List[Dict[str, str]]:
    # HNはAlgolia検索（キー不要）
    # queryは例: "convert tool" "calculator"
    url = "https://hn.algolia.com/api/v1/search_by_date"
    # 期間フィルタ（created_at_i >= now - days）
    now = int(time.time())
    since = now - days * 24 * 3600
    params = {
        "query": query,
        "tags": "story,comment",
        "numericFilters": f"created_at_i>{since}",
        "hitsPerPage": max_hits,
    }
    try:
        res = requests.get(url, params=params, timeout=20)
        res.raise_for_status()
        data = res.json()
    except Exception:
        return []

    out = []
    for h in data.get("hits", []):
        text = (h.get("comment_text") or h.get("title") or "").strip()
        if not text:
            continue
        object_id = h.get("objectID", "")
        # コメントなら item?id=... が辿れる
        link = h.get("url") or f"https://news.ycombinator.com/item?id={object_id}"
        out.append({"id": f"hn:{object_id}", "text": text, "url": link})
    return out


def bsky_search(handle: str, password: str, query: str, limit: int = 25) -> List[Dict[str, str]]:
    if not handle or not password or BskyClient is None:
        return []
    try:
        c = BskyClient()
        c.login(handle, password)
        # atproto raw call
        resp = c.app.bsky.feed.search_posts({"q": query, "limit": limit})
        out = []
        for p in resp.get("posts", []):
            uri = p.get("uri", "")
            cid = p.get("cid", "")
            text = (p.get("record") or {}).get("text", "") or ""
            # 共有用URL（bsky.app の post URL を組み立て）
            author = (p.get("author") or {}).get("handle", "")
            rkey = uri.split("/")[-1] if uri else ""
            link = f"https://bsky.app/profile/{author}/post/{rkey}" if author and rkey else ""
            pid = f"bsky:{cid or uri}"
            if text:
                out.append({"id": pid, "text": text, "url": link, "uri": uri, "cid": cid})
        return out
    except Exception:
        return []


def mastodon_search(api_base: str, access_token: str, query: str, limit: int = 20) -> List[Dict[str, str]]:
    if not api_base or not access_token or Mastodon is None:
        return []
    try:
        m = Mastodon(access_token=access_token, api_base_url=api_base)
        r = m.search_v2(query, result_type="statuses", limit=limit)
        statuses = r.get("statuses", []) if isinstance(r, dict) else []
        out = []
        for st in statuses:
            sid = st.get("id")
            content = st.get("content", "") or ""
            # HTMLタグ除去
            text = re.sub(r"<[^>]+>", " ", content)
            text = re.sub(r"\s{2,}", " ", text).strip()
            url = st.get("url", "") or ""
            if sid and text:
                out.append({"id": f"masto:{sid}", "text": text, "url": url, "status_id": str(sid)})
        return out
    except Exception:
        return []


def x_search_and_reply_ready() -> bool:
    # 検索はv2が必要になりがち。ここでは「返信はできるが検索はAPI権限次第」なので、
    # まずは home timeline / mentions から拾う実装にしている。
    # （あなたが search を使える権限を持つ場合は拡張可能）
    return True


def x_fetch_mentions() -> List[Dict[str, str]]:
    # 省コスト安全：自分へのメンションから「悩み」を拾う
    # ここはAPI権限に左右されるので、失敗しても全体は落とさない
    if tweepy is None:
        return []
    ck = os.getenv("X_CONSUMER_KEY", "")
    cs = os.getenv("X_CONSUMER_SECRET", "")
    at = os.getenv("X_ACCESS_TOKEN", "")
    ats = os.getenv("X_ACCESS_TOKEN_SECRET", "")
    if not ck or not cs or not at or not ats:
        return []

    try:
        auth = tweepy.OAuth1UserHandler(ck, cs, at, ats)
        api = tweepy.API(auth)
        mentions = api.mentions_timeline(count=20, tweet_mode="extended")
        out = []
        for tw in mentions:
            tid = str(tw.id)
            text = getattr(tw, "full_text", "") or ""
            url = f"https://x.com/{tw.user.screen_name}/status/{tid}"
            out.append({"id": f"x:{tid}", "text": text, "url": url, "tweet_id": tid})
        return out
    except Exception:
        return []


# -------------------------------
# Reply: 各SNSへ自動返信
# -------------------------------

def reply_bluesky(handle: str, password: str, uri: str, cid: str, text: str) -> bool:
    if not handle or not password or BskyClient is None:
        return False
    try:
        c = BskyClient()
        c.login(handle, password)
        # reply needs root/parent refs
        # simplest: create post with reply refs (atproto helper exists)
        c.send_post(
            text=text,
            reply_to={"root": {"uri": uri, "cid": cid}, "parent": {"uri": uri, "cid": cid}},
        )
        return True
    except Exception:
        return False


def reply_mastodon(api_base: str, access_token: str, in_reply_to_id: str, text: str) -> bool:
    if not api_base or not access_token or Mastodon is None:
        return False
    try:
        m = Mastodon(access_token=access_token, api_base_url=api_base)
        m.status_post(text, in_reply_to_id=in_reply_to_id, visibility="public")
        return True
    except Exception:
        return False


def reply_x(tweet_id: str, text: str) -> bool:
    if tweepy is None:
        return False
    ck = os.getenv("X_CONSUMER_KEY", "")
    cs = os.getenv("X_CONSUMER_SECRET", "")
    at = os.getenv("X_ACCESS_TOKEN", "")
    ats = os.getenv("X_ACCESS_TOKEN_SECRET", "")
    if not ck or not cs or not at or not ats:
        return False

    try:
        auth = tweepy.OAuth1UserHandler(ck, cs, at, ats)
        api = tweepy.API(auth)
        api.update_status(
            status=text,
            in_reply_to_status_id=int(tweet_id),
            auto_populate_reply_metadata=True
        )
        return True
    except Exception:
        return False


def create_issue(title: str, body: str):
    pat = os.getenv("GH_PAT", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if not pat or not repo:
        return
    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github+json"}
    payload = {"title": title, "body": body}
    try:
        requests.post(url, headers=headers, json=payload, timeout=20)
    except Exception:
        pass


def main():
    db = read_json(DB_PATH, [])
    if not db:
        # ツールがまだ無ければ何もしない
        return

    state = read_json(STATE_PATH, {"replied": {}, "last_run": ""})
    replied = state.get("replied", {}) if isinstance(state.get("replied"), dict) else {}

    # 1回の実行での最大返信数（暴発防止）
    max_replies = int(os.getenv("OUTREACH_MAX_REPLIES", "5"))

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))

    # 検索クエリ（あなたの方針：convert/generator/calculator寄り）
    queries = [
        "convert tool",
        "calculator",
        "generator",
        "compare plans",
        "timezone converter",
        "checklist template"
    ]

    candidates: List[Dict[str, Any]] = []

    # HN
    for q in queries[:3]:
        candidates += hn_search(q, days=365, max_hits=25)

    # Bluesky
    bsky_handle = os.getenv("BSKY_HANDLE", "")
    bsky_password = os.getenv("BSKY_PASSWORD", "")
    for q in queries[:2]:
        candidates += bsky_search(bsky_handle, bsky_password, q, limit=25)

    # Mastodon
    masto_base = os.getenv("MASTODON_API_BASE", "")
    masto_token = os.getenv("MASTODON_ACCESS_TOKEN", "")
    for q in queries[:2]:
        candidates += mastodon_search(masto_base, masto_token, q, limit=20)

    # X（まずは mentions から拾う。検索は権限次第で拡張）
    if x_search_and_reply_ready():
        candidates += x_fetch_mentions()

    # 重複排除
    uniq = {}
    for c in candidates:
        cid = c.get("id")
        if not cid:
            continue
        uniq[cid] = c
    candidates = list(uniq.values())

    done = 0
    report_lines = []
    for c in candidates:
        if done >= max_replies:
            break
        cid = c["id"]
        if replied.get(cid):
            continue

        text = c.get("text", "")
        tool, score = pick_best_tool(db, text)
        if not tool or score < float(os.getenv("OUTREACH_MIN_SCORE", "0.08")):
            continue

        tool_title = tool.get("title", "tool")
        tool_url = tool.get("public_url", "")
        if not tool_url:
            continue

        platform = cid.split(":")[0]
        reply_text = openai_reply_text(client, platform, text, tool_title, tool_url)

        ok = False
        if platform == "hn":
            # HNは自動投稿が強い制限＋炎上しやすいので「通知のみ」にする
            ok = True
            report_lines.append(f"- HN candidate (notify only): {c.get('url')}\n  - suggested reply: {reply_text}")
        elif platform == "bsky":
            ok = reply_bluesky(bsky_handle, bsky_password, c.get("uri",""), c.get("cid",""), reply_text)
            report_lines.append(f"- Bluesky replied={ok}: {c.get('url')}\n  - tool: {tool_url}\n  - score: {score:.3f}")
        elif platform == "masto":
            ok = reply_mastodon(masto_base, masto_token, c.get("status_id",""), reply_text)
            report_lines.append(f"- Mastodon replied={ok}: {c.get('url')}\n  - tool: {tool_url}\n  - score: {score:.3f}")
        elif platform == "x":
            ok = reply_x(c.get("tweet_id",""), reply_text)
            report_lines.append(f"- X replied={ok}: {c.get('url')}\n  - tool: {tool_url}\n  - score: {score:.3f}")
        else:
            continue

        replied[cid] = {"at": now_utc_iso(), "platform": platform, "tool": tool_url, "score": score}
        done += 1

    state["replied"] = replied
    state["last_run"] = now_utc_iso()
    write_json(STATE_PATH, state)

    if report_lines:
        create_issue(
            title=f"[Goliath] Outreach report ({done} actions)",
            body="\n".join(report_lines)
        )


if __name__ == "__main__":
    main()
# === Social-only outreach collection (no HN), with "days" filter ===
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _cutoff(days: int) -> datetime:
    return _utcnow() - timedelta(days=int(days))

def _to_dt(v: Any) -> Optional[datetime]:
    """
    Normalize timestamps from various SDKs to aware datetime(UTC).
    Accepts:
      - datetime
      - ISO string (e.g. "2026-01-22T12:34:56.789Z")
      - None
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        try:
            # Handle Z
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None

def collect_bluesky_days(query: str, limit: int = 30, days: int = 730) -> List[Dict[str, Any]]:
    """
    Bluesky search with client-side cutoff filtering (days).
    Requires: atproto Client, BSKY_HANDLE/BSKY_PASSWORD in env (your code already uses this).
    """
    cutoff = _cutoff(days)
    try:
        # You already import/use atproto Client somewhere; reuse your existing client init if you have it.
        from atproto import Client as BlueskyClient  # type: ignore
    except Exception:
        return []

    handle = (os.getenv("BSKY_HANDLE") or "").strip()
    pw = (os.getenv("BSKY_PASSWORD") or "").strip()
    if not handle or not pw:
        return []

    try:
        cli = BlueskyClient()
        cli.login(handle, pw)
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    cursor = None
    # Try to get enough results while respecting cutoff
    for _ in range(6):  # small pagination budget
        try:
            # atproto: app.bsky.feed.searchPosts
            res = cli.app.bsky.feed.search_posts({"q": query, "limit": min(100, max(1, limit)), "cursor": cursor})
        except Exception:
            break

        posts = (res.get("posts") or [])
        cursor = res.get("cursor")

        for p in posts:
            created = _to_dt(p.get("record", {}).get("createdAt"))
            if created and created < cutoff:
                continue
            uri = p.get("uri") or ""
            text = (p.get("record", {}).get("text") or "").strip()
            if not uri or not text:
                continue
            out.append({"source": "Bluesky", "text": text, "url": uri, "meta": {"created_at": (created.isoformat() if created else None)}})
            if len(out) >= limit:
                return out

        if not cursor:
            break

    return out[:limit]

def collect_mastodon_days(query: str, limit: int = 30, days: int = 730) -> List[Dict[str, Any]]:
    """
    Mastodon search with client-side cutoff filtering (days).
    Requires: Mastodon.py, MASTODON_API_BASE, MASTODON_ACCESS_TOKEN in env.
    """
    cutoff = _cutoff(days)

    try:
        from mastodon import Mastodon  # type: ignore
    except Exception:
        return []

    base = (os.getenv("MASTODON_API_BASE") or "").strip().rstrip("/")
    tok = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()
    if not base or not tok:
        return []

    try:
        m = Mastodon(access_token=tok, api_base_url=base)
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    try:
        # `search_v2` exists on Mastodon.py>=1.8; fallback to `search` if needed
        if hasattr(m, "search_v2"):
            res = m.search_v2(q=query, result_type="statuses", limit=min(40, max(1, limit)))
            statuses = (res.get("statuses") or [])
        else:
            res = m.search(q=query, result_type="statuses", limit=min(40, max(1, limit)))
            statuses = (res.get("statuses") or [])
    except Exception:
        return []

    for st in statuses:
        created = _to_dt(getattr(st, "created_at", None) if not isinstance(st, dict) else st.get("created_at"))
        if created and created < cutoff:
            continue
        url = getattr(st, "url", None) if not isinstance(st, dict) else st.get("url")
        content = getattr(st, "content", None) if not isinstance(st, dict) else st.get("content")
        if not url:
            continue
        # content is HTML; keep it short-ish and strip tags minimally if you want
        text = (content or "").strip()
        if not text:
            text = "(no text)"
        out.append({"source": "Mastodon", "text": text, "url": str(url), "meta": {"created_at": (created.isoformat() if created else None)}})
        if len(out) >= limit:
            break

    return out[:limit]

def collect_social_only_days(query: str, limit_per_source: int = 30, days: int = 730) -> List[Dict[str, Any]]:
    """
    Social-only (Bluesky + Mastodon), NO HN fallback.
    """
    out: List[Dict[str, Any]] = []
    out.extend(collect_bluesky_days(query, limit=limit_per_source, days=days))
    out.extend(collect_mastodon_days(query, limit=limit_per_source, days=days))

    # de-dupe by URL
    seen = set()
    uniq = []
    for it in out:
        u = it.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)
    return uniq
def _cutoff_iso(days: int) -> str:
    from datetime import datetime, timedelta, timezone
    dt = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    return dt.isoformat()

def bluesky_search(query: str, limit: int = 20, days: int = 730) -> list[dict]:
    """
    Bluesky: app.bsky.feed.searchPosts を使う（atproto Clientが入ってる前提）
    """
    import os
    try:
        from atproto import Client as BlueskyClient
    except Exception:
        return []

    handle = os.getenv("BSKY_HANDLE", "")
    pw = os.getenv("BSKY_PASSWORD", "")
    if not handle or not pw:
        return []

    try:
        c = BlueskyClient()
        c.login(handle, pw)

        # Blueskyは検索APIが時期で微妙に変わるので、安全に dict で叩く
        # 取れたものを日付フィルタする方針
        res = c.app.bsky.feed.search_posts({"q": query, "limit": min(100, max(1, limit))})
        posts = res.get("posts", []) if isinstance(res, dict) else getattr(res, "posts", [])
        cutoff = _cutoff_iso(days)

        out = []
        for p in posts:
            # indexedAt / createdAt があれば使う
            t = (p.get("indexedAt") or p.get("createdAt") or "") if isinstance(p, dict) else ""
            if t and t < cutoff:
                continue
            url = ""
            # 可能なら web URL を組み立て
            if isinstance(p, dict):
                uri = p.get("uri", "")
                # uri例: at://did:.../app.bsky.feed.post/xxx
                if "/app.bsky.feed.post/" in uri:
                    post_id = uri.split("/app.bsky.feed.post/")[-1]
                    # ハンドルが取れないケースもあるので、URLは無理せずuriでもOK
                    url = f"at://{post_id}"
            out.append({"source": "Bluesky", "text": query, "url": url or "bluesky://search", "meta": {"q": query}})
        return out[:limit]
    except Exception:
        return []

def mastodon_search_wrap(query: str, limit: int = 20, days: int = 730) -> list[dict]:
    """
    Mastodon: search APIで拾って、created_at で days フィルタ
    """
    import os
    try:
        from mastodon import Mastodon
    except Exception:
        return []

    api_base = (os.getenv("MASTODON_API_BASE", "") or "").strip().rstrip("/")
    token = (os.getenv("MASTODON_ACCESS_TOKEN", "") or "").strip()
    if not api_base or not token:
        return []

    try:
        m = Mastodon(access_token=token, api_base_url=api_base)
        results = m.search_v2(q=query, limit=min(40, max(1, limit)))
        statuses = results.get("statuses", []) if isinstance(results, dict) else []
        cutoff = _cutoff_iso(days)

        out = []
        for s in statuses:
            created = (s.get("created_at") or "")
            # created_at がdatetimeの場合があるので str に寄せる
            try:
                created_iso = created.isoformat() if hasattr(created, "isoformat") else str(created)
            except Exception:
                created_iso = ""
            if created_iso and created_iso < cutoff:
                continue
            url = s.get("url") or ""
            content = s.get("content") or ""
            out.append({"source": "Mastodon", "text": content[:120], "url": url, "meta": {"q": query}})
        return out[:limit]
    except Exception:
        return []

def x_search_wrap(query: str, limit: int = 10, days: int = 730) -> list[dict]:
    """
    X: 標準は recent search（直近7日）が基本。
    730日指定でも、まずは7日に丸めて安全運用。
    """
    import os
    try:
        import tweepy
    except Exception:
        return []

    # “無料枠”が不確実なので、1回の実行あたりのAPI呼び出し数を極小に制限する
    # 3回/日なら 月≈90回。reads=100/月を仮定しても超えにくい運用。
    max_req = int(os.getenv("X_MAX_REQUESTS_PER_RUN", "1"))
    per_req = int(os.getenv("X_RESULTS_PER_REQUEST", str(min(10, max(1, limit)))))

    # recent searchの都合で days は最大7に丸める（フルアーカイブあるなら拡張）
    full_archive = os.getenv("X_FULL_ARCHIVE", "0") == "1"
    if not full_archive:
        days = min(days, 7)

    consumer_key = os.getenv("X_API_KEY", "")
    consumer_secret = os.getenv("X_API_SECRET", "")
    access_token = os.getenv("X_ACCESS_TOKEN", "")
    access_token_secret = os.getenv("X_ACCESS_SECRET", "")
    if not (consumer_key and consumer_secret and access_token and access_token_secret):
        return []

    try:
        client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            wait_on_rate_limit=False,
        )

        out = []
        # recent search
        for _ in range(max_req):
            resp = client.search_recent_tweets(
                query=query,
                max_results=min(10, max(10, per_req)),  # API都合で10固定近い
                tweet_fields=["created_at", "author_id"],
            )
            if not resp or not resp.data:
                break
            for t in resp.data:
                url = f"https://x.com/i/web/status/{t.id}"
                out.append({"source": "X", "text": query, "url": url, "meta": {"q": query}})
            break  # 1回で十分（上限守る）
        return out[:limit]
    except Exception:
        return []
