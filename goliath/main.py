import os
import re
import json
import time
import glob
import random
import hashlib
import datetime
from pathlib import Path
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple, Optional

import requests
from openai import OpenAI

# Optional SNS libs (missing secretsなら黙ってスキップ)
try:
    from atproto import Client as BskyClient  # type: ignore
except Exception:
    BskyClient = None

try:
    from mastodon import Mastodon  # type: ignore
except Exception:
    Mastodon = None


# =========================
# Constants / Paths
# =========================
ROOT = "goliath"
PAGES_DIR = f"{ROOT}/pages"
DB_PATH = f"{ROOT}/db.json"
INDEX_PATH = f"{ROOT}/index.html"
SEED_SITES_PATH = f"{ROOT}/sites.seed.json"
SITEMAP_PATH = "sitemap.xml"
ROBOTS_PATH = "robots.txt"

STATE_DIR = Path(ROOT) / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
TOOL_HISTORY_PATH = STATE_DIR / "tool_history.json"

# ---- Model choices ----
MODEL_BUILD = os.getenv("MODEL_BUILD", "gpt-4.1-2025-04-14")
MODEL_REPLY = os.getenv("MODEL_REPLY", "gpt-4.1-nano-2025-04-14")

# ---- Lead count ----
LEADS_TOTAL = int(os.getenv("LEADS_TOTAL", "100"))          # Issuesに出す「返信候補」件数
LEADS_PER_SOURCE = int(os.getenv("LEADS_PER_SOURCE", "60"))  # 収集は多め→スコアで上位化

# ---- Collect limits ----
COLLECT_HN = int(os.getenv("COLLECT_HN", "50"))
COLLECT_BSKY = int(os.getenv("COLLECT_BSKY", "50"))
COLLECT_MASTODON = int(os.getenv("COLLECT_MASTODON", "50"))

# ---- Post announce ----
ENABLE_AUTO_POST = os.getenv("ENABLE_AUTO_POST", "1") == "1"


# =========================
# Helpers
# =========================
KEYWORDS = [
    "help", "need help", "anyone know", "any idea", "how do i", "how to", "can't", "cannot", "won't",
    "stuck", "blocked", "error", "bug", "issue", "problem", "failed", "failure", "broken", "crash",
    "exception", "traceback", "deploy", "build failed", "github actions", "workflow", "docker", "npm", "pip",
    "api", "oauth", "convert", "converter", "calculator", "template", "compare", "timezone"
]

BAN_WORDS = ["template", "boilerplate", "starter", "scaffold"]


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def slugify(s: str, max_len: int = 60) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return (s[:max_len] or "tool")


def ensure_dirs() -> None:
    os.makedirs(PAGES_DIR, exist_ok=True)


def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: str, obj: Any) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: str, text: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def extract_html_only(raw: str) -> str:
    """
    余計な挨拶/markdown/``` を排除して <!DOCTYPE html>..</html> のみ切り出す
    """
    raw = raw or ""
    m = re.search(r"(<!DOCTYPE\s+html.*?</html\s*>)", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())
    return raw.strip()


def stable_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


def hit_keywords(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in KEYWORDS)


def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _fingerprint(theme: str, tags: List[str]) -> str:
    base = _norm(theme) + "|" + "|".join(sorted(_norm(t) for t in (tags or [])))
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _too_similar(a: str, b: str) -> bool:
    # ゆるい類似判定（単語Jaccard）
    A = set(_norm(a).split())
    B = set(_norm(b).split())
    if not A or not B:
        return False
    j = len(A & B) / max(1, len(A | B))
    return j >= 0.80


def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d{10,}", "TS", s)     # timestamps
    s = re.sub(r"https?://\S+", "URL", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _read_existing_page_signatures() -> List[str]:
    sigs: List[str] = []
    for path in glob.glob(f"{ROOT}/pages/*/index.html"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                html = f.read()
            sigs.append(_normalize_text(html))
        except Exception:
            continue
    return sigs


def duplication_penalty(new_index_html: str, threshold: float = 0.88) -> Tuple[int, float]:
    """
    Returns (penalty, best_similarity). If too similar: -200.
    """
    new_sig = _normalize_text(new_index_html)
    best = 0.0
    for old in _read_existing_page_signatures():
        r = SequenceMatcher(a=new_sig, b=old).ratio()
        if r > best:
            best = r
    if best >= threshold:
        return (-200, best)
    return (0, best)


# =========================
# Tool history / existing slugs
# =========================
def _load_tool_history() -> List[Dict[str, Any]]:
    try:
        if TOOL_HISTORY_PATH.exists():
            data = json.loads(TOOL_HISTORY_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
    except Exception:
        pass
    return []


def _save_tool_history(hist: List[Dict[str, Any]]) -> None:
    # 最新200件だけ保持
    hist = hist[-200:]
    TOOL_HISTORY_PATH.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")


def _existing_slugs_from_pages_dir(pages_dir: Path) -> set:
    slugs = set()
    if not pages_dir.is_dir():
        return slugs
    for name in os.listdir(pages_dir):
        m = re.match(r"^\d+-(.+)$", name)
        if m:
            slugs.add(m.group(1))
    return slugs


def existing_tool_slugs() -> set:
    slugs = set()
    # 1) goliath/pages に既にあるslug
    slugs |= _existing_slugs_from_pages_dir(Path(PAGES_DIR))

    # 2) もし legacy "pages/" が同階層に存在する構成でも拾う（壊さない）
    slugs |= _existing_slugs_from_pages_dir(Path(__file__).resolve().parent / "pages")

    # 3) tool_history に既にあるslug
    hist = _load_tool_history()
    for it in hist:
        s = it.get("tool_slug") or it.get("slug")
        if isinstance(s, str) and s:
            slugs.add(s)
    return slugs


def ensure_unique_slug(base: str, seen: set) -> str:
    if base not in seen:
        return base
    i = 2
    while f"{base}-{i}" in seen:
        i += 1
    return f"{base}-{i}"


# =========================
# Titles (検索寄せの最低限)
# =========================
def make_search_title(theme: str, tags: List[str]) -> str:
    """
    SNS文っぽいテーマを「検索語に寄せたタイトル」にする最低限。
    """
    t = (theme or "").strip()
    tl = t.lower()

    # ノイズ除去
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"^(show hn:\s*)", "", t, flags=re.IGNORECASE)

    # タグから型を決める
    suffix = "tool"
    if "calculator" in (tags or []) or "finance" in (tags or []):
        suffix = "calculator"
    elif "convert" in (tags or []):
        suffix = "converter"
    elif "time" in (tags or []):
        suffix = "time converter"
    elif "pricing" in (tags or []):
        suffix = "pricing tool"
    elif "productivity" in (tags or []):
        suffix = "template tool"

    # できれば英語っぽい核を拾う
    kws = []
    for k in ["tax", "pta", "subscription", "pricing", "timezone", "time zone", "template", "convert", "calculator"]:
        if k in tl:
            kws.append(k.upper() if k == "pta" else k)
    core = kws[0] if kws else t[:60]

    title = f"{core} {suffix}".strip()
    title = re.sub(r"\s+", " ", title)
    return title[:80]


# =========================
# Scoring
# =========================
def score_item(text: str, url: str, meta: Dict[str, Any]) -> Tuple[int, Dict[str, int]]:
    """
    ポイント表（見える形）
    """
    t = (text or "").lower()

    table: Dict[str, int] = {
        "tool_request": 0,
        "convert_generator_calc": 0,
        "structured_output": 0,
        "specific_inputs": 0,
        "how_to_code_only": 0,
        "too_broad": 0,
        "adult_or_sensitive": 0,
        "duplicate_penalty": 0,
    }

    if any(k in t for k in ["is there a tool", "any tool", "tool for", "looking for a tool", "need a tool"]):
        table["tool_request"] += 8

    if any(k in t for k in ["convert", "converter", "generator", "calculate", "calculator", "format", "transform"]):
        table["convert_generator_calc"] += 7

    if any(k in t for k in ["json", "csv", "markdown", "notion", "template", "checklist", "table"]):
        table["structured_output"] += 5

    if any(k in t for k in ["timezone", "tax", "subscription", "plan", "pricing", "compare", "fee", "rate"]):
        table["specific_inputs"] += 4

    if any(k in t for k in ["how do i code", "write code", "bug in my code", "stack trace", "traceback"]):
        table["how_to_code_only"] -= 6

    if any(k in t for k in ["everything", "all-in-one", "ultimate", "perfect solution"]):
        table["too_broad"] -= 4

    if any(k in t for k in ["porn", "sexual", "nude", "violence", "illegal"]):
        table["adult_or_sensitive"] -= 20

    hn_points = int(meta.get("hn_points", 0) or 0)
    if hn_points > 0:
        table["tool_request"] += min(10, hn_points // 30)

    # ===== anti-duplicate penalty =====
    hist = _load_tool_history()
    theme_now = (meta.get("theme") or text or "")
    tags_now = meta.get("tags") or []
    if not isinstance(tags_now, list):
        tags_now = []
    fp_now = _fingerprint(theme_now, [str(x) for x in tags_now if isinstance(x, str)])

    dup_penalty = 0
    for h in hist:
        if h.get("fp") == fp_now:
            dup_penalty -= 200
            break
        if _too_similar(str(h.get("theme", "")), theme_now):
            dup_penalty -= 200
            break

    table["duplicate_penalty"] = dup_penalty

    score = int(sum(table.values()))
    return score, table


# =========================
# Collector (HN / Bluesky / Mastodon / Reddit / X limited)
# =========================
def hn_search(query: str, limit: int = 30, ask_only: bool = False) -> List[Dict[str, Any]]:
    """
    HN (Algolia) から検索。
    """
    try:
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {"query": query, "tags": "story", "hitsPerPage": min(100, max(1, limit))}
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        out: List[Dict[str, Any]] = []
        for h in data.get("hits", [])[:limit]:
            title = h.get("title") or ""
            if ask_only:
                if not (title.startswith("Ask HN:") or title.startswith("Tell HN:")):
                    continue

            story_url = h.get("url") or ""
            hn_url = f"https://news.ycombinator.com/item?id={h.get('objectID')}"
            points = h.get("points") or 0

            out.append({
                "source": "HN",
                "text": title,
                "url": story_url if story_url else hn_url,
                "meta": {"hn_points": points, "hn_discuss": hn_url}
            })
        return out
    except Exception:
        return []


def collect_hn(limit: int) -> List[Dict[str, Any]]:
    queries = [
        "tool for",
        "is there a tool",
        "convert",
        "calculator",
        "compare pricing",
        "timezone",
    ]
    all_items: List[Dict[str, Any]] = []
    per = max(5, limit // max(1, len(queries)))
    for q in queries:
        all_items.extend(hn_search(q, limit=per))

    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in all_items:
        u = it.get("url", "")
        if not u or u in seen:
            continue

        text_low = (it.get("text", "") or "").lower()
        url_low = (u or "").lower()
        if any(w in text_low for w in BAN_WORDS) or any(w in url_low for w in BAN_WORDS):
            continue

        seen.add(u)
        uniq.append(it)
        if len(uniq) >= limit:
            break

    return uniq


def bsky_uri_to_url(uri: str, did: str = "") -> str:
    # at://did:plc:xxxx/app.bsky.feed.post/3k... -> https://bsky.app/profile/did:plc:xxxx/post/3k...
    try:
        if uri.startswith("at://"):
            parts = uri[5:].split("/")
            _did = parts[0]
            rkey = parts[-1]
            return f"https://bsky.app/profile/{_did}/post/{rkey}"
        if did and uri:
            m = re.search(r"/app\.bsky\.feed\.post/([^/]+)$", uri)
            if m:
                return f"https://bsky.app/profile/{did}/post/{m.group(1)}"
        return ""
    except Exception:
        return ""

# --- add: atproto return normalization ---
def _to_dict(x):
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    try:
        if hasattr(x, "model_dump"):
            return x.model_dump()
    except Exception:
        pass
    try:
        if hasattr(x, "dict"):
            return x.dict()
    except Exception:
        pass
    return {}

def _to_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return []

def collect_bluesky(limit: int) -> List[Dict[str, Any]]:
    """
    Bluesky: atprotoで検索 + timeline fallback
    必要: BSKY_HANDLE / BSKY_PASSWORD
    """
    h = os.getenv("BSKY_HANDLE", "")
    p = os.getenv("BSKY_PASSWORD", "")
    if not h or not p or BskyClient is None:
        return []

    queries = [
        "need a tool",
        "is there a tool",
        "how can I convert",
        "converter",
        "calculator",
        "compare plans",
        "timezone",
        "template",
        "error",
        "bug",
        "issue",
        "failed",
    ]

    keywords = KEYWORDS[:]  # reuse

    out: List[Dict[str, Any]] = []
    try:
        c = BskyClient()
        c.login(h, p)

        # 1) search
        for q in queries:
            res = c.app.bsky.feed.search_posts({"q": q, "limit": 25})
            posts = (res or {}).get("posts", []) or []
            for post in posts:
                rec = post.get("record", {}) or {}
                txt = (rec.get("text", "") or "")
                uri = (post.get("uri", "") or "")
                did = (post.get("author", {}) or {}).get("did", "") or ""
                url = bsky_uri_to_url(uri, did) or uri
                if not url:
                    continue

                out.append({"source": "Bluesky", "text": txt[:300], "url": url, "meta": {}})
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break

        # 2) timeline fallback
        if len(out) < limit:
            try:
                tl = c.app.bsky.feed.get_timeline({"limit": 200})
                feed = (tl or {}).get("feed", []) or []
                for item in feed:
                    post = (item or {}).get("post", {}) or {}
                    record = (post or {}).get("record", {}) or {}
                    txt = (record.get("text", "") or "")
                    t = txt.lower()
                    if not any(k in t for k in keywords):
                        continue

                    uri = (post.get("uri", "") or "")
                    did = (post.get("author", {}) or {}).get("did", "") or ""
                    url = bsky_uri_to_url(uri, did) or uri
                    if not url:
                        continue

                    out.append({"source": "Bluesky", "text": txt[:300], "url": url, "meta": {}})
                    if len(out) >= limit:
                        break
            except Exception:
                pass

        # 3) dedupe
        seen = set()
        uniq: List[Dict[str, Any]] = []
        for it in out:
            u = it.get("url", "")
            if not u or u in seen:
                continue
            seen.add(u)
            uniq.append(it)

        return uniq[:limit]
    except Exception:
        return out[:limit]


def collect_reddit(limit: int) -> List[Dict[str, Any]]:
    cid = os.getenv("REDDIT_CLIENT_ID", "")
    csec = os.getenv("REDDIT_CLIENT_SECRET", "")
    user = os.getenv("REDDIT_USERNAME", "")
    pw = os.getenv("REDDIT_PASSWORD", "")
    ua = os.getenv("REDDIT_USER_AGENT", "")

    if not (cid and csec and user and pw and ua):
        return []

    try:
        import praw  # type: ignore
    except Exception:
        return []

    queries = [
        "help", "how to", "error", "bug", "issue", "failed", "broken",
        "convert", "converter", "calculator", "timezone", "template"
    ]

    out: List[Dict[str, Any]] = []
    try:
        r = praw.Reddit(
            client_id=cid,
            client_secret=csec,
            username=user,
            password=pw,
            user_agent=ua,
        )

        for q in queries:
            for s in r.subreddit("all").search(q, sort="new", time_filter="day", limit=80):
                txt = (getattr(s, "title", "") or "") + "\n" + (getattr(s, "selftext", "") or "")
                url = getattr(s, "url", "") or ""
                out.append({"source": "Reddit", "text": txt[:300], "url": url, "meta": {}})
                if len(out) >= limit:
                    return out[:limit]
    except Exception:
        pass

    return out[:limit]


def collect_mastodon(limit: int) -> List[Dict[str, Any]]:
    """
    Mastodon:
    - MASTODON_API_BASE があれば public timeline は読める
    - トークンがあれば search を使う
    """
    base = os.getenv("MASTODON_API_BASE", "")
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "")
    if not base or Mastodon is None:
        return []

    out: List[Dict[str, Any]] = []
    try:
        m = Mastodon(access_token=tok if tok else None, api_base_url=base)

        queries = [
            "help", "need help", "anyone know", "how do i", "how to", "what should i do", "can someone", "please help",
            "stuck", "blocked",
            "error", "bug", "issue", "problem", "failed", "broken", "crash", "exception", "traceback", "stack trace",
            "deploy", "build failed", "github actions", "workflow", "docker", "npm", "pip", "python", "javascript",
            "typescript", "api", "oauth",
            "need a tool", "is there a tool", "looking for a tool", "tool for",
            "convert", "converter", "calculator", "template", "compare", "timezone",
        ]

        # tokenがある時だけ検索
        if tok:
            for q in queries:
                try:
                    res = m.search_v2(q=q, result_type="statuses", limit=25)
                    statuses = (res or {}).get("statuses", []) or []
                    for st in statuses:
                        txt = re.sub(r"<[^>]+>", "", st.get("content", "") or "")
                        url = st.get("url", "") or ""
                        out.append({"source": "Mastodon", "text": txt[:300], "url": url, "meta": {}})
                        if len(out) >= limit:
                            break
                    if len(out) >= limit:
                        break
                except Exception:
                    pass

        # timeline fallback
        if len(out) < limit:
            try:
                statuses = m.timeline_public(limit=80)
                keywords = KEYWORDS[:]  # reuse
                for st in statuses:
                    txt = re.sub(r"<[^>]+>", "", st.get("content", "") or "")
                    t = txt.lower()
                    if not any(k in t for k in keywords):
                        continue
                    url = st.get("url", "") or ""
                    out.append({"source": "Mastodon", "text": txt[:300], "url": url, "meta": {}})
                    if len(out) >= limit:
                        break
            except Exception:
                pass

        # dedupe
        seen = set()
        uniq: List[Dict[str, Any]] = []
        for it in out:
            u = it.get("url", "")
            if not u or u in seen:
                continue
            seen.add(u)
            uniq.append(it)

        return uniq[:limit]
    except Exception:
        return out[:limit]


def collect_x_limited(theme: str) -> List[Dict[str, Any]]:
    """
    X Free枠想定: readsが厳しいので1 run あたり reads<=1 くらいに抑える設計。
    認証情報が揃っていない/権限不足なら空で返す（壊れないこと優先）。
    """
    runs_per_month = int(os.getenv("RUNS_PER_MONTH", "90"))
    max_reads_month = int(os.getenv("X_READS_PER_MONTH", "100"))
    max_reads_run = max(1, max_reads_month // max(1, runs_per_month))  # だいたい1

    api_key = os.getenv("X_API_KEY", "")
    api_secret = os.getenv("X_API_SECRET", "")
    access_token = os.getenv("X_ACCESS_TOKEN", "")
    access_secret = os.getenv("X_ACCESS_SECRET", "")

    if not (api_key and api_secret and access_token and access_secret):
        return []

    try:
        import tweepy  # type: ignore
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    try:
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret,
            wait_on_rate_limit=True,
        )

        me = client.get_me(user_auth=True)
        if not me or not getattr(me, "data", None):
            return []
        user_id = me.data.id

        resp = client.get_users_mentions(id=user_id, max_results=min(5, max_reads_run))
        if not resp or not getattr(resp, "data", None):
            return []

        for tw in resp.data[:max_reads_run]:
            txt = (tw.text or "")[:300]
            url = f"https://x.com/i/web/status/{tw.id}"
            out.append({"source": "X", "text": txt, "url": url, "meta": {}})
    except Exception:
        return []

    return out


def report_source_counts(counts: Dict[str, int], notes: str = "") -> None:
    msg = ["Source counts (collector/leads):"]
    for k, v in counts.items():
        msg.append(f"- {k}: {v}")
    if notes:
        msg.append("")
        msg.append(notes)
    create_github_issue("[Goliath] Source debug report", "\n".join(msg))


def collector_real() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    b = collect_bluesky(COLLECT_BSKY)
    m = collect_mastodon(COLLECT_MASTODON)
    h = collect_hn(COLLECT_HN)

    items.extend(b)
    items.extend(m)
    items.extend(h)

    if len(b) == 0 or len(m) == 0:
        notes = []
        notes.append("If Bluesky/Mastodon are 0, likely causes:")
        notes.append("- missing requirements: atproto / Mastodon.py")
        notes.append("- missing secrets: BSKY_HANDLE/BSKY_PASSWORD/MASTODON_API_BASE/MASTODON_ACCESS_TOKEN")
        notes.append("- MASTODON_API_BASE should be like https://mastodon.social")
        report_source_counts({"Bluesky": len(b), "Mastodon": len(m), "HN": len(h)}, "\n".join(notes))

    if not items:
        samples = [
            ("need a simple calculator to compare subscription plans with hidden fees", "https://news.ycombinator.com/"),
            ("how to convert a messy checklist into a clean template instantly", "https://bsky.app/"),
            ("time zone converter with meeting overlap and daylight saving awareness", "https://mastodon.social/"),
        ]
        random.shuffle(samples)
        for t, u in samples:
            items.append({"source": "Stub", "text": t, "url": u, "meta": {}})

    return items


# =========================
# Cluster / Theme
# =========================
def pick_best_theme(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    scored: List[Tuple[int, Dict[str, Any], Dict[str, int]]] = []
    for it in items:
        s, table = score_item(it.get("text", ""), it.get("url", ""), it.get("meta", {}) or {})
        scored.append((s, it, table))
    scored.sort(key=lambda x: x[0], reverse=True)

    best_score, best_item, best_table = scored[0]
    return {
        "theme": best_item.get("text", ""),
        "best_item": best_item,
        "best_score": best_score,
        "best_table": best_table,
        "scored": scored
    }


def cluster_20_around_theme(theme: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    近いものを雑に集める最小実装（キーワード一致ベース）
    """
    t = (theme or "").lower()
    keys: List[str] = []
    for k in ["convert", "calculator", "compare", "timezone", "template", "subscription", "pricing", "tax", "checklist"]:
        if k in t:
            keys.append(k)
    if not keys:
        keys = t.split()[:3] if t else ["tool"]

    def sim(x: str) -> int:
        xl = (x or "").lower()
        return sum(1 for k in keys if k and k in xl)

    ranked = sorted(items, key=lambda it: sim(it.get("text", "")), reverse=True)
    chosen = ranked[:20]
    return {
        "theme": theme,
        "items": [{"text": it.get("text", ""), "url": it.get("url", ""), "source": it.get("source", "")} for it in chosen],
        "urls": [it.get("url", "") for it in chosen],
        "texts": [it.get("text", "") for it in chosen],
        "keys": keys,
    }


# =========================
# Related Sites Logic
# =========================
def load_seed_sites() -> List[Dict[str, Any]]:
    """
    既存資産（hubや既存サイト）を「関連サイト候補」として入れておける。
    """
    if os.path.exists(SEED_SITES_PATH):
        data = read_json(SEED_SITES_PATH, [])
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            vals: List[Dict[str, Any]] = []
            for v in data.values():
                if isinstance(v, dict):
                    vals.append(v)
            return vals
    return []


def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _norm_tags(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        x = [x]
    if not isinstance(x, list):
        return []
    out: List[str] = []
    for t in x:
        if isinstance(t, str) and t.strip():
            out.append(t.strip().lower())
    return out


def _seed_map(seed_sites: Any) -> Dict[str, Dict[str, Any]]:
    m: Dict[str, Dict[str, Any]] = {}
    if isinstance(seed_sites, dict):
        for k, v in seed_sites.items():
            if isinstance(v, dict):
                slug = v.get("slug") or k
                if isinstance(slug, str) and slug:
                    m[slug] = v
    elif isinstance(seed_sites, list):
        for v in seed_sites:
            if isinstance(v, dict):
                slug = v.get("slug")
                if isinstance(slug, str) and slug:
                    m[slug] = v
    return m


def pick_related(current_tags: Any, all_entries: Any, seed_sites: Any, k: int = 8) -> List[Dict[str, str]]:
    """
    - DB内の過去ページ(all_entries) と seed_sites から「タグが近い」ものを最大k件返す
    - 型が崩れても落とさない
    """
    tags = _norm_tags(current_tags)
    seed_map = _seed_map(seed_sites)

    normalized_entries: List[Dict[str, Any]] = []
    if isinstance(all_entries, list):
        for e in all_entries:
            if isinstance(e, dict):
                normalized_entries.append(e)
            elif isinstance(e, str) and e in seed_map and isinstance(seed_map[e], dict):
                normalized_entries.append(seed_map[e])

    normalized_seeds: List[Dict[str, Any]] = []
    if isinstance(seed_sites, list):
        normalized_seeds = [s for s in seed_sites if isinstance(s, dict)]
    elif isinstance(seed_sites, dict):
        for v in seed_sites.values():
            if isinstance(v, dict):
                normalized_seeds.append(v)

    candidates: List[Tuple[float, Dict[str, str]]] = []

    for e in normalized_entries:
        etags = _norm_tags(e.get("tags", []))
        score = jaccard(tags, etags)
        if score <= 0:
            continue
        title = (e.get("title") or "").strip()
        url = (e.get("public_url") or e.get("url") or "").strip()
        if not url:
            continue
        candidates.append((score, {"title": title, "url": url}))

    for s in normalized_seeds:
        stags = _norm_tags(s.get("tags", []))
        score = jaccard(tags, stags)
        if score <= 0:
            continue
        title = (s.get("title") or "").strip()
        url = (s.get("url") or s.get("public_url") or "").strip()
        if not url:
            continue
        candidates.append((score, {"title": title, "url": url}))

    candidates.sort(key=lambda x: x[0], reverse=True)

    seen = set()
    related: List[Dict[str, str]] = []
    for _score, item in candidates:
        u = item.get("url", "")
        if not u or u in seen:
            continue
        seen.add(u)
        related.append(item)
        if len(related) >= k:
            return related

    # 0件回避: 新着から埋める
    for e in normalized_entries:
        u = (e.get("public_url") or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        related.append({"title": (e.get("title") or "").strip(), "url": u})
        if len(related) >= k:
            return related

    for s in normalized_seeds:
        u = (s.get("url") or s.get("public_url") or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        related.append({"title": (s.get("title") or "").strip(), "url": u})
        if len(related) >= k:
            break

    return related[:k]


# =========================
# Builder / Validator / Auto-fix
# =========================
def build_prompt(theme: str, cluster: Dict[str, Any], canonical_url: str) -> str:
    return f"""
You are generating a production-grade single-file HTML tool site.

STRICT OUTPUT RULE:
- Output ONLY raw HTML that starts with <!DOCTYPE html> and ends with </html>.
- No markdown, no backticks, no explanations.

[Goal]
Create a modern SaaS-style tool page to solve: "{theme}"

[Design]
- Use Tailwind CSS via CDN (no build step)
- Clean SaaS UI: top nav + hero section + centered tool card + sections
- Use white background + trusted blue/indigo accents + neutral grays
- Use generous spacing (padding/margin), professional typography (Inter / Noto Sans JP via CDN)
- Add subtle glassmorphism for the tool card (backdrop-blur, translucent)
- Dark/Light mode toggle (CSS class switch) and respect saved theme in localStorage.

[Navigation]
- Add a top nav link back to the hub: href="../../index.html" (label must be translated via i18n)
- Add a footer link back to hub as well (translated via i18n).

[Tool]
- Implement an interactive JS mini-tool relevant to the theme (static, no server).
- Must work without any server.

[Content]
- Include a long-form article primarily in English (>= 1200 English words).
- Also include Japanese full translation of the article (>= 2500 Japanese characters).
- Use clear structure with H2/H3 headings, checklist, pitfalls, FAQ(>=5).
- Add "References" section with 8-12 reputable external links (official docs / well-known sites).

[Multi-language - VERY STRICT]
- Default language MUST be English ("en").
- Provide a WORKING language switcher for JA/EN/FR/DE (must work on mobile).
- The switcher MUST be a <select> with id="langSelect" and options: en,ja,fr,de (in that order).
- EVERY visible text on the page MUST be translatable via i18n keys:
  - All UI labels, buttons, nav, footer, headings, paragraphs, checklist items, FAQ questions & answers,
    policy sections text, and any notices.
- Do NOT hardcode visible strings directly in HTML outside i18n keys.
- Any translatable UI text MUST use data-i18n keys. Example: <span data-i18n="hero.title"></span>
- For placeholders use data-i18n-placeholder.
- For title attributes use data-i18n-title.
- For aria-label use data-i18n-aria.
- Provide a JS dictionary exactly as: window.__I18N__ = { en:{...}, ja:{...}, fr:{...}, de:{...} }
- Provide a JS initializer that:
  - reads saved language from localStorage key "goliath_lang" (fallback to "en")
  - sets <html lang="..">
  - fills all [data-i18n] elements from window.__I18N__[lang][key]
  - also applies placeholder/title/aria-label using data-i18n-placeholder/data-i18n-title/data-i18n-aria
  - wires change event on #langSelect to re-render
- Ensure the article section headings and ALL article paragraphs/bullets/FAQ are also i18n-driven.

[Compliance / Footer]
- Auto-generate in-page sections for:
  - Privacy Policy (cookie/ads explanation)
  - Terms of Service
  - Disclaimer
  - About / Operator info
  - Contact
- These must be accessible via footer links using in-page anchors.
- All text in these sections MUST be i18n-driven.

[Related Sites]
- Include a "Related sites" section near bottom as a list:
  - It must be filled from a JSON embedded in the page: window.__RELATED__ = [];
  - Render it into the list on load.
  - If empty, hide the section.

[SEO]
- Include title/meta description/canonical.
- Canonical must be: {canonical_url}
- Title should be "search-friendly" (not SNS-like). Use a concise keyword-style title.

Return ONLY the final HTML.
""".strip()



def openai_generate_html(client: OpenAI, prompt: str) -> str:
    res = client.chat.completions.create(
        model=MODEL_BUILD,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = res.choices[0].message.content or ""
    return extract_html_only(raw)


def validate_html(html: str) -> Tuple[bool, str]:
    low = (html or "").lower()
    if "<!doctype html" not in low:
        return False, "missing doctype"
    if "</html>" not in low:
        return False, "missing </html>"
    if "tailwind" not in low:
        return False, "tailwind not found"
    if "__related__" not in low:
        return False, "missing window.__RELATED__"
    must = ["privacy", "terms", "disclaimer", "about", "contact"]
    missing = [m for m in must if m not in low]
    if missing:
        return False, f"missing policy sections: {missing}"

    # language switcher
    if 'id="langselect"' not in low:
        return False, "missing #langSelect"
            # ---- i18n coverage guard (prevents "only major text translates") ----
    # Require enough i18n bindings so most of the page is covered.
    if low.count("data-i18n=") < 80:
        return False, "i18n coverage too small (data-i18n < 80)"
    if "data-i18n-placeholder" not in low:
        return False, "missing data-i18n-placeholder"
    if "goliath_lang" not in low:
        return False, "missing localStorage key goliath_lang handling"
    # Default must be English fallback
    if 'fallback to "en"' not in low and 'fallback to \\"en\\"' not in low and '"en"' not in low:
        # loose check (generator varies). still helps catch wrong default.
        pass

    if "__i18n__" not in low:
        return False, "missing window.__I18N__"
    if "data-i18n" not in low:
        return False, "missing data-i18n bindings"
    if "goliath_lang" not in low:
        return False, "missing localStorage key goliath_lang handling"

    # hub link
    if "../../index.html" not in low and "../index.html" not in low:
        return False, "missing link back to hub (../../index.html)"
        
    return True, "ok"


def prompt_for_fix(error: str, html: str) -> str:
    return f"""
Return ONLY a unified diff patch for a single file named index.html.

Rules:
- Output ONLY the diff. No markdown. No explanations.
- The patch MUST fix this validation error: {error}
- Do not remove required features: Tailwind CDN, SaaS layout, dark/light toggle, language switcher,
  footer policy sections, window.__RELATED__ rendering, link back to hub.

Current index.html:
{html}
""".strip()


def apply_unified_diff_to_text(original: str, diff_text: str) -> Optional[str]:
    # unified diff must start with "---"
    if not diff_text.startswith("---"):
        return None
    lines = diff_text.splitlines()
    hunks = [i for i, l in enumerate(lines) if l.startswith("@@")]
    if not hunks:
        return None

    orig_lines = original.splitlines()
    try:
        result: List[str] = []
        oidx = 0
        i = 0
        while i < len(lines):
            if not lines[i].startswith("@@"):
                i += 1
                continue
            header = lines[i]
            m = re.match(r"@@ -(\d+),?(\d*) \+(\d+),?(\d*) @@", header)
            if not m:
                return None
            old_start = int(m.group(1)) - 1

            # copy unchanged up to hunk start
            while oidx < old_start and oidx < len(orig_lines):
                result.append(orig_lines[oidx])
                oidx += 1

            i += 1
            while i < len(lines) and not lines[i].startswith("@@"):
                l = lines[i]
                if l.startswith(" "):
                    result.append(l[1:])
                    oidx += 1
                elif l.startswith("-"):
                    oidx += 1
                elif l.startswith("+"):
                    result.append(l[1:])
                else:
                    return None
                i += 1

        # copy rest
        while oidx < len(orig_lines):
            result.append(orig_lines[oidx])
            oidx += 1

        return "\n".join(result) + ("\n" if original.endswith("\n") else "")
    except Exception:
        return None


def infer_tags_simple(theme: str) -> List[str]:
    t = (theme or "").lower()
    tags: List[str] = []
    rules = {
        "convert": "convert",
        "converter": "convert",
        "calculator": "calculator",
        "compare": "compare",
        "tax": "finance",
        "timezone": "time",
        "time zone": "time",
        "subscription": "pricing",
        "pricing": "pricing",
        "plan": "pricing",
        "checklist": "productivity",
        "template": "productivity",
    }
    for k, v in rules.items():
        if k in t and v not in tags:
            tags.append(v)
    if not tags:
        tags = ["tools"]
    return tags[:6]


def inject_related_json(html: str, related: List[Dict[str, str]]) -> str:
    rel_json = json.dumps(related, ensure_ascii=False)
    new = re.sub(
        r"window\.__RELATED__\s*=\s*\[[\s\S]*?\]\s*;",
        f"window.__RELATED__ = {rel_json};",
        html
    )
    if new == html:
        new = re.sub(
            r"</body>",
            f"<script>window.__RELATED__ = {rel_json};</script>\n</body>",
            html,
            flags=re.IGNORECASE
        )
    return new


# =========================
# Publishing / Index(Hub) / Sitemap / Notify / SNS
# =========================
def get_repo_pages_base() -> str:
    repo = os.getenv("GITHUB_REPOSITORY", "mikann20041029/goliath-auto-tool")
    owner, name = repo.split("/", 1)
    return f"https://{owner.lower()}.github.io/{name}/"


def normalize_db(raw_db: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_db, list):
        return [x for x in raw_db if isinstance(x, dict)]
    if isinstance(raw_db, dict):
        if isinstance(raw_db.get("entries"), list):
            return [x for x in raw_db["entries"] if isinstance(x, dict)]
        vals = [v for v in raw_db.values() if isinstance(v, dict)]
        return vals
    return []


def build_sitemap_and_robots(all_entries: List[Dict[str, Any]], pages_base: str) -> None:
    urls: List[str] = []
    hub_url = f"{pages_base}{ROOT}/index.html"
    urls.append(hub_url)

    for e in all_entries:
        u = (e.get("public_url") or "").strip()
        if u:
            urls.append(u.rstrip("/") + "/")

    seen = set()
    uniq: List[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)

    items = []
    for u in uniq:
        items.append(
            "  <url>\n"
            f"    <loc>{u}</loc>\n"
            "    <changefreq>daily</changefreq>\n"
            "  </url>"
        )

    sitemap = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(items) + "\n</urlset>\n"
    )
    write_text(SITEMAP_PATH, sitemap)

    robots = (
        "User-agent: *\n"
        "Allow: /\n"
        f"Sitemap: {pages_base}sitemap.xml\n"
    )
    write_text(ROBOTS_PATH, robots)


def update_db_and_index(entry: Dict[str, Any], all_entries: List[Dict[str, Any]], pages_base: str) -> None:
    if not isinstance(all_entries, list):
        all_entries = []
    all_entries.insert(0, entry)
    write_json(DB_PATH, all_entries)

    def safe_tags(e: Dict[str, Any]) -> List[str]:
        t = e.get("tags", []) or []
        return [x for x in t if isinstance(x, str)]

    popular = sorted(all_entries, key=lambda e: int(e.get("best_score", 0) or 0), reverse=True)[:12]
    newest = all_entries[:24]

    cat_map: Dict[str, List[Dict[str, Any]]] = {}
    for e in all_entries:
        for tg in safe_tags(e):
            cat_map.setdefault(tg, []).append(e)

    cat_order = ["pricing", "convert", "time", "productivity", "calculator", "finance", "compare", "tools"]
    cats: List[Tuple[str, List[Dict[str, Any]]]] = []
    for c in cat_order:
        if c in cat_map:
            cats.append((c, cat_map[c]))
    for c in sorted(cat_map.keys()):
        if c not in {x[0] for x in cats}:
            cats.append((c, cat_map[c]))

    def card(e: Dict[str, Any]) -> str:
        href = f"{e.get('path', '')}/"
        title = (e.get("search_title") or e.get("title") or "").strip()
        meta = f"{e.get('created_at','')} • {', '.join(safe_tags(e))}"
        return (
            '<a class="block p-4 rounded-xl border border-slate-200 dark:border-slate-800 '
            'hover:bg-slate-50 dark:hover:bg-slate-900 transition" '
            f'href="{href}">'
            f'<div class="font-semibold">{title}</div>'
            f'<div class="text-sm opacity-70 mt-1">{meta}</div>'
            "</a>"
        )

    popular_html = "\n".join([card(e) for e in popular])
    newest_html = "\n".join([card(e) for e in newest])

    cat_links = []
    for c, lst in cats[:16]:
        cat_links.append(
            f'<button class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800 text-sm" '
            f'onclick="filterByTag(\'{c}\')">{c} <span class="opacity-60">({len(lst)})</span></button>'
        )
    cat_links_html = "\n".join(cat_links)

    all_cards_html = "\n".join([card(e) for e in all_entries[:200]])

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Goliath Tools Hub</title>
  <meta name="description" content="Auto-generated tools + long-form guides. Browse by purpose, popular tools, and newest releases." />
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-50">
  <div class="max-w-5xl mx-auto p-6">
    <div class="flex items-center justify-between gap-4">
      <div>
        <div class="text-xl font-bold">Goliath Tools Hub</div>
        <div class="text-sm opacity-70 mt-1">発見（SEO）と回遊（内部リンク）を強化するための上位ページ</div>
      </div>
      <button id="themeBtn" class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800">Dark/Light</button>
    </div>

    <div class="mt-6">
      <div class="text-sm font-semibold opacity-80">目的別（カテゴリ）</div>
      <div class="mt-3 flex flex-wrap gap-2">
        {cat_links_html}
      </div>
    </div>

    <div class="mt-8 grid md:grid-cols-2 gap-6">
      <div>
        <div class="text-sm font-semibold opacity-80">人気ツール（暫定：スコア上位）</div>
        <div class="mt-3 grid gap-3">
          {popular_html}
        </div>
      </div>
      <div>
        <div class="text-sm font-semibold opacity-80">新着</div>
        <div class="mt-3 grid gap-3">
          {newest_html}
        </div>
      </div>
    </div>

    <div class="mt-10">
      <div class="flex items-center justify-between gap-3">
        <div class="text-sm font-semibold opacity-80">全ツール（フィルタ/検索）</div>
        <input id="q" class="w-56 px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800 bg-transparent text-sm"
               placeholder="search..." />
      </div>
      <div id="allList" class="mt-3 grid gap-3">
        {all_cards_html}
      </div>
      <div class="mt-10 text-xs opacity-60">
        <a class="underline" href="./pages/">All pages</a>
      </div>
    </div>
  </div>

  <script>
    // theme
    const root = document.documentElement;
    const k = "goliath_theme";
    const saved = localStorage.getItem(k);
    if (saved === "dark") root.classList.add("dark");
    document.getElementById("themeBtn").onclick = () => {{
      root.classList.toggle("dark");
      localStorage.setItem(k, root.classList.contains("dark") ? "dark" : "light");
    }};

    // filter/search
    const allCards = Array.from(document.querySelectorAll("#allList > a"));
    function applyFilter(tag, q) {{
      const qq = (q || "").toLowerCase().trim();
      allCards.forEach(a => {{
        const text = a.innerText.toLowerCase();
        const okTag = !tag || text.includes(tag.toLowerCase());
        const okQ = !qq || text.includes(qq);
        a.style.display = (okTag && okQ) ? "" : "none";
      }});
    }}
    window.filterByTag = (tag) => {{
      document.getElementById("q").value = "";
      applyFilter(tag, "");
      window.scrollTo({{ top: document.getElementById("allList").offsetTop - 20, behavior: "smooth" }});
    }};
    document.getElementById("q").addEventListener("input", (e) => {{
      applyFilter("", e.target.value);
    }});
  </script>
</body>
</html>
"""
    write_text(INDEX_PATH, html)

    # sitemap/robots 更新
    build_sitemap_and_robots(all_entries, pages_base)


def create_github_issue(title: str, body: str) -> None:
    pat = os.getenv("GH_PAT", "") or os.getenv("GITHUB_TOKEN", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if not pat or not repo:
        print("[issue] skip: missing token or repo", {"has_token": bool(pat), "repo": repo})
        return

    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"title": title, "body": body}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        print("[issue] status", r.status_code)
        if r.status_code >= 300:
            print("[issue] error body:", r.text[:500])
        else:
            data = r.json()
            print("[issue] created:", data.get("html_url"))
    except Exception as e:
        print("[issue] exception:", repr(e))


def post_bluesky(text: str) -> None:
    h = os.getenv("BSKY_HANDLE", "")
    p = os.getenv("BSKY_PASSWORD", "")
    if not h or not p or BskyClient is None:
        return
    try:
        c = BskyClient()
        c.login(h, p)
        c.send_post(text=text)
    except Exception:
        pass


def post_mastodon(text: str) -> None:
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "")
    base = os.getenv("MASTODON_API_BASE", "")
    if not tok or not base or Mastodon is None:
        return
    try:
        m = Mastodon(access_token=tok, api_base_url=base)
        m.status_post(text)
    except Exception:
        pass


def post_x(text: str) -> None:
    """
    Xは無料枠/権限/認証が可変なので、壊れないこと優先でこの版はIssueに通知だけ。
    """
    if not (os.getenv("X_API_KEY") and os.getenv("X_API_SECRET") and os.getenv("X_ACCESS_TOKEN") and os.getenv("X_ACCESS_TOKEN_SECRET")):
        return
    create_github_issue(
        title="[Goliath] X auto-post skipped (OAuth/plan dependent)",
        body="X投稿はプラン/権限/OAuth実装差で壊れやすいので、この版では安全にスキップ（Issue通知）しています。"
    )


# =========================
# Leads + Reply drafts
# =========================
def collect_leads(theme: str) -> List[Dict[str, Any]]:
    """
    Issuesに出す「悩みURL」は SNS優先で集める。
    """
    leads: List[Dict[str, Any]] = []

    try:
        leads.extend(collect_bluesky(min(LEADS_PER_SOURCE, LEADS_TOTAL)))
    except Exception:
        pass

    try:
        leads.extend(collect_mastodon(min(LEADS_PER_SOURCE, LEADS_TOTAL)))
    except Exception:
        pass

    try:
        leads.extend(collect_reddit(min(LEADS_PER_SOURCE, LEADS_TOTAL)))
    except Exception:
        pass

    # Xはreadsが厳しいので軽く
    try:
        leads.extend(collect_x_limited(theme))
    except Exception:
        pass

    # URL重複除去
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in leads:
        u = it.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    return uniq[:LEADS_TOTAL]


def openai_generate_reply(client: OpenAI, post_text: str, tool_url: str) -> str:
    prompt = (
        "You write a short, natural, polite reply to an online post.\n"
        "Rules:\n"
        "- Tone: kind, non-spammy, helpful.\n"
        "- End with a gentle question.\n"
        "- Append the tool URL at the end on a new line.\n"
        "- Do NOT mention \"AI\", \"automation\", \"bot\".\n"
        "- Keep it under 280 characters if possible.\n\n"
        "Post:\n"
        + (post_text or "")
        + "\n\nTool URL:\n"
        + (tool_url or "")
    ).strip()

    res = client.chat.completions.create(
        model=MODEL_REPLY,
        messages=[{"role": "user", "content": prompt}],
    )
    txt = (res.choices[0].message.content or "").strip()
    if tool_url and tool_url not in txt:
        txt = txt.rstrip() + "\n" + tool_url
    return txt


def build_leads_issue_body(leads: List[Dict[str, Any]], tool_url: str) -> str:
    lines: List[str] = []
    lines.append("以下は「手動返信用」の候補です。\n")
    lines.append("形式:\n- 対象の悩みURL\n- 返信文（末尾にツールURL入り）\n")
    lines.append("----\n")

    for i, it in enumerate(leads, 1):
        url = it.get("url", "")
        src = it.get("source", "")
        lines.append(f"#{i} [{src}]")
        lines.append(url)
        lines.append("返信文:")
        lines.append((it.get("reply", "") or "").strip())
        lines.append("\n----\n")

    return "\n".join(lines)


def chunk_and_create_issues(title_prefix: str, body: str, max_chars: int = 60000) -> None:
    if len(body) <= max_chars:
        create_github_issue(title_prefix, body)
        return

    parts: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for block in body.split("\n----\n"):
        blk = block + "\n----\n"
        if cur_len + len(blk) > max_chars and cur:
            parts.append("".join(cur))
            cur = [blk]
            cur_len = len(blk)
        else:
            cur.append(blk)
            cur_len += len(blk)
    if cur:
        parts.append("".join(cur))

    for idx, p in enumerate(parts, 1):
        create_github_issue(f"{title_prefix} (part {idx}/{len(parts)})", p)


# =========================
# Main
# =========================
def main() -> None:
    ensure_dirs()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        create_github_issue("[Goliath] Missing OPENAI_API_KEY", "OPENAI_API_KEY が未設定です。")
        return
    client = OpenAI(api_key=api_key)

    pages_base = get_repo_pages_base()

    # 1) Collector -> pick best theme
    items = collector_real()
    best = pick_best_theme(items)
    theme = best["theme"]
    best_item = best["best_item"]
    best_score = best["best_score"]
    best_table = best["best_table"]

    # 2) Cluster
    cluster = cluster_20_around_theme(theme, items)

    created_at = now_utc_iso()
    tags = infer_tags_simple(theme)

    # タイトル（検索寄せ）
    search_title = make_search_title(theme, tags)

    # BAN_WORDSはslug生成前に弾く（壊れない保険）
    if any(w in (search_title or "").lower() for w in BAN_WORDS):
        search_title = re.sub("|".join([re.escape(w) for w in BAN_WORDS]), "tool", search_title, flags=re.IGNORECASE).strip()
        if not search_title:
            search_title = "tool"

    slug = slugify(search_title)
    seen = existing_tool_slugs()
    slug = ensure_unique_slug(slug, seen)

    folder = f"{int(time.time())}-{slug}"
    page_dir = f"{PAGES_DIR}/{folder}"
    os.makedirs(page_dir, exist_ok=True)

    public_url = f"{pages_base}{ROOT}/pages/{folder}/"
    canonical = public_url.rstrip("/")

    # 3) Build with Auto-fix loop (max 5)
    prompt = build_prompt(theme, cluster, canonical)
    html = openai_generate_html(client, prompt)

    ok, msg = validate_html(html)
    attempts = 0
    while not ok and attempts < 5:
        attempts += 1
        fix_prompt = prompt_for_fix(msg, html)
        diff = client.chat.completions.create(
            model=MODEL_BUILD,
            messages=[{"role": "user", "content": fix_prompt}],
        ).choices[0].message.content or ""

        patched = apply_unified_diff_to_text(html, diff.strip())
        if patched is None:
            regen_prompt = build_prompt(theme, cluster, canonical) + f"\n\nFix validation error: {msg}\nReturn ONLY corrected HTML.\n"
            html = openai_generate_html(client, regen_prompt)
        else:
            html = patched

        ok, msg = validate_html(html)

    if not ok:
        create_github_issue(
            title=f"[Goliath] Build failed after 5 fixes: {slug}",
            body=(
                f"- theme: {theme}\n"
                f"- best_source: {best_item.get('source')}\n"
                f"- best_url: {best_item.get('url')}\n"
                f"- best_score: {best_score}\n"
                f"- score_breakdown: {json.dumps(best_table, ensure_ascii=False)}\n"
                f"- error: {msg}\n"
                f"- created_at: {created_at}\n"
            )
        )
        return

    # 4) Related sites and inject
    raw_db = read_json(DB_PATH, [])
    all_entries = normalize_db(raw_db)
    seed_sites = load_seed_sites()
    related = pick_related(tags, all_entries, seed_sites, k=8)
    html = inject_related_json(html, related)

    # 5) Save page
    page_path = f"{page_dir}/index.html"
    write_text(page_path, html)

    # 6) Update DB + hub index + sitemap/robots
    entry = {
        "id": stable_id(created_at, slug),
        "title": (theme or "")[:80],
        "search_title": search_title,
        "created_at": created_at,
        "path": f"./pages/{folder}",
        "public_url": public_url,
        "tags": tags,
        "source_urls": (cluster.get("urls", []) or [])[:20],
        "related": related,
        "best_source": best_item.get("source"),
        "best_url": best_item.get("url"),
        "best_score": best_score,
        "best_score_breakdown": best_table,
        "score_keys": cluster.get("keys", []),
    }
    update_db_and_index(entry, all_entries, pages_base)

    # tool_history（重複回避用）に追記：機能追加だけで既存を壊さない
    try:
        hist = _load_tool_history()
        hist.append({
            "created_at": created_at,
            "tool_slug": slug,
            "theme": theme,
            "tags": tags,
            "fp": _fingerprint(theme, tags),
            "public_url": public_url,
        })
        _save_tool_history(hist)
    except Exception:
        pass

    # 7) Auto-post (announce)
    if ENABLE_AUTO_POST:
        short_value = (search_title or "New tool").strip()[:80]
        post_text = f"{short_value}\n{public_url}"
        post_bluesky(post_text)
        post_mastodon(post_text)
        post_x(post_text)

    # 8) Lead collection + draft replies
    leads = collect_leads(theme)

    scored: List[Tuple[int, Dict[str, Any]]] = []
    for it in leads:
        s, _tbl = score_item(it.get("text", ""), it.get("url", ""), it.get("meta", {}) or {})
        scored.append((s, it))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [it for _s, it in scored[:max(LEADS_TOTAL, 10)]]

    final: List[Dict[str, Any]] = []
    for it in top[:LEADS_TOTAL]:
        txt = it.get("text", "") or ""
        reply = openai_generate_reply(client, txt, public_url)
        it2 = dict(it)
        it2["reply"] = reply
        final.append(it2)

    header: List[str] = []
    header.append(f"Tool URL: {public_url}")
    header.append(f"Theme: {theme}")
    header.append(f"Search title: {search_title}")
    header.append(f"Picked from: {best_item.get('source')} / {best_item.get('url')}")
    header.append(f"Best score: {best_score} / breakdown: {json.dumps(best_table, ensure_ascii=False)}")
    header.append(f"Tags: {', '.join(tags)}")
    header.append(f"Related sites count: {len(related)}")
    header.append("")
    header.append("")

    body = "\n".join(header) + build_leads_issue_body(final, public_url)

    chunk_and_create_issues(
        title_prefix=f"[Goliath] Reply candidates ({LEADS_TOTAL}) + new tool: {slug}",
        body=body,
        max_chars=60000
    )


if __name__ == "__main__":
    main()

