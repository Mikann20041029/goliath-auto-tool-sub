import os
import re
import json
import time
import glob
import random
import hashlib
import datetime
import math
from pathlib import Path
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple, Optional

import requests
from openai import OpenAI

# ============================================================
# COST LOCK: cheapest only
# ============================================================
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
if MODEL != "gpt-4o-mini":
    raise RuntimeError(f"MODEL LOCK: only gpt-4o-mini allowed, got {MODEL}")

# Reply output cap (short)
MAX_OUTPUT_TOKENS_REPLY = int(os.getenv("MAX_OUTPUT_TOKENS_REPLY", "300"))
# Build output cap (needs to be larger than reply, but still bounded)
MAX_OUTPUT_TOKENS_BUILD = int(os.getenv("MAX_OUTPUT_TOKENS_BUILD", "2400"))
if MAX_OUTPUT_TOKENS_BUILD > 3200:
    raise RuntimeError("MAX_OUTPUT_TOKENS_BUILD too large; cap at <= 3200 to control cost")

# ============================================================
# Optional SNS libs
# ============================================================
try:
    from atproto import Client as BskyClient  # type: ignore
except Exception:
    BskyClient = None

try:
    from mastodon import Mastodon  # type: ignore
except Exception:
    Mastodon = None

# External collector (your existing module)
try:
    from collectors import collect_bluesky as collect_bluesky_ext  # type: ignore
except Exception:
    collect_bluesky_ext = None

# ============================================================
# Paths / Constants
# ============================================================
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

AFFILIATES_PATH = f"{ROOT}/affiliates.json"

ENABLE_AUTO_POST = os.getenv("ENABLE_AUTO_POST", "1") == "1"

LEADS_TOTAL = int(os.getenv("LEADS_TOTAL", "100"))
LEADS_PER_SOURCE = int(os.getenv("LEADS_PER_SOURCE", "60"))

COLLECT_HN = int(os.getenv("COLLECT_HN", "50"))
COLLECT_BSKY = int(os.getenv("COLLECT_BSKY", "50"))
COLLECT_MASTODON = int(os.getenv("COLLECT_MASTODON", "50"))

# Click log endpoint (Cloudflare Worker / GAS)
CLICK_LOG_ENDPOINT = os.getenv("CLICK_LOG_ENDPOINT", "").strip()

# Unsplash
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY", "").strip()
UNSPLASH_QUERIES = [
    "abstract gradient",
    "minimalist background",
    "soft colors",
    "modern texture",
]

# ============================================================
# Keywords / Scoring
# ============================================================
KEYWORDS = [
    "help", "need help", "anyone know", "any idea", "how do i", "how to", "can't", "cannot", "won't",
    "stuck", "blocked", "error", "bug", "issue", "problem", "failed", "failure", "broken", "crash",
    "exception", "traceback", "deploy", "build failed", "github actions", "workflow", "docker", "npm", "pip",
    "api", "oauth", "convert", "converter", "calculator", "template", "compare", "timezone"
]
BAN_WORDS = ["template", "boilerplate", "starter", "scaffold"]

# Fixed 12 genres (must match exactly)
GENRES = [
    "Web/Hosting",
    "Dev/Tools",
    "AI/Automation",
    "Security/Privacy",
    "Media: Video/Audio",
    "PDF/Docs",
    "Images/Design",
    "Data/Spreadsheets",
    "Business/Accounting/Tax",
    "Marketing/Social",
    "Productivity",
    "Education/Language",
]

# ============================================================
# Utilities
# ============================================================
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


def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _fingerprint(theme: str, tags: List[str]) -> str:
    base = _norm(theme) + "|" + "|".join(sorted(_norm(t) for t in (tags or [])))
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _too_similar(a: str, b: str) -> bool:
    A = set(_norm(a).split())
    B = set(_norm(b).split())
    if not A or not B:
        return False
    j = len(A & B) / max(1, len(A | B))
    return j >= 0.80


def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d{10,}", "TS", s)
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
    new_sig = _normalize_text(new_index_html)
    best = 0.0
    for old in _read_existing_page_signatures():
        r = SequenceMatcher(a=new_sig, b=old).ratio()
        if r > best:
            best = r
    if best >= threshold:
        return (-200, best)
    return (0, best)


# ============================================================
# Tool history
# ============================================================
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
    slugs |= _existing_slugs_from_pages_dir(Path(PAGES_DIR))
    slugs |= _existing_slugs_from_pages_dir(Path(__file__).resolve().parent / "pages")
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


# ============================================================
# Title / Tags / Genre
# ============================================================
def make_search_title(theme: str, tags: List[str]) -> str:
    t = (theme or "").strip()
    tl = t.lower()

    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"^(show hn:\s*)", "", t, flags=re.IGNORECASE)

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
        suffix = "tool"

    kws = []
    for k in ["tax", "subscription", "pricing", "timezone", "time zone", "convert", "calculator", "pdf", "image", "video", "dns", "ssl", "oauth", "api"]:
        if k in tl:
            kws.append(k)
    core = kws[0] if kws else t[:60]

    title = f"{core} {suffix}".strip()
    title = re.sub(r"\s+", " ", title)
    return title[:80]


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
        "notes": "productivity",
        "pdf": "pdf",
        "ocr": "pdf",
        "ffmpeg": "media",
        "video": "media",
        "audio": "media",
        "image": "images",
        "resize": "images",
        "dns": "web",
        "ssl": "web",
        "cloudflare": "web",
        "github": "dev",
        "ci": "dev",
        "api": "dev",
        "oauth": "security",
        "vpn": "security",
        "password": "security",
        "2fa": "security",
        "seo": "marketing",
        "analytics": "marketing",
        "email": "marketing",
        "english": "education",
        "toeic": "education",
        "eiken": "education",
    }
    for k, v in rules.items():
        if k in t and v not in tags:
            tags.append(v)
    if not tags:
        tags = ["tools"]
    return tags[:8]


def infer_genre(theme: str, tags: List[str]) -> str:
    t = (theme or "").lower()
    tagset = set([x.lower() for x in (tags or [])])

    if any(k in t for k in ["dns", "ssl", "cloudflare", "vercel", "uptime", "github pages"]) or "web" in tagset:
        return "Web/Hosting"
    if any(k in t for k in ["ide", "git", "ci", "cd", "logging", "api", "sdk"]) or "dev" in tagset:
        return "Dev/Tools"
    if any(k in t for k in ["llm", "agent", "automation", "workflow", "openai", "chatgpt"]) or "ai" in tagset:
        return "AI/Automation"
    if any(k in t for k in ["vpn", "password", "2fa", "malware", "privacy", "oauth"]) or "security" in tagset:
        return "Security/Privacy"
    if any(k in t for k in ["ffmpeg", "video", "audio", "subtitle", "compress"]) or "media" in tagset:
        return "Media: Video/Audio"
    if any(k in t for k in ["pdf", "ocr", "merge", "sign", "doc"]) or "pdf" in tagset:
        return "PDF/Docs"
    if any(k in t for k in ["image", "resize", "optimize", "remove bg", "design"]) or "images" in tagset:
        return "Images/Design"
    if any(k in t for k in ["csv", "excel", "spreadsheet", "google sheets", "finance template"]) or "data" in tagset:
        return "Data/Spreadsheets"
    if any(k in t for k in ["invoice", "bookkeeping", "tax", "accounting"]) or "finance" in tagset:
        return "Business/Accounting/Tax"
    if any(k in t for k in ["seo", "analytics", "scheduling", "email", "social"]) or "marketing" in tagset:
        return "Marketing/Social"
    if any(k in t for k in ["notes", "task", "calendar", "focus", "template", "checklist"]) or "productivity" in tagset:
        return "Productivity"
    if any(k in t for k in ["english", "language", "toeic", "eiken", "exam", "study"]) or "education" in tagset:
        return "Education/Language"

    # default
    return "Dev/Tools"


# ============================================================
# Scoring
# ============================================================
def score_item(text: str, url: str, meta: Dict[str, Any]) -> Tuple[int, Dict[str, int]]:
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

    # anti-duplicate penalty
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


# ============================================================
# GitHub Issue
# ============================================================
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


# ============================================================
# HN Collector
# ============================================================
def hn_search(query: str, limit: int = 30, ask_only: bool = False) -> List[Dict[str, Any]]:
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
    except Exception as e:
        print("[hn] exc", repr(e))
        return []


def collect_hn(limit: int) -> List[Dict[str, Any]]:
    queries = ["tool for", "is there a tool", "convert", "calculator", "compare pricing", "timezone"]
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


# ============================================================
# Bluesky Collector (DEBUG: reasons visible in Actions logs)
# ============================================================
def collect_bluesky(limit: int) -> List[Dict[str, Any]]:
    """
    Blueskyは「0の理由」を確実にログに出す:
    - atproto libの有無
    - secretsの有無
    - collectors.collect_bluesky(ext)の有無
    - 実行例外
    """
    h = os.getenv("BSKY_HANDLE", "").strip()
    p = os.getenv("BSKY_PASSWORD", "").strip()

    print("[bsky] preflight",
          json.dumps({
              "has_handle": bool(h),
              "has_password": bool(p),
              "has_atproto": (BskyClient is not None),
              "has_ext_collector": bool(collect_bluesky_ext),
              "limit": limit
          }, ensure_ascii=False))

    # Prefer your external collector if present (it might do search/timeline efficiently)
    if collect_bluesky_ext:
        try:
            # Expected signature: collect_bluesky_ext(KEYWORDS, limit_per_query=25) etc.
            # We'll call it in a safe way and normalize output.
            out = collect_bluesky_ext(KEYWORDS, limit_per_query=25)  # type: ignore
            if not isinstance(out, list):
                print("[bsky] ext_collector returned non-list")
                return []
            norm: List[Dict[str, Any]] = []
            for it in out:
                if not isinstance(it, dict):
                    continue
                url = (it.get("url") or "").strip()
                txt = (it.get("text") or "").strip()
                if not url or not txt:
                    continue
                norm.append({"source": "Bluesky", "text": txt[:300], "url": url, "meta": it.get("meta") or {}})
            print("[bsky] ext_collector ok", {"count": len(norm)})
            return norm[:limit]
        except Exception as e:
            print("[bsky] ext_collector EXC", repr(e))
            # fallthrough to direct atproto if possible

    # If no atproto or no secrets -> impossible
    if BskyClient is None:
        print("[bsky] skip: atproto not installed")
        return []
    if not (h and p):
        print("[bsky] skip: missing BSKY_HANDLE/BSKY_PASSWORD")
        return []

    # Direct atproto minimal (search posts)
    out: List[Dict[str, Any]] = []
    queries = [
        "need a tool",
        "is there a tool",
        "converter",
        "calculator",
        "timezone",
        "pricing",
        "subscription",
        "pdf",
        "image resize",
        "ffmpeg",
    ]

    def bsky_uri_to_url(uri: str, did: str = "") -> str:
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

    try:
        c = BskyClient()
        c.login(h, p)
        print("[bsky] login ok")
    except Exception as e:
        print("[bsky] login EXC", repr(e))
        return []

    for q in queries:
        try:
            res = c.app.bsky.feed.search_posts({"q": q, "limit": 25})
            resd = _to_dict(res)
            posts = _to_list(resd.get("posts"))
            print("[bsky] search", {"q": q, "posts": len(posts), "keys": list(resd.keys())})

            for raw in posts:
                post = _to_dict(raw)
                base = post.get("post") if isinstance(post.get("post"), dict) else post

                rec = _to_dict(base.get("record"))
                txt = (rec.get("text") or "").strip()
                if not txt:
                    continue

                uri = (base.get("uri") or "").strip()
                author = _to_dict(base.get("author"))
                did = (author.get("did") or "").strip()
                url = bsky_uri_to_url(uri, did) or uri
                if not url:
                    continue

                out.append({"source": "Bluesky", "text": txt[:300], "url": url, "meta": {"q": q}})
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break

        except Exception as e:
            print("[bsky] search EXC", {"q": q, "err": repr(e)})
            continue

    # dedupe
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in out:
        u = it.get("url", "")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)

    print("[bsky] out_total", len(uniq))
    return uniq[:limit]


# ============================================================
# Mastodon Collector
# ============================================================
def collect_mastodon(limit: int) -> List[Dict[str, Any]]:
    base = os.getenv("MASTODON_API_BASE", "").strip()
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "").strip()
    print("[mastodon] preflight", {"has_base": bool(base), "has_token": bool(tok), "has_lib": (Mastodon is not None), "limit": limit})

    if not base or Mastodon is None:
        return []

    out: List[Dict[str, Any]] = []
    try:
        m = Mastodon(access_token=tok if tok else None, api_base_url=base)

        queries = [
            "help", "need help", "anyone know", "how do i", "how to", "please help",
            "error", "bug", "issue", "problem", "failed", "broken", "crash", "traceback",
            "github actions", "docker", "npm", "pip", "api", "oauth",
            "need a tool", "is there a tool", "tool for",
            "convert", "converter", "calculator", "template", "compare", "timezone",
        ]

        if tok:
            for q in queries:
                try:
                    res = m.search_v2(q=q, result_type="statuses", limit=25)
                    statuses = (res or {}).get("statuses", []) or []
                    for st in statuses:
                        txt = re.sub(r"<[^>]+>", "", st.get("content", "") or "")
                        url = st.get("url", "") or ""
                        if txt and url:
                            out.append({"source": "Mastodon", "text": txt[:300], "url": url, "meta": {"q": q}})
                        if len(out) >= limit:
                            break
                    if len(out) >= limit:
                        break
                except Exception as e:
                    print("[mastodon] search EXC", {"q": q, "err": repr(e)})

        if len(out) < limit:
            try:
                statuses = m.timeline_public(limit=80)
                for st in statuses:
                    txt = re.sub(r"<[^>]+>", "", st.get("content", "") or "")
                    t = txt.lower()
                    if not any(k in t for k in KEYWORDS):
                        continue
                    url = st.get("url", "") or ""
                    if txt and url:
                        out.append({"source": "Mastodon", "text": txt[:300], "url": url, "meta": {"from": "timeline"}})
                    if len(out) >= limit:
                        break
            except Exception as e:
                print("[mastodon] timeline EXC", repr(e))

        seen = set()
        uniq: List[Dict[str, Any]] = []
        for it in out:
            u = it.get("url", "")
            if not u or u in seen:
                continue
            seen.add(u)
            uniq.append(it)

        print("[mastodon] out_total", len(uniq))
        return uniq[:limit]
    except Exception as e:
        print("[mastodon] top EXC", repr(e))
        return out[:limit]


# ============================================================
# Reddit Collector (optional)
# ============================================================
def collect_reddit(limit: int) -> List[Dict[str, Any]]:
    cid = os.getenv("REDDIT_CLIENT_ID", "")
    csec = os.getenv("REDDIT_CLIENT_SECRET", "")
    user = os.getenv("REDDIT_USERNAME", "")
    pw = os.getenv("REDDIT_PASSWORD", "")
    ua = os.getenv("REDDIT_USER_AGENT", "")

    if not (cid and csec and user and pw and ua):
        print("[reddit] skip: missing secrets")
        return []

    try:
        import praw  # type: ignore
    except Exception:
        print("[reddit] skip: praw not installed")
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
                if url and txt:
                    out.append({"source": "Reddit", "text": txt[:300], "url": url, "meta": {"q": q}})
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break
    except Exception as e:
        print("[reddit] exc", repr(e))

    print("[reddit] out_total", len(out))
    return out[:limit]


# ============================================================
# X limited (optional)
# ============================================================
def collect_x_limited(theme: str) -> List[Dict[str, Any]]:
    runs_per_month = int(os.getenv("RUNS_PER_MONTH", "90"))
    max_reads_month = int(os.getenv("X_READS_PER_MONTH", "100"))
    max_reads_run = max(1, max_reads_month // max(1, runs_per_month))

    api_key = os.getenv("X_API_KEY", "")
    api_secret = os.getenv("X_API_SECRET", "")
    access_token = os.getenv("X_ACCESS_TOKEN", "")
    access_secret = os.getenv("X_ACCESS_SECRET", "")

    if not (api_key and api_secret and access_token and access_secret):
        print("[x] skip: missing secrets")
        return []

    try:
        import tweepy  # type: ignore
    except Exception:
        print("[x] skip: tweepy not installed")
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
            print("[x] skip: get_me failed")
            return []
        user_id = me.data.id

        resp = client.get_users_mentions(id=user_id, max_results=min(5, max_reads_run))
        if not resp or not getattr(resp, "data", None):
            return []

        for tw in resp.data[:max_reads_run]:
            txt = (tw.text or "")[:300]
            url = f"https://x.com/i/web/status/{tw.id}"
            out.append({"source": "X", "text": txt, "url": url, "meta": {}})
    except Exception as e:
        print("[x] exc", repr(e))
        return []

    print("[x] out_total", len(out))
    return out


# ============================================================
# Collector orchestration
# ============================================================
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


# ============================================================
# Cluster / Pick theme
# ============================================================
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
        "scored": scored,
    }


def cluster_20_around_theme(theme: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    t = (theme or "").lower()
    keys: List[str] = []
    for k in ["convert", "calculator", "compare", "timezone", "template", "subscription", "pricing", "tax", "checklist", "pdf", "image", "video"]:
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


# ============================================================
# Related sites
# ============================================================
def load_seed_sites() -> List[Dict[str, Any]]:
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

    # fallback fill
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


# ============================================================
# Unsplash background (server-side pick + inject)
# ============================================================
def unsplash_pick_image_url() -> str:
    """
    Prefer Unsplash API random photo with fixed queries.
    Fallback to source.unsplash.com (no key).
    """
    q = random.choice(UNSPLASH_QUERIES)
    if UNSPLASH_ACCESS_KEY:
        try:
            url = "https://api.unsplash.com/photos/random"
            params = {
                "query": q,
                "orientation": "landscape",
                "content_filter": "high",
            }
            headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}
            r = requests.get(url, params=params, headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json()
            # prefer regular
            u = (((data or {}).get("urls") or {}).get("regular") or "").strip()
            if u:
                print("[unsplash] picked via api", {"q": q})
                return u
        except Exception as e:
            print("[unsplash] api EXC", repr(e))

    # fallback
    print("[unsplash] fallback via source", {"q": q})
    return f"https://source.unsplash.com/featured/1600x900/?{requests.utils.quote(q)}"


def inject_unsplash_bg(html: str, bg_url: str) -> str:
    """
    Adds:
    - CSS background image on body
    - overlay div for readability
    """
    if not bg_url:
        return html

    style = f"""
<style id="goliath-bg-style">
  body {{
    background-image: url('{bg_url}');
    background-size: cover;
    background-position: center;
    background-attachment: fixed;
  }}
  .goliath-bg-overlay {{
    position: fixed;
    inset: 0;
    background: rgba(255,255,255,0.60);
    backdrop-filter: blur(6px);
    -webkit-backdrop-filter: blur(6px);
    pointer-events: none;
    z-index: -1;
  }}
  .dark .goliath-bg-overlay {{
    background: rgba(2,6,23,0.55);
  }}
</style>
""".strip()

    # inject overlay after <body ...>
    out = html
    if "goliath-bg-style" in html:
        return html

    out = re.sub(r"</head>", style + "\n</head>", out, flags=re.IGNORECASE)
    out = re.sub(r"<body([^>]*)>", r"<body\1>\n<div class=\"goliath-bg-overlay\"></div>", out, flags=re.IGNORECASE)
    return out


# ============================================================
# Affiliates (sanitize + select + inject + click logging)
# ============================================================
def load_affiliates() -> Dict[str, List[Dict[str, Any]]]:
    data = read_json(AFFILIATES_PATH, {})
    if not isinstance(data, dict):
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for k, v in data.items():
        if k not in GENRES:
            continue
        if isinstance(v, list):
            out[k] = [x for x in v if isinstance(x, dict)]
    return out


def _has_script(html: str) -> bool:
    return bool(re.search(r"<\s*script\b", html or "", flags=re.IGNORECASE))


def _ensure_a_attrs(html: str) -> str:
    """
    - add target="_blank"
    - add rel="nofollow sponsored noopener" (merge if exists)
    """
    if not html:
        return html

    def repl_a(m: re.Match) -> str:
        tag = m.group(0)
        # target
        if re.search(r"\btarget\s*=", tag, flags=re.IGNORECASE) is None:
            tag = tag[:-1] + ' target="_blank">'
        # rel
        rel_m = re.search(r'\brel\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', tag, flags=re.IGNORECASE)
        need = ["nofollow", "sponsored", "noopener"]
        if rel_m:
            raw = rel_m.group(1)
            val = raw.strip("\"'").strip()
            parts = set([p for p in re.split(r"\s+", val) if p])
            for n in need:
                parts.add(n)
            new_rel = 'rel="' + " ".join(sorted(parts)) + '"'
            tag = re.sub(r'\brel\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', new_rel, tag, flags=re.IGNORECASE)
        else:
            tag = tag[:-1] + ' rel="nofollow sponsored noopener">'
        return tag

    return re.sub(r"<\s*a\b[^>]*>", repl_a, html, flags=re.IGNORECASE)


def sanitize_affiliate_html(html: str) -> Optional[str]:
    if not html:
        return None
    if _has_script(html):
        return None
    html = _ensure_a_attrs(html)
    return html


def pick_top_ads(aff: Dict[str, List[Dict[str, Any]]], genre: str, n: int = 2) -> List[Dict[str, Any]]:
    arr = aff.get(genre, []) or []
    clean: List[Dict[str, Any]] = []
    for it in arr:
        aid = (it.get("id") or "").strip()
        title = (it.get("title") or "").strip()
        raw_html = (it.get("html") or "")
        pr = int(it.get("priority", 50) or 50)

        if not aid or not raw_html:
            continue
        s = sanitize_affiliate_html(raw_html)
        if not s:
            continue
        clean.append({"id": aid, "title": title, "html": s, "priority": pr})

    clean.sort(key=lambda x: int(x.get("priority", 0)), reverse=True)
    return clean[:max(1, min(3, n))]


def inject_affiliate_slots(html: str, page_id: str, page_url: str, genre: str, ads: List[Dict[str, Any]]) -> str:
    """
    Requires placeholders in HTML. If missing, inject near top of main content.
    Adds JS to:
      - decorate affiliate links with data-ad-id
      - send click logs to endpoint (if configured)
    """
    if not ads:
        return html

    # Render block HTML
    cards = []
    for ad in ads:
        aid = ad["id"]
        title = (ad.get("title") or "").strip()
        # wrap inside container so we can attach dataset
        cards.append(
            f"""
<div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white/70 dark:bg-slate-900/60 p-4">
  <div class="text-sm font-semibold mb-2">{title if title else "Recommended"}</div>
  <div class="goliath-ad" data-ad-id="{aid}">
    {ad["html"]}
  </div>
</div>
""".strip()
        )

    cards_html = "\n".join(cards)
block = f"""
<section id="aff-section" class="mt-6">
  <div class="text-sm font-semibold opacity-80 mb-2" data-i18n="ads.title"></div>
  <div class="grid gap-3">
    {cards_html}
  </div>
</section>
""".strip()


    out = html

    # Placeholder marker preferred
    if "AFF_SLOT" in out:
        out = out.replace("<!-- AFF_SLOT -->", block)
    else:
        # Inject after first <main> or after body start if main missing
        out2 = re.sub(r"(<main[^>]*>)", r"\1\n" + block, out, flags=re.IGNORECASE)
        if out2 == out:
            out2 = re.sub(r"(<body[^>]*>)", r"\1\n" + block, out, flags=re.IGNORECASE)
        out = out2


    # Inject click logger script
    js = f"""
<script id="goliath-ads-js">
(function() {{
  const endpoint = {json.dumps(CLICK_LOG_ENDPOINT)};
  const payloadBase = {{
    page_id: {json.dumps(page_id)},
    page_url: {json.dumps(page_url)},
    genre: {json.dumps(genre)}
  }};

  function send(ad_id) {{
    if (!endpoint) return;
    const data = Object.assign({{}}, payloadBase, {{ ad_id: ad_id, ts: new Date().toISOString() }});
    try {{
      // Prefer sendBeacon when available
      if (navigator.sendBeacon) {{
        const blob = new Blob([JSON.stringify(data)], {{ type: "application/json" }});
        navigator.sendBeacon(endpoint, blob);
        return;
      }}
    }} catch(e) {{}}
    try {{
      fetch(endpoint, {{
        method: "POST",
        headers: {{ "content-type": "application/json" }},
        body: JSON.stringify(data),
        keepalive: true
      }}).catch(()=>{{}});
    }} catch(e) {{}}
  }}

  // Attach click handler to any link inside .goliath-ad
  document.querySelectorAll(".goliath-ad").forEach(function(box) {{
    const adId = box.getAttribute("data-ad-id") || "";
    if (!adId) return;
    box.querySelectorAll("a").forEach(function(a) {{
      a.addEventListener("click", function() {{
        send(adId);
      }}, {{ passive: true }});
    }});
  }});
}})();
</script>
""".strip()

    if "goliath-ads-js" not in out:
        out = re.sub(r"</body>", js + "\n</body>", out, flags=re.IGNORECASE)

    # Also add i18n key for ads title (if missing)
    out = ensure_i18n_key(out, "ads.title", {
        "en": "Sponsored / Recommended",
        "ja": "おすすめ（広告）",
        "fr": "Sponsorisé / Recommandé",
        "de": "Gesponsert / Empfohlen",
    })

    return out


def ensure_i18n_key(html: str, key: str, vals: Dict[str, str]) -> str:
    """
    If window.__I18N__ exists, inject missing key into each locale dict (best-effort string replace).
    This is a safe additive patch—if it fails, page still works.
    """
    if "__I18N__" not in html:
        return html

    out = html
    for lang, v in vals.items():
        # naive insertion: find "lang":{ ... } and add if key missing
        # We'll only add if key isn't already present.
        if re.search(rf"{re.escape(key)}\s*:", out):
            break

    # best-effort: insert near start of each lang dict
    for lang, v in vals.items():
        pat = rf"({lang}\s*:\s*\{{)"
        if re.search(pat, out):
            # only insert if that lang block does not already contain the key
            # (rough but prevents dup)
            lang_block_m = re.search(rf"{lang}\s*:\s*\{{([\s\S]*?)\}}\s*,", out)
            if lang_block_m and (key in lang_block_m.group(1)):
                continue
            out = re.sub(pat, rf"\1\n  {json.dumps(key)}: {json.dumps(v)},", out, count=1)
    return out


# ============================================================
# Builder prompt (adds AFF_SLOT placeholder + genre + unsplash hint)
# ============================================================
def build_prompt(theme: str, cluster: Dict[str, Any], canonical_url: str, genre: str) -> str:
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
- Use generous spacing, professional typography (Inter / Noto Sans JP via CDN)
- Include a dedicated placeholder comment exactly: <!-- AFF_SLOT --> near top of main content area.
- Include a subtle translucent white overlay style on content containers to improve readability over photo background.

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
- Provide a WORKING language switcher for JA/EN/FR/DE (mobile-friendly).
- The switcher MUST be a <select> with id="langSelect" and options: en,ja,fr,de (in that order).
- EVERY visible text on the page MUST be translatable via i18n keys (data-i18n, data-i18n-placeholder, data-i18n-title, data-i18n-aria).
- Provide a JS dictionary: window.__I18N__ = {{ en:{{...}}, ja:{{...}}, fr:{{...}}, de:{{...}} }}
- Save language in localStorage key "goliath_lang" (fallback "en"), set <html lang=".."> and re-render on change.

[Compliance / Footer]
- Auto-generate in-page sections for: Privacy Policy, Terms of Service, Disclaimer, About, Contact
- These must be accessible via footer links using anchors. All text i18n-driven.

[Related Sites]
- Include a "Related sites" section near bottom:
  - window.__RELATED__ = [];
  - Render it into the list on load. If empty, hide the section.

[Ads / Affiliate]
- Keep the placeholder <!-- AFF_SLOT --> where sponsored blocks can be injected later.
- DO NOT include any third-party scripts.

[Metadata]
- Include meta description/canonical.
- Canonical must be: {canonical_url}
- Add a meta tag: <meta name="goliath-genre" content="{genre}">

Return ONLY the final HTML.
""".strip()


def openai_generate_html(client: OpenAI, prompt: str) -> str:
    res = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_OUTPUT_TOKENS_BUILD,
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
    if 'id="langselect"' not in low:
        return False, "missing #langSelect"
    if low.count("data-i18n=") < 80:
        return False, "i18n coverage too small (data-i18n < 80)"
    if "data-i18n-placeholder" not in low:
        return False, "missing data-i18n-placeholder"
    if "__i18n__" not in low:
        return False, "missing window.__I18N__"
    if "goliath_lang" not in low:
        return False, "missing localStorage key goliath_lang handling"
    if "../../index.html" not in low and "../index.html" not in low:
        return False, "missing link back to hub (../../index.html)"
    if "aff_slot" not in low and "<!-- aff_slot -->" not in low:
        return False, "missing affiliate placeholder <!-- AFF_SLOT -->"
    return True, "ok"


def prompt_for_fix(error: str, html: str) -> str:
    return f"""
Return ONLY a unified diff patch for a single file named index.html.

Rules:
- Output ONLY the diff. No explanations.
- The patch MUST fix this validation error: {error}
- Do not remove required features: Tailwind CDN, SaaS layout, dark/light toggle, language switcher,
  footer policy sections, window.__RELATED__ rendering, link back to hub, <!-- AFF_SLOT --> placeholder.

Current index.html:
{html}
""".strip()


def apply_unified_diff_to_text(original: str, diff_text: str) -> Optional[str]:
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

        while oidx < len(orig_lines):
            result.append(orig_lines[oidx])
            oidx += 1

        return "\n".join(result) + ("\n" if original.endswith("\n") else "")
    except Exception:
        return None


# ============================================================
# Publishing / Hub / Sitemap
# ============================================================
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
        meta = f"{e.get('created_at','')} • {', '.join(safe_tags(e))} • {e.get('genre','')}"
        return (
            '<a class="block p-4 rounded-xl border border-slate-200 dark:border-slate-800 '
            'bg-white/70 dark:bg-slate-900/60 hover:bg-white/90 dark:hover:bg-slate-900/80 transition" '
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
            f'<button class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800 text-sm bg-white/70 dark:bg-slate-900/60" '
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
      <button id="themeBtn" class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800 bg-white/70 dark:bg-slate-900/60">Dark/Light</button>
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
        <input id="q" class="w-56 px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800 bg-white/70 dark:bg-slate-900/60 text-sm"
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
    const root = document.documentElement;
    const k = "goliath_theme";
    const saved = localStorage.getItem(k);
    if (saved === "dark") root.classList.add("dark");
    document.getElementById("themeBtn").onclick = () => {{
      root.classList.toggle("dark");
      localStorage.setItem(k, root.classList.contains("dark") ? "dark" : "light");
    }};

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
    build_sitemap_and_robots(all_entries, pages_base)


# ============================================================
# Posting (announce) - keep safe / optional
# ============================================================
def post_bluesky(text: str) -> None:
    h = os.getenv("BSKY_HANDLE", "")
    p = os.getenv("BSKY_PASSWORD", "")
    if not h or not p or BskyClient is None:
        return
    try:
        c = BskyClient()
        c.login(h, p)
        c.send_post(text=text)
    except Exception as e:
        print("[bsky] post EXC", repr(e))


def post_mastodon(text: str) -> None:
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "")
    base = os.getenv("MASTODON_API_BASE", "")
    if not tok or not base or Mastodon is None:
        return
    try:
        m = Mastodon(access_token=tok, api_base_url=base)
        m.status_post(text)
    except Exception as e:
        print("[mastodon] post EXC", repr(e))


def post_x(text: str) -> None:
    # safer: skip actual X posting here
    if not (os.getenv("X_API_KEY") and os.getenv("X_API_SECRET") and os.getenv("X_ACCESS_TOKEN") and os.getenv("X_ACCESS_TOKEN_SECRET")):
        return
    create_github_issue(
        title="[Goliath] X auto-post skipped (OAuth/plan dependent)",
        body="X投稿はプラン/権限/OAuth実装差で壊れやすいので、この版では安全にスキップ（Issue通知）しています。"
    )


# ============================================================
# Leads + Reply drafts (cheapest model only)
# ============================================================
def collect_leads(theme: str) -> List[Dict[str, Any]]:
    leads: List[Dict[str, Any]] = []

    try:
        b = collect_bluesky(min(LEADS_PER_SOURCE, LEADS_TOTAL))
        print(f"[counts] bluesky={len(b)}")
        leads.extend(b)
    except Exception as e:
        print("[counts] bluesky error:", repr(e))

    try:
        m = collect_mastodon(min(LEADS_PER_SOURCE, LEADS_TOTAL))
        print(f"[counts] mastodon={len(m)}")
        leads.extend(m)
    except Exception as e:
        print("[counts] mastodon error:", repr(e))

    try:
        r = collect_reddit(min(LEADS_PER_SOURCE, LEADS_TOTAL))
        print(f"[counts] reddit={len(r)}")
        leads.extend(r)
    except Exception as e:
        print("[counts] reddit error:", repr(e))

    try:
        x = collect_x_limited(theme)
        print(f"[counts] x={len(x)}")
        leads.extend(x)
    except Exception as e:
        print("[counts] x error:", repr(e))

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
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_OUTPUT_TOKENS_REPLY,
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


# ============================================================
# Priority auto-update (reads click logs) — OPTIONAL
# ============================================================
def update_affiliate_priorities_from_log(log_summary: Dict[str, int]) -> None:
    """
    log_summary: {ad_id: clicks} in window (last 7d/30d)
    priority = clamp(30, 90, 30 + log(1+clicks)*20)
    """
    if not log_summary:
        print("[aff] no log summary, skip priority update")
        return

    aff = read_json(AFFILIATES_PATH, {})
    if not isinstance(aff, dict):
        print("[aff] affiliates.json missing or invalid")
        return

    changed = 0
    for genre, items in aff.items():
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            aid = (it.get("id") or "").strip()
            if not aid:
                continue
            clicks = int(log_summary.get(aid, 0) or 0)
            score = math.log(1 + max(0, clicks))
            pr = int(30 + score * 20)
            pr = max(30, min(90, pr))
            old = int(it.get("priority", 50) or 50)
            if pr != old:
                it["priority"] = pr
                changed += 1

    if changed:
        write_json(AFFILIATES_PATH, aff)
        print("[aff] priority updated", changed)
    else:
        print("[aff] no changes")


# ============================================================
# Build prompt and auto-fix loop
# ============================================================
def openai_fix_with_diff(client: OpenAI, error: str, html: str) -> str:
    fix_prompt = prompt_for_fix(error, html)
    diff = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": fix_prompt}],
        max_tokens=min(1200, MAX_OUTPUT_TOKENS_BUILD),
    ).choices[0].message.content or ""
    patched = apply_unified_diff_to_text(html, diff.strip())
    if patched is not None:
        return patched

    regen_prompt = "Fix validation error: " + error + "\nReturn ONLY corrected HTML.\n\n" + html
    res = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": regen_prompt}],
        max_tokens=MAX_OUTPUT_TOKENS_BUILD,
    )
    return extract_html_only(res.choices[0].message.content or "")


# ============================================================
# Main
# ============================================================
def main() -> None:
    ensure_dirs()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        create_github_issue("[Goliath] Missing OPENAI_API_KEY", "OPENAI_API_KEY が未設定です。")
        return
    client = OpenAI(api_key=api_key)

    pages_base = get_repo_pages_base()

    # 1) Collect -> pick theme
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
    genre = infer_genre(theme, tags)

    # title
    search_title = make_search_title(theme, tags)
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

    # 3) Build (bounded) + auto-fix (max 5)
    prompt = build_prompt(theme, cluster, canonical, genre)
    html = openai_generate_html(client, prompt)

    ok, msg = validate_html(html)
    attempts = 0
    while not ok and attempts < 5:
        attempts += 1
        html = openai_fix_with_diff(client, msg, html)
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

    # 4) Related sites
    raw_db = read_json(DB_PATH, [])
    all_entries = normalize_db(raw_db)
    seed_sites = load_seed_sites()
    related = pick_related(tags, all_entries, seed_sites, k=8)
    html = inject_related_json(html, related)

    # 5) Unsplash background (server-side pick + inject)
    bg_url = unsplash_pick_image_url()
    html = inject_unsplash_bg(html, bg_url)

    # 6) Affiliate inject (genre-based)
    aff = load_affiliates()
    top_ads = pick_top_ads(aff, genre, n=2)
    page_id = stable_id(created_at, slug)
    html = inject_affiliate_slots(html, page_id=page_id, page_url=public_url, genre=genre, ads=top_ads)

    # 7) Save page
    page_path = f"{page_dir}/index.html"
    write_text(page_path, html)

    # 8) Update DB + hub
    entry = {
        "id": page_id,
        "title": (theme or "")[:80],
        "search_title": search_title,
        "created_at": created_at,
        "path": f"./pages/{folder}",
        "public_url": public_url,
        "tags": tags,
        "genre": genre,
        "source_urls": (cluster.get("urls", []) or [])[:20],
        "related": related,
        "best_source": best_item.get("source"),
        "best_url": best_item.get("url"),
        "best_score": best_score,
        "best_score_breakdown": best_table,
        "score_keys": cluster.get("keys", []),
        "unsplash_bg": bg_url,
        "ads_injected": [a.get("id") for a in top_ads],
    }
    update_db_and_index(entry, all_entries, pages_base)

    # 9) tool_history
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
    except Exception as e:
        print("[history] exc", repr(e))

    # 10) Auto-post (announce)
    if ENABLE_AUTO_POST:
        short_value = (search_title or "New tool").strip()[:80]
        post_text = f"{short_value}\n{public_url}"
        post_bluesky(post_text)
        post_mastodon(post_text)
        post_x(post_text)

    # 11) Leads + replies (cheapest model only)
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
    header.append(f"Genre: {genre}")
    header.append(f"Picked from: {best_item.get('source')} / {best_item.get('url')}")
    header.append(f"Best score: {best_score} / breakdown: {json.dumps(best_table, ensure_ascii=False)}")
    header.append(f"Tags: {', '.join(tags)}")
    header.append(f"Related sites count: {len(related)}")
    header.append(f"Unsplash bg: {bg_url}")
    header.append(f"Ads injected: {', '.join([a.get('id','') for a in top_ads]) if top_ads else '(none)'}")
    header.append("")
    header.append("Bluesky debug hint:")
    header.append("- In Actions logs, search '[bsky] preflight' and '[bsky] search' to see exact reason for 0.")
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

