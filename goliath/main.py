import os
import re
import json
import time
import hashlib
import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
from openai import OpenAI

# Optional
try:
    from atproto import Client as BskyClient
except Exception:
    BskyClient = None

try:
    from mastodon import Mastodon
except Exception:
    Mastodon = None


ROOT = "goliath"
DB_PATH = f"{ROOT}/db.json"
SEED_SITES_PATH = f"{ROOT}/sites.seed.json"

# -------- utils --------

def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def stable_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:16]

def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


# -------- data model --------
# tools.json をあなたが用意する想定（既存ツール一覧）
# 形式: [{"title":"PDF to ...","url":"https://...","tags":["pdf","convert"]}, ...]
TOOLS_PATH = f"{ROOT}/tools.json"

def load_tools() -> List[Dict[str, Any]]:
    if os.path.exists(TOOLS_PATH):
        return read_json(TOOLS_PATH, [])
    # ない場合は“最低限”のプレースホルダ（あなたが後で埋める）
    return [
        {"title": "Hub", "url": "https://mikann20041029.github.io/hub/", "tags": ["hub", "tools"]}
    ]

def load_seed_sites() -> List[Dict[str, Any]]:
    if os.path.exists(SEED_SITES_PATH):
        return read_json(SEED_SITES_PATH, [])
    return []


# -------- collectors (read-only) --------

def collect_hn(max_items: int) -> List[Dict[str, Any]]:
    """
    HN: Algolia Search API (read-only)
    """
    out: List[Dict[str, Any]] = []
    # “tool / convert / calculator / template / alternative” 系で悩みを拾う
    queries = [
        "need a tool",
        "how can I convert",
        "calculator for",
        "alternative to",
        "is there a tool",
    ]
    per_q = max(3, max_items // max(1, len(queries)))

    for q in queries:
        try:
            url = "https://hn.algolia.com/api/v1/search_by_date"
            params = {"query": q, "tags": "story", "hitsPerPage": per_q}
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            for hit in data.get("hits", []):
                title = hit.get("title") or ""
                story_url = hit.get("url") or ""
                hn_url = f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
                text = title
                out.append({
                    "platform": "hn",
                    "text": text,
                    "url": story_url or hn_url,
                    "thread_url": hn_url,
                    "created_at": hit.get("created_at") or "",
                })
        except Exception:
            continue

    # 重複除去
    seen = set()
    uniq = []
    for x in out:
        k = (x.get("platform"), x.get("thread_url"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
        if len(uniq) >= max_items:
            break
    return uniq


def collect_mastodon(max_items: int) -> List[Dict[str, Any]]:
    """
    Mastodon: read-only search (token/baseがある場合のみ)
    """
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "").strip()
    base = os.getenv("MASTODON_API_BASE", "").strip()
    if not tok or not base or Mastodon is None:
        return []

    m = Mastodon(access_token=tok, api_base_url=base)
    # 悩みっぽいキーワード（英語中心）
    queries = [
        "need a tool",
        "how do I convert",
        "is there a calculator",
        "alternative to",
        "anyone know a tool",
    ]
    out: List[Dict[str, Any]] = []
    per_q = max(2, max_items // max(1, len(queries)))

    for q in queries:
        try:
            res = m.search_v2(q, result_type="statuses", limit=per_q)
            for st in res.get("statuses", []):
                content = re.sub("<[^>]+>", "", st.get("content", "")).strip()
                url = st.get("url") or ""
                out.append({
                    "platform": "mastodon",
                    "text": content,
                    "url": url,
                    "thread_url": url,
                    "created_at": st.get("created_at") or "",
                })
        except Exception:
            continue

    # dedupe
    seen = set()
    uniq = []
    for x in out:
        k = x.get("thread_url")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(x)
        if len(uniq) >= max_items:
            break
    return uniq


def collect_bluesky(max_items: int) -> List[Dict[str, Any]]:
    """
    Bluesky: read-only searchPosts (handle/passがある場合のみ)
    """
    h = os.getenv("BSKY_HANDLE", "").strip()
    p = os.getenv("BSKY_PASSWORD", "").strip()
    if not h or not p or BskyClient is None:
        return []

    c = BskyClient()
    c.login(h, p)

    queries = [
        "need a tool",
        "how can I convert",
        "calculator",
        "alternative to",
        "template generator",
    ]
    out: List[Dict[str, Any]] = []
    per_q = max(2, max_items // max(1, len(queries)))

    for q in queries:
        try:
            # atproto wrapper: app.bsky.feed.searchPosts
            res = c.app.bsky.feed.search_posts({"q": q, "limit": per_q})
            for p0 in res.get("posts", []):
                rec = p0.get("record", {}) or {}
                text = (rec.get("text") or "").strip()
                uri = p0.get("uri") or ""
                # 公開URLは分解が面倒なので、URIをそのままログに残す（人間が辿れる）
                out.append({
                    "platform": "bluesky",
                    "text": text,
                    "url": uri,
                    "thread_url": uri,
                    "created_at": rec.get("createdAt") or "",
                })
        except Exception:
            continue

    # dedupe
    seen = set()
    uniq = []
    for x in out:
        k = x.get("thread_url")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(x)
        if len(uniq) >= max_items:
            break
    return uniq


def collect_x(max_items: int) -> List[Dict[str, Any]]:
    """
    X: read-only search (Bearer tokenがある場合のみ)
    注意: X APIはプラン/制限が厳しいので、入ってないなら黙ってスキップ。
    """
    bearer = os.getenv("X_BEARER_TOKEN", "").strip()
    if not bearer:
        return []

    # recent search endpoint (v2)
    # docsの都合で落ちる可能性があるため、失敗時は0件で戻す
    headers = {"Authorization": f"Bearer {bearer}"}
    q = '(need a tool OR "how can I convert" OR calculator OR "alternative to") lang:en -is:retweet'
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {"query": q, "max_results": min(10, max_items), "tweet.fields": "created_at"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code != 200:
            return []
        data = r.json()
        out = []
        for t in data.get("data", [])[:max_items]:
            tid = t.get("id")
            text = t.get("text", "")
            thread_url = f"https://twitter.com/i/web/status/{tid}"
            out.append({
                "platform": "x",
                "text": text,
                "url": thread_url,
                "thread_url": thread_url,
                "created_at": t.get("created_at") or "",
            })
        return out
    except Exception:
        return []


# -------- matching + drafting --------

def openai_match_and_draft(client: OpenAI, post: Dict[str, Any], tools: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    1) どのツールが合うか選ぶ
    2) 違和感ない返信文を生成（URLを最後に付ける）
    """
    post_text = clip(post.get("text", ""), 800)
    platform = post.get("platform", "")
    thread_url = post.get("thread_url", "")

    tools_compact = [
        {"title": t.get("title", ""), "url": t.get("url", ""), "tags": t.get("tags", [])}
        for t in tools
    ]

    prompt = f"""
You are assisting outreach, but DO NOT auto-post. Generate a draft reply only.

Given:
- Platform: {platform}
- Post text: {post_text}
- Thread URL: {thread_url}

Task:
1) Choose the single best matching tool from the provided tool list. If none fits, return {"ok": false}.
2) If ok=true, write a natural, friendly, non-pushy reply draft in English.
   - No aggressive marketing.
   - No promises.
   - Mention you made/found a small tool that might help.
   - Add the tool URL at the very end on its own line.
   - Keep it <= 320 characters (so it fits most platforms), avoid weird tone.

Return STRICT JSON only:
{{
  "ok": true/false,
  "tool_title": "...",
  "tool_url": "...",
  "reply": "..."
}}

Tool list JSON:
{json.dumps(tools_compact, ensure_ascii=False)}
""".strip()

    try:
        res = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = (res.choices[0].message.content or "").strip()
        m = re.search(r"\{[\s\S]*\}$", raw)
        if not m:
            return None
        obj = json.loads(m.group(0))
        if not obj.get("ok"):
            return None
        if not obj.get("tool_url") or not obj.get("reply"):
            return None
        return {
            "platform": platform,
            "thread_url": thread_url,
            "post_excerpt": clip(post_text, 200),
            "tool_title": obj.get("tool_title", ""),
            "tool_url": obj.get("tool_url", ""),
            "reply": obj.get("reply", ""),
        }
    except Exception:
        return None


def create_github_issue(title: str, body: str):
    pat = os.getenv("GH_PAT", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    if not pat or not repo:
        return

    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github+json"}
    payload = {"title": title, "body": body}
    try:
        requests.post(url, headers=headers, json=payload, timeout=20)
    except Exception:
        pass


# -------- main --------

def main():
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_key:
        # 何もしない（赤くならないように）
        return

    max_candidates = env_int("MAX_CANDIDATES", 20)
    max_drafts = env_int("MAX_DRAFTS", 10)

    tools = load_tools()
    client = OpenAI(api_key=openai_key)

    candidates: List[Dict[str, Any]] = []
    # ここが「必ず使う」場所：Bluesky/Mastodon/HN/X すべて試す（鍵がないのは黙ってスキップ）
    candidates += collect_hn(max_candidates)
    candidates += collect_bluesky(max_candidates)
    candidates += collect_mastodon(max_candidates)
    candidates += collect_x(max_candidates)

    # DBに収集ログ残す
    db = read_json(DB_PATH, {"runs": []})
    run_id = stable_id(now_utc_iso(), str(time.time()))
    run = {
        "run_id": run_id,
        "created_at": now_utc_iso(),
        "candidates_count": len(candidates),
        "drafts_count": 0,
        "drafts": [],
    }

    drafts: List[Dict[str, Any]] = []
    seen_threads = set()

    for post in candidates:
        if len(drafts) >= max_drafts:
            break
        turl = post.get("thread_url") or ""
        if not turl or turl in seen_threads:
            continue
        seen_threads.add(turl)

        d = openai_match_and_draft(client, post, tools)
        if not d:
            continue
        drafts.append(d)

    run["drafts_count"] = len(drafts)
    run["drafts"] = drafts[:]

    # 先頭に追加
    db_runs = db.get("runs", [])
    db_runs.insert(0, run)
    db["runs"] = db_runs[:50]
    write_json(DB_PATH, db)

    # Issues通知（返信は“下書き”として出すだけ）
    lines = []
    lines.append(f"- run_id: {run_id}")
    lines.append(f"- created_at: {run['created_at']}")
    lines.append(f"- candidates: {len(candidates)}")
    lines.append(f"- drafts: {len(drafts)}")
    lines.append("")
    lines.append("Drafts (copy-paste manually):")
    lines.append("")

    for i, d in enumerate(drafts, 1):
        lines.append(f"---")
        lines.append(f"{i}) platform: {d['platform']}")
        lines.append(f"thread: {d['thread_url']}")
        lines.append(f"match: {d['tool_title']}")
        lines.append(f"tool: {d['tool_url']}")
        lines.append("")
        lines.append(d["reply"])
        lines.append("")

    body = "\n".join(lines).strip()
    create_github_issue(title=f"[Goliath] Outreach drafts ({len(drafts)})", body=body)


if __name__ == "__main__":
    main()
