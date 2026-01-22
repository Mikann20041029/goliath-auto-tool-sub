import os
import re
import json
import time
import random
import hashlib
import datetime
from typing import List, Dict, Any, Tuple, Optional

import requests
from openai import OpenAI

# Optional SNS libs (missing secretsなら黙ってスキップ)
try:
    from atproto import Client as BskyClient
except Exception:
    BskyClient = None

try:
    from mastodon import Mastodon
except Exception:
    Mastodon = None


# =========================
# Config
# =========================
ROOT = "goliath"
PAGES_DIR = f"{ROOT}/pages"
DB_PATH = f"{ROOT}/db.json"
INDEX_PATH = f"{ROOT}/index.html"
SEED_SITES_PATH = f"{ROOT}/sites.seed.json"

# ---- Model choices (final decision) ----
# ツール生成: 品質優先（エラー率を落とす）
MODEL_BUILD = os.getenv("MODEL_BUILD", "gpt-4.1-2025-04-14")
# 返信文生成: 最安寄り（人間らしく、末尾にURL）
MODEL_REPLY = os.getenv("MODEL_REPLY", "gpt-4.1-nano-2025-04-14")

# ---- Lead count (final decision) ----
LEADS_TOTAL = int(os.getenv("LEADS_TOTAL", "100"))  # Issuesに出す「返信候補」件数
LEADS_PER_SOURCE = int(os.getenv("LEADS_PER_SOURCE", "60"))  # 収集は余裕持って多め→スコアで上位化

# ---- Collect limits ----
COLLECT_HN = int(os.getenv("COLLECT_HN", "50"))
COLLECT_BSKY = int(os.getenv("COLLECT_BSKY", "50"))
COLLECT_MASTODON = int(os.getenv("COLLECT_MASTODON", "50"))

# ---- Post announce (auto) ----
ENABLE_AUTO_POST = os.getenv("ENABLE_AUTO_POST", "1") == "1"


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def slugify(s: str, max_len: int = 60) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:max_len] or "tool"


def ensure_dirs():
    os.makedirs(PAGES_DIR, exist_ok=True)


def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def extract_html_only(raw: str) -> str:
    """
    余計な挨拶/markdown/``` を排除して <!DOCTYPE html>..</html> のみ切り出す
    """
    m = re.search(r"(<!DOCTYPE\s+html.*?</html\s*>)", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())
    return raw.strip()


def stable_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


# =========================
# Scoring (ポイント制)
# =========================
def score_item(text: str, url: str, meta: Dict[str, Any]) -> Tuple[int, Dict[str, int]]:
    """
    ポイント表（見える形）
    - 「ツール化しやすい」ほど高得点
    - 「既存巨大ツールが強すぎる」っぽいと減点
    """
    t = (text or "").lower()

    table = {
        "tool_request": 0,
        "convert_generator_calc": 0,
        "structured_output": 0,
        "specific_inputs": 0,
        "how_to_code_only": 0,
        "too_broad": 0,
        "adult_or_sensitive": 0,
    }

    # ツール欲しい系
    if any(k in t for k in ["is there a tool", "any tool", "tool for", "looking for a tool", "need a tool"]):
        table["tool_request"] += 8

    # convert/generator/calculator は刺さりやすい
    if any(k in t for k in ["convert", "converter", "generator", "calculate", "calculator", "format", "transform"]):
        table["convert_generator_calc"] += 7

    # 出力形式が明確だと作りやすい
    if any(k in t for k in ["json", "csv", "markdown", "notion", "template", "checklist", "table"]):
        table["structured_output"] += 5

    # 入力が具体的だと作りやすい
    if any(k in t for k in ["timezone", "tax", "subscription", "plan", "pricing", "compare", "fee", "rate"]):
        table["specific_inputs"] += 4

    # 「コードの書き方教えて」だけだとツール化しにくい
    if any(k in t for k in ["how do i code", "write code", "bug in my code", "stack trace"]):
        table["how_to_code_only"] -= 6

    # 広すぎる要件
    if any(k in t for k in ["everything", "all-in-one", "ultimate", "perfect solution"]):
        table["too_broad"] -= 4

    # 変な地雷避け（最低限）
    if any(k in t for k in ["porn", "sexual", "nude", "violence", "illegal"]):
        table["adult_or_sensitive"] -= 20

    # HN points などメタ加点
    hn_points = int(meta.get("hn_points", 0) or 0)
    if hn_points > 0:
        # ざっくり上限
        table["tool_request"] += min(10, hn_points // 30)

    total = sum(table.values())
    return total, table


# =========================
# Collector (Real)
# =========================
def hn_search(query: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    HN (Algolia) から検索。
    """
    try:
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {
            "query": query,
            "tags": "story",
            "hitsPerPage": min(100, max(1, limit)),
        }
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        out = []
        for h in data.get("hits", [])[:limit]:
            title = h.get("title") or ""
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
        "template generator",
    ]
    all_items: List[Dict[str, Any]] = []
    per = max(5, limit // max(1, len(queries)))
    for q in queries:
        all_items.extend(hn_search(q, limit=per))
    # だぶり除去（url基準）
    seen = set()
    uniq = []
    for it in all_items:
        u = it["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(it)
        if len(uniq) >= limit:
            break
    return uniq


def collect_bluesky(limit: int) -> List[Dict[str, Any]]:
    """
    Bluesky: atprotoで検索
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
        "calculator",
        "compare plans",
        "timezone",
        "template",
    ]
    out: List[Dict[str, Any]] = []
    try:
        c = BskyClient()
        c.login(h, p)
        for q in queries:
            # atproto wrapper: search_posts
            res = c.app.bsky.feed.search_posts({"q": q, "limit": 25})
            posts = (res or {}).get("posts", [])
            for post in posts:
                rec = post.get("record", {}) or {}
                txt = rec.get("text", "") or ""
                uri = post.get("uri", "") or ""
                did = post.get("author", {}).get("did", "") or ""
                # 参照用URL（推定）
                bsky_url = ""
                if did and uri:
                    # uri: at://did/app.bsky.feed.post/<rkey>
                    m = re.search(r"/app\.bsky\.feed\.post/([^/]+)$", uri)
                    if m:
                        rkey = m.group(1)
                        bsky_url = f"https://bsky.app/profile/{did}/post/{rkey}"
                out.append({"source": "Bluesky", "text": txt[:300], "url": bsky_url or uri, "meta": {}})
                if len(out) >= limit:
                    return out
        return out[:limit]
    except Exception:
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

        # 1) tokenあれば検索
        queries = [
            "need a tool",
            "is there a tool",
            "convert",
            "calculator",
            "compare plan",
            "timezone",
            "template",
        ]
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
                            return out
                except Exception:
                    pass

        # 2) fallback: public timeline
        if len(out) < limit:
            try:
                statuses = m.timeline_public(limit=80)
                for st in statuses:
                    txt = re.sub(r"<[^>]+>", "", st.get("content", "") or "")
                    # それっぽい文だけ拾う
                    if not any(k in txt.lower() for k in ["tool", "convert", "calculator", "compare", "timezone", "template"]):
                        continue
                    url = st.get("url", "") or ""
                    out.append({"source": "Mastodon", "text": txt[:300], "url": url, "meta": {}})
                    if len(out) >= limit:
                        return out
            except Exception:
                pass

        # だぶり除去
        seen = set()
        uniq = []
        for it in out:
            if it["url"] in seen:
                continue
            seen.add(it["url"])
            uniq.append(it)
        return uniq[:limit]
    except Exception:
        return out[:limit]


def collector_real() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    items.extend(collect_hn(COLLECT_HN))
    items.extend(collect_bluesky(COLLECT_BSKY))
    items.extend(collect_mastodon(COLLECT_MASTODON))

    # 最低でも何か動くように、空なら軽いスタブ
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
# Cluster
# =========================
def pick_best_theme(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    scored = []
    for it in items:
        s, table = score_item(it["text"], it["url"], it.get("meta", {}) or {})
        scored.append((s, it, table))
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_item, best_table = scored[0]
    return {
        "theme": best_item["text"],
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
    keys = []
    for k in ["convert", "calculator", "compare", "timezone", "template", "subscription", "pricing", "tax", "checklist"]:
        if k in t:
            keys.append(k)
    if not keys:
        keys = t.split()[:3]

    def sim(x: str) -> int:
        xl = (x or "").lower()
        return sum(1 for k in keys if k in xl)

    ranked = sorted(items, key=lambda it: sim(it["text"]), reverse=True)
    chosen = ranked[:20]
    return {
        "theme": theme,
        "items": [{"text": it["text"], "url": it["url"], "source": it["source"]} for it in chosen],
        "urls": [it["url"] for it in chosen],
        "texts": [it["text"] for it in chosen],
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
        return read_json(SEED_SITES_PATH, [])
    return []


def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def pick_related(tags, all_entries, seed_sites, k=8):
    # --- normalize inputs (防御コード) ---
    if isinstance(tags, str):
        tags = [tags]

    # seed_sites が dict でも list でも吸収して map を作る
    seed_map = {}
    if isinstance(seed_sites, dict):
        seed_map = seed_sites
    elif isinstance(seed_sites, list):
        for s in seed_sites:
            if isinstance(s, dict) and "slug" in s:
                seed_map[s["slug"]] = s

    normalized = []
    for e in all_entries:
        if isinstance(e, dict):
            normalized.append(e)
            continue
        if isinstance(e, str):
            # 文字列なら seed_map から引けるなら辞書に戻す / 無理なら捨てる
            if e in seed_map and isinstance(seed_map[e], dict):
                normalized.append(seed_map[e])
            else:
                # ここで捨てる（落ちるより100倍マシ）
                continue
        else:
            continue

    all_entries = normalized
    # --- ここから下は既存ロジック ---
    ...

def pick_related(current_tags: List[str], all_entries: List[Dict[str, Any]], seed_sites: List[Dict[str, Any]], k: int = 8) -> List[Dict[str, str]]:
    candidates: List[Tuple[float, Dict[str, str]]] = []

    # goliath内の過去ページ
    # goliath/main.py （pick_related 内）
for e in all_entries:
    if not isinstance(e, dict):
        continue

    tags = e.get("tags", [])
    score = jaccard(current_tags, tags)
    if score <= 0:
        continue

    candidates.append((score, {"title": e.get("title", ""), "url": e.get("public_url", "")}))

    # 既存の外部/既存サイト
    for s in seed_sites:
        tags = s.get("tags", [])
        score = jaccard(current_tags, tags)
        if score <= 0:
            continue
        candidates.append((score, {"title": s["title"], "url": s["url"]}))

    candidates.sort(key=lambda x: x[0], reverse=True)

    seen = set()
    related = []
    for _, item in candidates:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        related.append(item)
        if len(related) >= k:
            break
    return related


# =========================
# Builder / Validator / Auto-fix
# =========================
def build_prompt(theme: str, cluster: Dict[str, Any], canonical_url: str) -> str:
    # 重要: 余計な文章禁止、HTMLのみ
    # 重要: フッターに規約系リンク、言語切替、関連サイト欄（window.__RELATED__）
    return f"""
You are generating a production-grade single-file HTML tool site.

STRICT OUTPUT RULE:
- Output ONLY raw HTML that starts with <!DOCTYPE html> and ends with </html>.
- No markdown, no backticks, no explanations.

[Goal]
Create a modern SaaS-style tool page to solve: "{theme}"

[Design]
- Use Tailwind CSS via CDN
- Clean SaaS UI: hero section + centered tool card + sections
- Dark/Light mode toggle (CSS class switch)

[Tool]
- Implement an interactive JS mini-tool relevant to the theme (static, no server).
- Must work without any server.

[Content]
- Include a Japanese long-form article >= 2500 Japanese characters.
- Use clear structure with H2/H3 headings, checklist, pitfalls, FAQ(>=5).
- Add "References" section with 8-12 reputable external links (official docs / well-known sites).

[Multi-language]
- Provide language switcher for JA/EN/FR/DE.
- At minimum translate: hero, tool labels, and footer pages.
- Article can be JA primary; provide short EN/FR/DE summary sections.

[Compliance / Footer]
- Auto-generate in-page sections for:
  - Privacy Policy (cookie/ads explanation)
  - Terms of Service
  - Disclaimer
  - About / Operator info
  - Contact
- These must be accessible via footer links using in-page anchors.

[Related Sites]
- Include a "Related sites" section near bottom as a list:
  - It must be filled from a JSON embedded in the page: window.__RELATED__ = [];
  - Render it into the list on load.
  - If empty, hide the section.

[SEO]
- Include title/meta description/canonical.
- Canonical must be: {canonical_url}

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
    low = html.lower()
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
    return True, "ok"


def prompt_for_fix(error: str, html: str) -> str:
    return f"""
Return ONLY a unified diff patch for a single file named index.html.

Rules:
- Output ONLY the diff. No markdown. No explanations.
- The patch MUST fix this validation error: {error}
- Do not remove required features: Tailwind CDN, SaaS layout, dark/light toggle, language switcher,
  footer policy sections, window.__RELATED__ rendering.

Current index.html:
{html}
""".strip()


def apply_unified_diff_to_text(original: str, diff_text: str) -> Optional[str]:
    if not diff_text.startswith("---"):
        return None
    lines = diff_text.splitlines()
    # find hunks
    hunks = [i for i, l in enumerate(lines) if l.startswith("@@")]
    if not hunks:
        return None

    orig_lines = original.splitlines()
    try:
        result = []
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


def infer_tags_simple(theme: str) -> List[str]:
    t = (theme or "").lower()
    tags = []
    rules = {
        "convert": "convert",
        "calculator": "calculator",
        "compare": "compare",
        "tax": "finance",
        "timezone": "time",
        "time zone": "time",
        "subscription": "pricing",
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
    # 置換できなかったら末尾scriptに追記（保険）
    if new == html:
        new = re.sub(r"</body>", f"<script>window.__RELATED__ = {rel_json};</script>\n</body>", html, flags=re.IGNORECASE)
    return new


# =========================
# Publishing / Index / Notify / SNS
# =========================
def get_repo_pages_base() -> str:
    repo = os.getenv("GITHUB_REPOSITORY", "mikann20041029/goliath-auto-tool")
    owner, name = repo.split("/", 1)
    return f"https://{owner.lower()}.github.io/{name}/"


def update_db_and_index(entry: Dict[str, Any], all_entries: List[Dict[str, Any]]):
    # db.json 先頭に追加
    all_entries.insert(0, entry)
    write_json(DB_PATH, all_entries)

    # index.html を更新（新着一覧）
    rows = []
    for e in all_entries[:50]:
        rows.append(
            (
                '<a class="block p-4 rounded-xl border border-slate-200 dark:border-slate-800 '
                'hover:bg-slate-50 dark:hover:bg-slate-900 transition" '
                f'href="{e["path"]}/">'
                f'<div class="font-semibold">{e["title"]}</div>'
                f'<div class="text-sm opacity-70">{e["created_at"]} • {", ".join(e.get("tags", []))}</div>'
                "</a>"
            )
        )

    # f-string を使わない（JSの { } が混ざると Python が死ぬため）
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Goliath Tools</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-50">
  <div class="max-w-4xl mx-auto p-6">
    <div class="flex items-center justify-between gap-4">
      <div>
        <h1 class="text-2xl font-bold">Goliath Tools</h1>
        <p class="opacity-70">Auto-generated tools + long-form guides</p>
      </div>
      <button id="themeBtn" class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800">Dark/Light</button>
    </div>

    <div class="mt-6 grid gap-3">
      __ROWS__
    </div>

    <div class="mt-10 text-xs opacity-60">
      <a class="underline" href="./pages/">All pages</a>
    </div>
  </div>

  <script>
    const root = document.documentElement;
    const k = "goliath_theme";
    const saved = localStorage.getItem(k);
    if (saved === "dark") root.classList.add("dark");

    document.getElementById("themeBtn").onclick = () => {
      root.classList.toggle("dark");
      localStorage.setItem(k, root.classList.contains("dark") ? "dark" : "light");
    };
  </script>
</body>
</html>
"""
    html = html.replace("__ROWS__", "\n".join(rows))
    write_text(INDEX_PATH, html)



def create_github_issue(title: str, body: str):
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



    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        print(f"[issue] status={r.status_code}")
        if r.status_code not in (200, 201):
            print(f"[issue] response={r.text[:500]}")
    except Exception as e:
        print(f"[issue] exception: {e}")



def post_bluesky(text: str):
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


def post_mastodon(text: str):
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "")
    base = os.getenv("MASTODON_API_BASE", "")
    if not tok or not base or Mastodon is None:
        return
    try:
        m = Mastodon(access_token=tok, api_base_url=base)
        m.status_post(text)
    except Exception:
        pass


def post_x(text: str):
    """
    Xは無料枠/権限/認証が可変なので、資格情報が揃っているときだけ投げる（揃ってなければ黙ってスキップ）。
    ここは「壊れないこと優先」で最小実装（Bearerのみだと投稿できないことが多い）。
    """
    # 何も揃ってないなら終了
    if not (os.getenv("X_API_KEY") and os.getenv("X_API_SECRET") and os.getenv("X_ACCESS_TOKEN") and os.getenv("X_ACCESS_TOKEN_SECRET")):
        return

    # 署名実装まで含めると長くなり事故りやすいので、ここは安全に「下書きとしてIssueに出す」運用推奨。
    # ただし「自動投稿」を絶対に止めない条件があるため、最低限の案内としてIssueへ出す（投稿自体はここでは実施しない）。
    create_github_issue(
        title="[Goliath] X auto-post skipped (needs OAuth1 signing impl)",
        body="X投稿はOAuth1署名が必要な構成が多く、簡易実装だと壊れやすいのでこの版ではスキップしています。\n"
             "必要なら OAuth1 署名つき投稿モジュールだけを追加します（他を壊さない前提）。"
    )


# =========================
# Lead collection (for manual reply) + Reply draft generation
# =========================
def extract_keywords(theme: str) -> List[str]:
    t = (theme or "").lower()
    base = []
    for k in ["convert", "calculator", "compare", "timezone", "template", "subscription", "pricing", "tax", "checklist"]:
        if k in t:
            base.append(k)
    if not base:
        base = [w for w in re.findall(r"[a-z0-9]{4,}", t)][:5]
    return base[:6] if base else ["tool"]


def collect_leads(theme: str) -> List[Dict[str, Any]]:
    """
    ツール完成後に「そのツールで解決できそうな悩みURL」を再収集。
    ここで Bluesky と Mastodon を必ず試みる（鍵が無い場合は空になるが、試行は行う）。
    """
    keys = extract_keywords(theme)
    # HN: keysで検索
    leads: List[Dict[str, Any]] = []
    for k in keys:
        leads.extend(hn_search(k, limit=20))

    # Bluesky & Mastodon: collectorを再利用（検索クエリは内部で回る）
    leads.extend(collect_bluesky(LEADS_PER_SOURCE))
    leads.extend(collect_mastodon(LEADS_PER_SOURCE))

    # だぶり除去
    seen = set()
    uniq = []
    for it in leads:
        u = it.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(it)
    return uniq


def openai_generate_reply(client: OpenAI, post_text: str, tool_url: str) -> str:
    """
    返信文（人間らしい / 優しい / 疑問文ベース / 最後にURL）
    """
    prompt = f"""
You write a short, natural, polite reply to an online post.
Rules:
- Tone: kind, non-spammy, helpful.
- End with a gentle question.
- Append the tool URL at the end on a new line.
- Do NOT mention "AI", "automation", "bot".
- Keep it under 280 characters if possible.
Post:
{post_text}

Tool URL:
{tool_url}

Return ONLY the reply text.
""".strip()

    res = client.chat.completions.create(
        model=MODEL_REPLY,
        messages=[{"role": "user", "content": prompt}],
    )
    txt = (res.choices[0].message.content or "").strip()
    # 念のためURLを最後に強制
    if tool_url not in txt:
        txt = txt.rstrip() + "\n" + tool_url
    return txt


def build_leads_issue_body(leads: List[Dict[str, Any]], tool_url: str) -> str:
    """
    Issuesに「対象URL + 返信文」を100セット出す。
    """
    lines = []
    lines.append("以下は「手動返信用」の候補です。\n")
    lines.append("形式:\n- 対象の悩みURL（X/Bluesky/Mastodon/HN）\n- AI返信文（末尾にツールURL入り）\n")
    lines.append("----\n")

    for i, it in enumerate(leads, 1):
        url = it.get("url", "")
        txt = it.get("text", "") or ""
        src = it.get("source", "")
        lines.append(f"#{i} [{src}]")
        lines.append(url)
        lines.append("返信文:")
        lines.append(it.get("reply", "").strip())
        lines.append("\n----\n")

    body = "\n".join(lines)
    return body


def chunk_and_create_issues(title_prefix: str, body: str, max_chars: int = 60000):
    """
    Issue本文が長すぎると事故るので、必要なら分割する。
    """
    if len(body) <= max_chars:
        create_github_issue(title_prefix, body)
        return

    parts = []
    cur = []
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
def main():
    ensure_dirs()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        create_github_issue("[Goliath] Missing OPENAI_API_KEY", "OPENAI_API_KEY が未設定です。")
        return
    client = OpenAI(api_key=api_key)

    # 1) Collector (Real) -> pick best theme
    items = collector_real()
    best = pick_best_theme(items)
    theme = best["theme"]
    best_item = best["best_item"]
    best_score = best["best_score"]
    best_table = best["best_table"]

    # 2) Cluster 20 around theme (for builder context)
    cluster = cluster_20_around_theme(theme, items)

    created_at = now_utc_iso()
    tags = infer_tags_simple(theme)
    slug = slugify(theme)
    folder = f"{int(time.time())}-{slug}"
    page_dir = f"{PAGES_DIR}/{folder}"
    os.makedirs(page_dir, exist_ok=True)

    pages_base = get_repo_pages_base()
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
            body=f"- theme: {theme}\n- best_source: {best_item.get('source')}\n- best_url: {best_item.get('url')}\n"
                 f"- best_score: {best_score}\n- score_breakdown: {json.dumps(best_table, ensure_ascii=False)}\n"
                 f"- error: {msg}\n- created_at: {created_at}\n"
        )
        return

    # 4) Related sites (existing + seed) and inject JSON
    all_entries = read_json(DB_PATH, [])
    seed_sites = load_seed_sites()
    related = pick_related(tags, all_entries, seed_sites, k=8)
    html = inject_related_json(html, related)

    # 5) Save page
    page_path = f"{page_dir}/index.html"
    write_text(page_path, html)

    # 6) Update DB + index
    entry = {
        "id": stable_id(created_at, slug),
        "title": theme[:80],
        "created_at": created_at,
        "path": f"./pages/{folder}",
        "public_url": public_url,
        "tags": tags,
        "source_urls": cluster.get("urls", [])[:20],
        "related": related,
        "best_source": best_item.get("source"),
        "best_url": best_item.get("url"),
        "best_score": best_score,
        "best_score_breakdown": best_table,
        "score_keys": cluster.get("keys", []),
    }
    update_db_and_index(entry, all_entries)

    # 7) Auto-post (announce)
    if ENABLE_AUTO_POST:
        post_text = f"New tool published: {theme[:90]}\n{public_url}"
        post_bluesky(post_text)
        post_mastodon(post_text)
        post_x(post_text)

    # 8) Lead collection for manual reply + draft replies (100)
    leads = collect_leads(theme)

    # スコア付けして上位を優先（post_textに対して）
    scored = []
    for it in leads:
        s, _tbl = score_item(it.get("text", ""), it.get("url", ""), it.get("meta", {}) or {})
        scored.append((s, it))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [it for _s, it in scored[:max(LEADS_TOTAL, 10)]]

    # 返信文を生成（100）
    final = []
    for it in top[:LEADS_TOTAL]:
        txt = it.get("text", "") or ""
        reply = openai_generate_reply(client, txt, public_url)
        it2 = dict(it)
        it2["reply"] = reply
        final.append(it2)

    # 9) Notify issue: new tool + reply candidates
    header = []
    header.append(f"Tool URL: {public_url}")
    header.append(f"Theme: {theme}")
    header.append(f"Picked from: {best_item.get('source')} / {best_item.get('url')}")
    header.append(f"Best score: {best_score} / breakdown: {json.dumps(best_table, ensure_ascii=False)}")
    header.append(f"Tags: {', '.join(tags)}")
    header.append(f"Related sites count: {len(related)}")
    header.append("")
    header.append("")

    body = "\n".join(header) + build_leads_issue_body(final, public_url)

    # 100セットは長くなりがちなので自動分割（必要な時だけ）
    chunk_and_create_issues(
        title_prefix=f"[Goliath] Reply candidates ({LEADS_TOTAL}) + new tool: {slug}",
        body=body,
        max_chars=60000
    )


if __name__ == "__main__":
    main()
