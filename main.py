import os
import re
import json
import time
import random
from datetime import datetime, timezone
from pathlib import Path

from openai import OpenAI
from atproto import Client as BskyClient
from mastodon import Mastodon


SITE_BASE_URL = os.getenv("SITE_BASE_URL", "https://mikanntool.com").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

ROOT_DIR = Path(__file__).resolve().parent
GOLIATH_DIR = ROOT_DIR / "goliath"
PAGES_DIR = GOLIATH_DIR / "pages"
INDEX_HTML = GOLIATH_DIR / "index.html"
DB_JSON = GOLIATH_DIR / "db.json"


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or f"topic-{int(time.time())}"


def get_trend_keyword() -> str:
    # 最小構成（後でReddit/APIに差し替え可能）
    problems = [
        "BMI calculator",
        "Compound interest calculator",
        "Daily calorie needs",
        "Pomodoro time management",
        "Loan repayment simulator",
        "Sleep cycle calculator",
    ]
    return random.choice(problems)


def extract_clean_html(text: str) -> str:
    m = re.search(r"(<!DOCTYPE html>.*?</html>)", text, re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()

    safe = text.strip()
    return (
        "<!DOCTYPE html>\n<html><head><meta charset=\"utf-8\"></head>"
        "<body><pre style=\"white-space:pre-wrap;\">"
        + safe.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        + "</pre></body></html>"
    )


def generate_html(topic: str, page_url: str) -> str:
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    prompt = f"""
You are generating a SINGLE-FILE website.

Return ONLY raw HTML that starts with <!DOCTYPE html> and ends with </html>.
No markdown. No explanations. No code fences.

[Topic]
{topic}

[Hard Requirements]
1) Design: modern SaaS, responsive, clean typography. Use Tailwind via CDN.
2) Must include: Dark/Light mode toggle.
3) Must include: Language switcher (JP/EN/DE/FR). Default JP.
4) Must include: An interactive JavaScript tool (calculator/simulator) that directly solves the topic.
5) Must include: SEO article text in Japanese (>= 2500 Japanese characters) with headings, checklist, FAQ(>=5), and “next steps”.
6) Must include: A small “Sources / Further reading” section with 8–15 generic reference categories (no need to browse).
7) Must include Ad placeholders as HTML comments exactly:
   <!-- ADS_SLOT_TOP -->
   <!-- ADS_SLOT_MID -->
   <!-- ADS_SLOT_BOTTOM -->
8) Must include canonical URL = {page_url} in <link rel="canonical" ...>

[Output]
ONLY HTML.
""".strip()

    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
    )
    return extract_clean_html(resp.output_text)


def load_db():
    if DB_JSON.exists():
        return json.loads(DB_JSON.read_text(encoding="utf-8"))
    return {"items": []}


def save_db(db):
    GOLIATH_DIR.mkdir(parents=True, exist_ok=True)
    DB_JSON.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")


def write_page(slug: str, html: str):
    out_dir = PAGES_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(html, encoding="utf-8")


def build_index(db):
    items = db["items"][:80]
    li = "\n".join(
        [
            f'<li><a href="{i["path"]}">{i["title"]}</a> <small>({i["created_at"]})</small></li>'
            for i in items
        ]
    )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Goliath Tools</title>
  <link rel="canonical" href="{SITE_BASE_URL}/goliath/" />
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen bg-gray-50 text-gray-900">
  <main class="max-w-3xl mx-auto p-6">
    <div class="flex items-baseline justify-between gap-4">
      <h1 class="text-2xl font-bold">Goliath Tools</h1>
      <p class="text-xs text-gray-500">更新: {utc_now_iso()}</p>
    </div>

    <p class="mt-2 text-sm text-gray-600">
      自動生成されたツール一覧（このフォルダ以外は一切触らない安全設計）
    </p>

    <!-- ADS_SLOT_TOP -->

    <ul class="mt-6 list-disc pl-6 space-y-2">
      {li}
    </ul>

    <!-- ADS_SLOT_BOTTOM -->
  </main>
</body>
</html>
"""
    GOLIATH_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_HTML.write_text(html, encoding="utf-8")


def post_sns(message: str):
    # Bluesky
    try:
        b = BskyClient()
        b.login(os.environ["BSKY_HANDLE"], os.environ["BSKY_PASSWORD"])
        b.send_post(text=message)
    except Exception:
        pass

    # Mastodon
    try:
        mstdn = Mastodon(
            access_token=os.environ["MASTODON_ACCESS_TOKEN"],
            api_base_url=os.environ["MASTODON_API_BASE"],
        )
        mstdn.status_post(message)
    except Exception:
        pass


def main():
    topic = get_trend_keyword()
    created_at = utc_now_iso()

    slug = f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{slugify(topic)}"
    path = f"/goliath/pages/{slug}/"
    page_url = f"{SITE_BASE_URL}{path}"

    html = generate_html(topic, page_url)
    write_page(slug, html)

    db = load_db()
    db["items"].insert(0, {"title": topic, "created_at": created_at, "path": path})
    save_db(db)
    build_index(db)

    post_sns(f"【新着】{topic} を解決するツールを公開しました\n{page_url}")


if __name__ == "__main__":
    main()
