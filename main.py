--- a/main.py
+++ b/main.py
@@
 import requests
 from openai import OpenAI
 
@@
 Mastodon = None
 
+try:
+    import tweepy
+except Exception:
+    tweepy = None
+
 ROOT = "goliath"
 PAGES_DIR = f"{ROOT}/pages"
 DB_PATH = f"{ROOT}/db.json"
 INDEX_PATH = f"{ROOT}/index.html"
 SEED_SITES_PATH = f"{ROOT}/sites.seed.json"
+ASSETS_DIR = f"{ROOT}/assets"
 
@@
 def ensure_dirs():
     os.makedirs(PAGES_DIR, exist_ok=True)
+    os.makedirs(ASSETS_DIR, exist_ok=True)
 
@@
 def collector_stub() -> List[Dict[str, str]]:
@@
     return out
 
+def collector_hn(days: int = 365, max_items: int = 60) -> List[Dict[str, str]]:
+    """
+    HN: Algolia Search API (公開)
+    """
+    try:
+        now = int(time.time())
+        since = now - days * 86400
+        # recent stories, paginate a bit
+        items: List[Dict[str, str]] = []
+        page = 0
+        while len(items) < max_items and page < 3:
+            r = requests.get(
+                "https://hn.algolia.com/api/v1/search_by_date",
+                params={"tags": "story", "hitsPerPage": 20, "page": page},
+                timeout=20,
+            )
+            data = r.json()
+            for h in data.get("hits", []):
+                created = h.get("created_at_i", 0)
+                if created and created < since:
+                    continue
+                title = (h.get("title") or "").strip()
+                url = (h.get("url") or "").strip()
+                if not url:
+                    # fallback to item link
+                    objid = h.get("objectID")
+                    if objid:
+                        url = f"https://news.ycombinator.com/item?id={objid}"
+                if title and url:
+                    items.append({"text": title, "url": url})
+            page += 1
+        return items[:max_items]
+    except Exception:
+        return []
+
+def collector_reddit_public(subs: List[str], max_items: int = 60) -> List[Dict[str, str]]:
+    """
+    Reddit: 認証なしで取れる範囲（公開JSON）。
+    レート/ブロックされる可能性があるので失敗時は空。
+    """
+    out: List[Dict[str, str]] = []
+    headers = {"User-Agent": "goliath-auto-tool/1.0 (by Mikann20041029)"}
+    try:
+        for sub in subs:
+            if len(out) >= max_items:
+                break
+            r = requests.get(
+                f"https://www.reddit.com/r/{sub}/new.json",
+                params={"limit": 25},
+                headers=headers,
+                timeout=20,
+            )
+            data = r.json()
+            children = (((data or {}).get("data") or {}).get("children") or [])
+            for c in children:
+                d = (c or {}).get("data") or {}
+                title = (d.get("title") or "").strip()
+                permalink = (d.get("permalink") or "").strip()
+                if title and permalink:
+                    out.append({"text": title, "url": "https://www.reddit.com" + permalink})
+        return out[:max_items]
+    except Exception:
+        return []
+
@@
 def build_prompt(theme: str, cluster: Dict[str, Any], base_url: str) -> str:
@@
   [Design]
@@
   [Related Sites]
@@
   [SEO]
@@
+  [Hero Image Placeholder]
+  - In the hero section, include an <img> with src="{{HERO_IMAGE_URL}}" (exact token) so it can be replaced after generation.
+
   Return ONLY the final HTML.
 """.strip()
 
@@
 def validate_html(html: str) -> Tuple[bool, str]:
@@
     return True, "ok"
 
@@
 def post_mastodon(text: str):
@@
     except Exception:
         pass
 
+def post_x(text: str):
+    api_key = os.getenv("X_API_KEY", "")
+    api_secret = os.getenv("X_API_SECRET", "")
+    access_token = os.getenv("X_ACCESS_TOKEN", "")
+    access_secret = os.getenv("X_ACCESS_SECRET", "")
+    if not api_key or not api_secret or not access_token or not access_secret or tweepy is None:
+        return
+    try:
+        auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_secret)
+        api = tweepy.API(auth)
+        api.update_status(status=text[:270])
+    except Exception:
+        pass
+
 def inject_related_json(html: str, related: List[Dict[str, str]]) -> str:
@@
     return new
 
+def fetch_unsplash_image(query: str) -> Optional[str]:
+    """
+    UNSPLASH_ACCESS_KEY があればAPIで1枚取り、ローカルに保存して相対パスを返す。
+    無ければ None。
+    """
+    key = os.getenv("UNSPLASH_ACCESS_KEY", "")
+    if not key:
+        return None
+    try:
+        r = requests.get(
+            "https://api.unsplash.com/photos/random",
+            params={"query": query, "orientation": "landscape", "content_filter": "high"},
+            headers={"Authorization": f"Client-ID {key}"},
+            timeout=20,
+        )
+        data = r.json()
+        img = (((data or {}).get("urls") or {}).get("regular") or "").strip()
+        if not img:
+            return None
+        # download
+        img_r = requests.get(img, timeout=30)
+        if img_r.status_code != 200:
+            return None
+        fname = f"{stable_id(query, str(time.time()))}.jpg"
+        path = f"{ASSETS_DIR}/{fname}"
+        with open(path, "wb") as f:
+            f.write(img_r.content)
+        # return relative path from page: go up to /goliath/
+        return f"../../assets/{fname}"
+    except Exception:
+        return None
+
+def replace_hero_image_token(html: str, url: str) -> str:
+    return html.replace("{{HERO_IMAGE_URL}}", url)
+
 def main():
     ensure_dirs()
 
     client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))
 
     # 1) Collector -> Cluster
-    items = collector_stub()
+    # 実データ優先（失敗したらスタブにフォールバック）
+    items: List[Dict[str, str]] = []
+    items += collector_hn(days=365, max_items=60)
+    items += collector_reddit_public(["SideProject", "Entrepreneur", "webdev", "selfhosted", "productivity"], max_items=60)
+    if not items:
+        items = collector_stub()
     cluster = cluster_20(items)
     theme = cluster["theme"]
@@
     prompt = build_prompt(theme, cluster, canonical)
     html = openai_generate_html(client, prompt)
@@
     if not ok:
@@
         return
 
     # 4) Related sites list generation
     all_entries = read_json(DB_PATH, [])
     seed_sites = load_seed_sites()
     related = pick_related(tags, all_entries, seed_sites, k=8)
 
     # Ensure page has a window.__RELATED__ assignment filled
     html = inject_related_json(html, related)
+
+    # Unsplash hero image (optional)
+    hero = fetch_unsplash_image("modern SaaS abstract background")
+    if hero:
+        html = replace_hero_image_token(html, hero)
+    else:
+        # fallback to a safe public source (no key)
+        html = replace_hero_image_token(html, "https://source.unsplash.com/featured/1600x900/?abstract,technology")
 
     # 5) Save page
     page_path = f"{page_dir}/index.html"
     write_text(page_path, html)
@@
     # 8) SNS (optional)
     post_text = f"New tool: {theme}\n{public_url}"
     post_bluesky(post_text)
     post_mastodon(post_text)
+    post_x(post_text)
 
 if __name__ == "__main__":
     main()
-
-if __name__ == "__main__":
-    main()

