import os
import json
import random
import re
from openai import OpenAI

# 認証設定
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_perfect_site():
    # トピックの選定
    topics = ["BMI健康管理ツール", "複利資産運用シミュレーター", "毎日の消費カロリー計算機"]
    topic = random.choice(topics)
    
    print(f"💎 サイト生成開始: {topic}")

    prompt = f"""
    Create a complete, single-file professional HTML website for '{topic}'.
    - Requirements: Use Tailwind CSS, modern UI, 2000+ characters Japanese SEO article.
    - Features: Fully working JavaScript tool, multi-language buttons (JP, EN, FR, DE).
    - Format: Return ONLY raw HTML code starting with <!DOCTYPE html>. 
    - NO markdown tags (like ```html), NO JSON, ONLY HTML.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        
        raw_content = response.choices[0].message.content.strip()

        # 【超重要】AIがもしマークダウン(```html)を混ぜた場合の強制除去
        clean_html = re.sub(r'^```html\s*|\s*```$', '', raw_content, flags=re.MULTILINE)
        
        # 万が一JSON形式で返ってきた場合の保険
        if clean_html.startswith('{'):
            try:
                data = json.loads(clean_html)
                clean_html = data.get('html', data.get('html_code', clean_html))
            except:
                pass

        # index.htmlとして保存
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(clean_html)
        
        print(f"✅ 修正完了: {topic} のHTMLを正常に書き出しました。")
    except Exception as e:
        print(f"❌ エラー発生: {e}")
        exit(1)

if __name__ == "__main__":
    generate_perfect_site()
