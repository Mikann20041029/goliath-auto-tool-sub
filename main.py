import os
import random
import re
from openai import OpenAI

# 認証設定
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_perfect_site():
    # トピックを固定してまずは確実な「成功」を掴む
    topic = "理想の体型を作るためのBMI・健康管理シミュレーター"
    
    print(f"💎 サイト生成開始: {topic}")

    prompt = f"""
    Create a professional single-file HTML website for '{topic}'.
    - Use Tailwind CSS for a high-end, modern, and clean UI.
    - Include 2000+ characters Japanese SEO article about health.
    - Features: A fully working JavaScript BMI calculator tool.
    - Multi-language buttons (JP, EN, FR, DE).
    - Output ONLY the raw HTML code starting with <!DOCTYPE html>.
    - ABSOLUTELY NO explanation, NO markdown blocks (```html), NO JSON. Just raw code.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.choices[0].message.content.strip()

        # 【修正の核心】もしAIが ```html ... ``` と返してきた場合、その中身だけを抽出
        if "```" in content:
            # 正規表現で <!DOCTYPE から </html> までを抜き出す
            match = re.search(r'(<!DOCTYPE html>.*</html>)', content, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)
            else:
                # 記号だけを力技で消す
                content = content.replace("```html", "").replace("```", "").strip()

        # index.htmlとして保存
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✅ ブラウザが即座に認識できる形式で index.html を書き出しました。")
    except Exception as e:
        print(f"❌ エラー発生: {e}")
        exit(1)

if __name__ == "__main__":
    generate_perfect_site()
