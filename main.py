import os
import re
from openai import OpenAI

# 認証設定
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_tool():
    # トピックを「健康管理」に固定し、AdSenseが好む専門的な内容を指示
    topic = "BMIと健康管理シミュレーター"
    
    print("🚀 サイト生成を開始します...")

    # AIへの命令（JSONではなく、直接HTMLを出すように指示）
    prompt = f"""
    Create a professional single-file HTML for '{topic}'.
    - Use Tailwind CSS.
    - Include 2000+ characters of Japanese SEO article.
    - Features: JS BMI calculator.
    - Return ONLY the raw HTML code. Do NOT use markdown code blocks like ```html.
    - Start directly with <!DOCTYPE html>.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.choices[0].message.content.strip()

        # 【修正の核心】もしAIが ```html ... ``` と返してきた場合、その中身だけを強制抽出
        if "```" in content:
            # <!DOCTYPE から </html> までを正規表現で抜き出す
            match = re.search(r'(<!DOCTYPE html>.*</html>)', content, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)
            else:
                # 記号だけを物理的に削除
                content = content.replace("```html", "").replace("```", "").strip()

        # index.htmlとして保存（これでVercelがウェブサイトとして認識します）
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✅ index.html の正常な書き出しに成功しました。")
    except Exception as e:
        print(f"❌ エラー発生: {e}")
        exit(1)

if __name__ == "__main__":
    generate_tool()
