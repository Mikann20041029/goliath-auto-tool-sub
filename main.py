import os
import json
from openai import OpenAI

# 鍵の読み込み
api_key = os.environ.get("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

def generate_tool():
    print("🚀 AIによるツール生成を開始します...")
    
    prompt = """
    Create a professional 'BMI Calculator' web tool.
    - Single HTML file using Tailwind CSS.
    - Include a 2000-character SEO article in Japanese about health and BMI.
    - Multi-language support buttons (JP, EN, FR, DE).
    - Design: Modern and clean.
    Return ONLY JSON format: {"title": "title", "html": "full html code"}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={ "type": "json_object" }
        )
        data = json.loads(response.choices[0].message.content)
        
        # index.html として保存（Vercelのトップページになります）
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(data['html'])
        
        print(f"✅ 生成成功: {data['title']}")
    except Exception as e:
        print(f"❌ エラー発生: {e}")
        exit(1)

if __name__ == "__main__":
    generate_tool()
