import os
import json
import random
import requests
from openai import OpenAI
from atproto import Client as BskyClient
from mastodon import Mastodon

# 1. 初期設定と認証
def get_clients():
    return {
        "openai": OpenAI(api_key=os.environ.get("OPENAI_API_KEY")),
        "bsky": BskyClient()
    }

# 2. 悩み（トピック）の選定
def get_random_topic():
    topics = [
        "BMIと健康管理の重要性", "複利計算で将来の資産をシミュレーション", 
        "時間管理マトリックスの使い方", "毎日の必要カロリー計算", 
        "タイピング速度向上トレーニング", "暗号通貨の損益計算方法"
    ]
    return random.choice(topics)

# 3. ツールとSEO記事の生成
def generate_content(client, topic):
    print(f"🚀 トピック '{topic}' でコンテンツを生成中...")
    
    prompt = f"""
    以下の条件で、最高品質のWebツールとSEO記事を1つのHTMLファイルで作成してください。
    
    【トピック】: {topic}
    【条件】:
    1. デザイン: Tailwind CSSを使用し、モバイル対応でモダンなデザインにすること。
    2. 記事: Google AdSense審査を突破するため、2000文字以上の専門的な解説文（日本語）を含めること。
    3. ツール: JavaScriptで完結する、実際に動作する便利なツール（計算機など）を実装すること。
    4. 多言語化: 日本語、英語、フランス語、ドイツ語の切り替えボタンをつけること。
    5. 広告枠: 'AdSense Placeholder' というコメントをHTML内に残すこと。
    
    返信は必ず以下のJSON形式のみで行ってください。
    {{
        "title": "ページのタイトル",
        "description": "SNS投稿用の短い紹介文",
        "html_code": "HTML全コード"
    }}
    """
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={ "type": "json_object" }
    )
    return json.loads(response.choices[0].message.content)

# 4. SNSへの自動投稿
def post_to_sns(data):
    url = "https://mikanntool.com" # あなたのドメイン
    message = f"【新着ツール】{data['description']}\n詳しくはこちら：{url}"

    # Bluesky投稿
    try:
        bsky = BskyClient()
        bsky.login(os.environ.get("BSKY_HANDLE"), os.environ.get("BSKY_PASSWORD"))
        bsky.send_post(text=message)
        print("✅ Posted to Bluesky")
    except Exception as e:
        print(f"⚠️ Bluesky error: {e}")

    # Mastodon投稿
    try:
        masto = Mastodon(
            access_token=os.environ.get("MASTODON_ACCESS_TOKEN"),
            api_base_url=os.environ.get("MASTODON_API_BASE")
        )
        masto.status_post(message)
        print("✅ Posted to Mastodon")
    except Exception as e:
        print(f"⚠️ Mastodon error: {e}")

# 5. メイン実行
def main():
    clients = get_clients()
    topic = get_random_topic()
    content = generate_content(clients["openai"], topic)
    
    # index.html として保存
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(content["html_code"])
    
    print(f"✅ ファイル保存完了: {content['title']}")
    
    # SNS投稿
    post_to_sns(content)

if __name__ == "__main__":
    main()
