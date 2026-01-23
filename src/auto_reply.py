import os
import json
import re
from atproto import Client as BlueskyClient
from mastodon import Mastodon
import tweepy

def parse_issue_body(body: str):
    print("=== Raw Issue Body Start ===")
    print(body)
    print("=== Raw Issue Body End ===")
    
    drafts = []
    blocks = re.split(r'(?=\n?#\d+\s*\[)', body.strip())
    print(f"Split into {len(blocks)} potential blocks")
    
    for i, block in enumerate(blocks):
        block = block.strip()
        if not block or not block.startswith('#'):
            continue
        print(f"\n--- Processing Block {i+1} ---")
        print(block)
        
        platform_match = re.search(r'#\d+\s*\[([^\]]+)\]', block, re.IGNORECASE)
        if platform_match:
            platform = platform_match.group(1).strip().upper()
            print(f"Found platform: {platform}")
        else:
            continue
        
        url_match = re.search(r'https?://[^\s\n]+', block)
        if url_match:
            target_url = url_match.group(0).rstrip('.').strip()
            print(f"Found URL: {target_url}")
        else:
            continue
        
        reply_match = re.search(r'返信文:\s*([\s\S]*?)(?=\n?#\d+|$)', block, re.IGNORECASE)
        if reply_match:
            reply_text = reply_match.group(1).strip()
            if reply_text:
                print(f"Found reply: {reply_text[:100]}...")
                drafts.append({
                    "platform": platform,
                    "target_url": target_url,
                    "reply": reply_text
                })
    
    print(f"\nParsed {len(drafts)} valid drafts")
    return drafts

def post_to_bluesky(target_url: str, reply_text: str):
    handle = os.environ.get('BSKY_HANDLE')
    app_password = os.environ.get('BSKY_PASSWORD')
    print(f"Bluesky creds: handle={handle[:5] if handle else 'None'}..., password={'set' if app_password else 'missing'}")
    if not handle or not app_password:
        print("Bluesky credentials missing → skip")
        return False
    
    try:
        client = BlueskyClient()
        client.login(handle, app_password)
        print("Bluesky login success")
        
        match = re.search(r'/profile/([^/]+)/post/([^/]+)', target_url)
        if not match:
            print(f"Invalid Bluesky URL: {target_url}")
            return False
        
        author_handle = match.group(1)
        rkey = match.group(2)
        root_uri = f"at://{author_handle}/app.bsky.feed.post/{rkey}"
        print(f"Replying to: {root_uri}")
        
        client.send_post(
            text=reply_text,
            reply_to={'root': {'uri': root_uri, 'cid': None}, 'parent': {'uri': root_uri, 'cid': None}}
        )
        print(f"Bluesky SUCCESS: {target_url}")
        return True
    except Exception as e:
        print(f"Bluesky ERROR: {str(e)}")
        return False

def post_to_mastodon(target_url: str, reply_text: str):
    access_token = os.environ.get('MASTODON_ACCESS_TOKEN')
    instance_url = os.environ.get('MASTODON_API_BASE')
    print(f"Mastodon creds: token={'set' if access_token else 'missing'}, base={instance_url}")
    if not access_token or not instance_url:
        print("Mastodon credentials missing → skip")
        return False
    
    try:
        mastodon = Mastodon(
            access_token=access_token,
            api_base_url=instance_url.rstrip('/')
        )
        print("Mastodon init success")
        
        status_id = target_url.split('/')[-1].split('?')[0]
        if not status_id.isdigit():
            print(f"Invalid Mastodon ID: {status_id}")
            return False
        
        mastodon.status_post(status=reply_text, in_reply_to_id=int(status_id))
        print(f"Mastodon SUCCESS: {target_url}")
        return True
    except Exception as e:
        print(f"Mastodon ERROR: {str(e)}")
        return False

def post_to_x(target_url: str, reply_text: str):
    consumer_key = os.environ.get('X_API_KEY')
    consumer_secret = os.environ.get('X_API_SECRET')
    access_token = os.environ.get('X_ACCESS_TOKEN')
    access_token_secret = os.environ.get('X_ACCESS_SECRET')
    print(f"X creds: consumer_key={'set' if consumer_key else 'missing'}, access_token={'set' if access_token else 'missing'}")
    if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
        print("X credentials missing → skip")
        return False
    
    try:
        auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
        api = tweepy.API(auth)
        print("X auth success")
        
        match = re.search(r'/status/(\d+)', target_url)
        if not match:
            print(f"Invalid X URL: {target_url}")
            return False
        status_id = match.group(1)
        
        api.update_status(status=reply_text, in_reply_to_status_id=int(status_id))
        print(f"X SUCCESS: {target_url}")
        return True
    except Exception as e:
        print(f"X ERROR: {str(e)}")
        return False

def main():
    event_path = os.environ.get('GITHUB_EVENT_PATH')
    if not event_path:
        print("No event path")
        return
    
    with open(event_path, 'r') as f:
        event = json.load(f)
    
    issue_body = event.get('issue', {}).get('body', '')
    if not issue_body:
        print("Empty issue body")
        return
    
    # ★ここ！draftsをちゃんと定義
    drafts = parse_issue_body(issue_body)
    
    if not drafts:
        print("WARNING: No valid drafts parsed")
        return
    
    success_count = 0
    for i, d in enumerate(drafts, 1):
        print(f"\nProcessing {i}/{len(drafts)}: {d['platform']} - {d['target_url']}")
        platform = d['platform'].upper()
        url = d['target_url']
        text = d['reply']
        
        if platform in ['BLUESKY', 'BSKY']:
            if post_to_bluesky(url, text):
                success_count += 1
        elif platform in ['MASTODON', 'MSTD', 'MASTO']:
            if post_to_mastodon(url, text):
                success_count += 1
        elif platform in ['X', 'TWITTER']:
            if post_to_x(url, text):
                success_count += 1
        elif platform == 'HN':
            print(f"Skipping HN (no write API)")
        else:
            print(f"Unsupported: {platform}")
    
    print(f"\n=== FINAL: {success_count}/{len(drafts)} sent ===")

if __name__ == '__main__':
    main()
