import os, json, urllib.request

def gh_api(method: str, url: str, token: str, payload=None):
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req.add_header("Content-Type", "application/json")
        req.data = data
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        return resp.getcode(), json.loads(body) if body else {}

def read_file(path: str, limit_chars: int = 12000) -> str:
    if not os.path.exists(path):
        return "(run_log.txt not found)"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        s = f.read()
    if len(s) <= limit_chars:
        return s
    return s[:limit_chars] + "\n...(truncated)..."

def main():
    token = os.environ.get("GITHUB_TOKEN", "")
    repo  = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")

    if not token or not repo or not run_id:
        raise SystemExit("Missing GITHUB_TOKEN/GITHUB_REPOSITORY/GITHUB_RUN_ID")

    run_url = f"{server}/{repo}/actions/runs/{run_id}"
    log = read_file("run_log.txt")

    title = f"[Goliath] Run report #{run_id}"
    body = (
        f"Run: {run_url}\n\n"
        f"### run_log.txt (excerpt)\n"
        f"```txt\n{log}\n```\n"
    )

    api = f"https://api.github.com/repos/{repo}/issues"
    payload = {"title": title, "body": body}

    code, res = gh_api("POST", api, token, payload)
    if code not in (200, 201):
        raise SystemExit(f"Failed to create issue: HTTP {code} {res}")

if __name__ == "__main__":
    main()