import os
import json
import math
import datetime
from typing import Any, Dict, List, Optional
import requests

ROOT = "goliath"
AFFILIATES_PATH = f"{ROOT}/affiliates.json"

GENRES = [
    "Web/Hosting",
    "Dev/Tools",
    "AI/Automation",
    "Security/Privacy",
    "Media: Video/Audio",
    "PDF/Docs",
    "Images/Design",
    "Data/Spreadsheets",
    "Business/Accounting/Tax",
    "Marketing/Social",
    "Productivity",
    "Education/Language",
]

# stats endpoint: Cloudflare Worker の /stats を想定
CLICK_STATS_ENDPOINT = os.getenv("CLICK_STATS_ENDPOINT", "").strip()  # 例: https://xxx.workers.dev/stats
CLICK_STATS_TOKEN = os.getenv("CLICK_STATS_TOKEN", "").strip()
CLICK_STATS_DAYS = int(os.getenv("CLICK_STATS_DAYS", "7"))

def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def clamp(lo: int, hi: int, x: float) -> int:
    return max(lo, min(hi, int(round(x))))

def score_to_priority(clicks: int) -> int:
    # 暴れ抑制: log(1+clicks)
    score = math.log(1.0 + max(0, int(clicks)))
    # 例: priority = clamp(30, 90, 30 + score*20)
    return clamp(30, 90, 30 + score * 20)

def fetch_stats() -> Dict[str, int]:
    if not CLICK_STATS_ENDPOINT:
        print("[stats] skip: missing CLICK_STATS_ENDPOINT")
        return {}
    headers = {}
    if CLICK_STATS_TOKEN:
        headers["authorization"] = f"Bearer {CLICK_STATS_TOKEN}"

    params = {"days": str(CLICK_STATS_DAYS)}
    r = requests.get(CLICK_STATS_ENDPOINT, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # 期待形式: { "by_ad_id": { "ad1": 12, "ad2": 0, ... } }
    by = (data or {}).get("by_ad_id") or {}
    out: Dict[str, int] = {}
    if isinstance(by, dict):
        for k, v in by.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
    print("[stats] fetched", {"count": len(out)})
    return out

def update_affiliates(aff: Any, by_ad_id: Dict[str, int]) -> bool:
    if not isinstance(aff, dict):
        return False

    changed = False
    for genre in GENRES:
        arr = aff.get(genre)
        if not isinstance(arr, list):
            continue
        for item in arr:
            if not isinstance(item, dict):
                continue
            aid = (item.get("id") or "").strip()
            if not aid:
                continue
            clicks = int(by_ad_id.get(aid, 0))
            new_pr = score_to_priority(clicks) if clicks > 0 else int(item.get("priority", 50) or 50)
            old_pr = int(item.get("priority", 50) or 50)
            if new_pr != old_pr:
                item["priority"] = new_pr
                changed = True
    return changed

def main() -> None:
    aff = read_json(AFFILIATES_PATH, {})
    by = fetch_stats()
    changed = update_affiliates(aff, by)
    if changed:
        write_json(AFFILIATES_PATH, aff)
        print("[ok] affiliates.json updated")
    else:
        print("[ok] no changes")

if __name__ == "__main__":
    main()
