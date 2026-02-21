"""
fetch_instagram.py

역할:
  1. data/config.json 에서 Instagram 해시태그 목록과 필터 조건을 읽는다.
  2. Apify Instagram Hashtag Scraper로 해시태그별 게시물을 수집한다.
  3. 게시물 작성자 계정을 추출해 중복을 제거한다.
  4. 팔로워 수 필터를 적용한다.
  5. data/instagram.json 으로 저장한다.

환경변수:
  APIFY_API_TOKEN : Apify API 토큰 (GitHub Secret으로 주입)

Apify Actor:
  apify/instagram-hashtag-scraper
  https://apify.com/apify/instagram-hashtag-scraper

크레딧 소모 예시:
  해시태그 7개 × max_results 50개 = 게시물 350개
  → 약 $0.10 ~ $0.20 소모 (초기 $5 크레딧으로 수십 회 실행 가능)

실행:
  APIFY_API_TOKEN=xxx uv run python scripts/fetch_instagram.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ── 경로 설정 ──────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "data" / "config.json"
OUTPUT_PATH = ROOT / "data" / "instagram.json"

APIFY_BASE = "https://api.apify.com/v2"
ACTOR_ID = "apify~instagram-hashtag-scraper"


# ── Tier 분류 ──────────────────────────────────────────────
def get_tier(followers: int) -> str:
    if followers >= 1_000_000:
        return "mega"
    if followers >= 100_000:
        return "macro"
    if followers >= 50_000:
        return "mid"
    if followers >= 10_000:
        return "micro"
    return "nano"


# ── Apify Actor 실행 + 결과 대기 ──────────────────────────
def run_actor(token: str, hashtag: str, max_results: int) -> list[dict]:
    """Apify Actor를 실행하고 결과를 반환한다."""

    # 1) Actor 실행
    run_resp = requests.post(
        f"{APIFY_BASE}/acts/{ACTOR_ID}/runs",
        params={"token": token},
        json={
            "hashtags": [hashtag],
            "resultsLimit": max_results,
            "addParentData": False,
        },
        timeout=30,
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["data"]["id"]
    dataset_id = run_resp.json()["data"]["defaultDatasetId"]
    print(f"   Actor 실행됨 (run_id: {run_id})")

    # 2) 완료 대기 (최대 5분)
    for _ in range(60):
        time.sleep(5)
        status_resp = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}",
            params={"token": token},
            timeout=10,
        )
        status = status_resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"   ⚠️ Actor 실패: {status}", file=sys.stderr)
            return []
    else:
        print("   ⚠️ 타임아웃: Actor가 5분 내에 완료되지 않음", file=sys.stderr)
        return []

    # 3) 결과 가져오기
    items_resp = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=30,
    )
    items_resp.raise_for_status()
    return items_resp.json()


# ── 게시물 → 인플루언서 계정 추출 ─────────────────────────
def extract_accounts(posts: list[dict]) -> dict[str, dict]:
    """게시물 목록에서 작성자 계정 정보를 추출한다. {username: account_dict}"""
    accounts = {}
    for post in posts:
        owner = post.get("ownerUsername") or post.get("owner", {}).get("username")
        if not owner:
            continue
        if owner in accounts:
            continue

        followers = (
            post.get("ownerFollowersCount")
            or post.get("owner", {}).get("followersCount")
            or 0
        )
        full_name = (
            post.get("ownerFullName") or post.get("owner", {}).get("fullName") or owner
        )
        profile_pic = (
            post.get("ownerProfilePicUrl")
            or post.get("owner", {}).get("profilePicUrl")
            or ""
        )

        accounts[owner] = {
            "id": f"ig_{owner}",
            "platform": "instagram",
            "handle": owner,
            "name": full_name,
            "profile_url": f"https://www.instagram.com/{owner}/",
            "profile_image": profile_pic,
            "followers": followers,
            "engagement_rate": None,  # Hashtag Scraper는 참여율 미제공
            "video_count": None,
            "category": ["skincare", "beauty"],
            "tier": get_tier(followers),
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }
    return accounts


# ── 메인 ──────────────────────────────────────────────────
def main() -> None:
    token = os.environ.get("APIFY_API_TOKEN", "")
    if not token:
        print("❌ APIFY_API_TOKEN 환경변수가 설정되지 않았습니다.", file=sys.stderr)
        sys.exit(1)

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    ig_config = config.get("instagram", {})
    hashtags = ig_config.get("hashtags", [])
    max_results = ig_config.get("max_results_per_hashtag", 50)
    filters = config.get("filters", {})
    min_f = filters.get("min_followers", 10_000)
    max_f = filters.get("max_followers", 1_000_000)

    all_accounts: dict[str, dict] = {}

    for tag in hashtags:
        print(f"🔍 #{tag} 크롤링 중... (최대 {max_results}개 게시물)")
        posts = run_actor(token, tag, max_results)
        accounts = extract_accounts(posts)
        before = len(all_accounts)
        all_accounts.update(accounts)
        print(f"   → 신규 계정 {len(all_accounts) - before}개 추가")

    # 필터 적용
    filtered = [
        acc for acc in all_accounts.values() if min_f <= acc["followers"] <= max_f
    ]
    filtered.sort(key=lambda x: x["followers"], reverse=True)

    print(f"\n✅ 필터 통과: {len(filtered)}개 (팔로워 {min_f:,} ~ {max_f:,})")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "count": len(filtered),
                "influencers": filtered,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"💾 저장 완료: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
