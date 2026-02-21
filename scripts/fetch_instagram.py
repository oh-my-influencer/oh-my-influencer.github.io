"""
fetch_instagram.py

역할:
  1. data/config.json 에서 Instagram 해시태그 목록과 필터 조건을 읽는다.
  2. 기존 data/instagram.json 에서 이미 알려진 계정 목록을 로드한다.
  3. Apify Instagram Hashtag Scraper로 해시태그별 게시물을 수집한다.
  4. 신규 계정(기존에 없던 핸들)만 Apify로 추가 처리한다. (비용 최적화)
  5. 팔로워 수 필터를 적용하고 기존 + 신규를 합쳐 저장한다.

환경변수:
  APIFY_API_TOKEN : Apify API 토큰 (GitHub Secret으로 주입)

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


# ── 기존 데이터 로드 ───────────────────────────────────────
def load_existing(path: Path) -> dict[str, dict]:
    """기존 instagram.json에서 {handle: account} 딕셔너리를 반환한다."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {acc["handle"]: acc for acc in data.get("influencers", [])}


# ── Apify Actor 실행 + 결과 대기 ──────────────────────────
def run_actor(token: str, hashtag: str, max_results: int) -> list[dict]:
    """Apify Actor를 실행하고 게시물 목록을 반환한다."""

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
    run_data = run_resp.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]
    print(f"   Actor 실행됨 (run_id: {run_id})")

    # 완료 대기 (최대 5분)
    for _ in range(60):
        time.sleep(5)
        status = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}",
            params={"token": token},
            timeout=10,
        ).json()["data"]["status"]

        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"   ⚠️ Actor 실패: {status}", file=sys.stderr)
            return []
    else:
        print("   ⚠️ 타임아웃: 5분 내 완료되지 않음", file=sys.stderr)
        return []

    items = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=30,
    )
    items.raise_for_status()
    return items.json()


# ── 게시물 → 계정 추출 ────────────────────────────────────
def extract_accounts(posts: list[dict]) -> dict[str, dict]:
    """게시물 목록에서 작성자 핸들 기준으로 계정 정보를 추출한다."""
    accounts: dict[str, dict] = {}
    for post in posts:
        handle = post.get("ownerUsername") or post.get("owner", {}).get("username")
        if not handle or handle in accounts:
            continue

        followers = (
            post.get("ownerFollowersCount")
            or post.get("owner", {}).get("followersCount")
            or 0
        )
        accounts[handle] = {
            "id": f"ig_{handle}",
            "platform": "instagram",
            "handle": handle,
            "name": post.get("ownerFullName")
            or post.get("owner", {}).get("fullName")
            or handle,
            "profile_url": f"https://www.instagram.com/{handle}/",
            "profile_image": post.get("ownerProfilePicUrl")
            or post.get("owner", {}).get("profilePicUrl")
            or "",
            "followers": followers,
            "engagement_rate": None,
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
    max_results = ig_config.get("max_results_per_hashtag", 30)
    filters = config.get("filters", {})
    min_f = filters.get("min_followers", 10_000)
    max_f = filters.get("max_followers", 1_000_000)

    # 기존 계정 로드 (중복 스킵용)
    existing = load_existing(OUTPUT_PATH)
    print(f"📂 기존 계정 {len(existing)}개 로드됨")

    newly_found: dict[str, dict] = {}

    for tag in hashtags:
        print(f"\n🔍 #{tag} 크롤링 중... (최대 {max_results}개 게시물)")
        posts = run_actor(token, tag, max_results)
        accounts = extract_accounts(posts)

        # 신규 계정만 추가
        new_in_tag = {
            h: a
            for h, a in accounts.items()
            if h not in existing and h not in newly_found
        }
        newly_found.update(new_in_tag)
        skipped = len(accounts) - len(new_in_tag)
        print(f"   → 신규 {len(new_in_tag)}개 발굴 / {skipped}개 중복 스킵")

    print(f"\n📊 이번 실행 신규 발굴: {len(newly_found)}개")

    # 기존 + 신규 병합 후 필터 적용
    merged = {**existing, **newly_found}
    filtered = [acc for acc in merged.values() if min_f <= acc["followers"] <= max_f]
    filtered.sort(key=lambda x: x["followers"], reverse=True)

    print(f"✅ 필터 통과: {len(filtered)}개 (팔로워 {min_f:,} ~ {max_f:,})")

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
