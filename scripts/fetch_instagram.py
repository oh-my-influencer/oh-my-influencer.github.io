"""
fetch_instagram.py

역할:
  1. Apify Instagram Hashtag Scraper로 해시태그별 게시물 수집
  2. 게시물에서 ownerUsername 추출 (신규만)
  3. Apify Instagram Profile Scraper로 팔로워 수 등 상세 정보 수집
  4. 프로필 이미지를 data/images/ 에 다운로드 후 로컬 경로로 저장
  5. 필터 적용 후 기존 + 신규 병합해서 instagram.json 저장

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

ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "data" / "config.json"
OUTPUT_PATH = ROOT / "data" / "instagram.json"
IMAGES_DIR = ROOT / "data" / "images"

APIFY_BASE = "https://api.apify.com/v2"
HASHTAG_ACTOR = "apify~instagram-hashtag-scraper"
PROFILE_ACTOR = "apify~instagram-profile-scraper"


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
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {acc["handle"]: acc for acc in data.get("influencers", [])}


# ── Apify Actor 실행 공통 함수 ─────────────────────────────
def run_actor(
    token: str, actor_id: str, input_data: dict, timeout_sec: int = 300
) -> list[dict]:
    run_resp = requests.post(
        f"{APIFY_BASE}/acts/{actor_id}/runs",
        params={"token": token},
        json=input_data,
        timeout=30,
    )
    run_resp.raise_for_status()
    run_data = run_resp.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]
    print(f"   Actor 실행됨 (run_id: {run_id})")

    for _ in range(timeout_sec // 5):
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
        print(f"   ⚠️ 타임아웃 ({timeout_sec}초)", file=sys.stderr)
        return []

    items = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=30,
    )
    items.raise_for_status()
    return items.json()


# ── 이미지 다운로드 ────────────────────────────────────────
def download_image(url: str, handle: str) -> str:
    """
    프로필 이미지를 data/images/ig_{handle}.jpg 로 저장하고
    HTML에서 참조할 상대 경로를 반환한다.
    이미 존재하면 다운로드 스킵.
    """
    if not url:
        return ""

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    dest = IMAGES_DIR / f"ig_{handle}.jpg"

    # 이미 있으면 스킵
    if dest.exists():
        return f"data/images/ig_{handle}.jpg"

    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return f"data/images/ig_{handle}.jpg"
    except Exception as e:
        print(f"   ⚠️ 이미지 다운로드 실패 ({handle}): {e}", file=sys.stderr)
        return ""


# ── Step 1: 해시태그 → username 목록 수집 ─────────────────
def collect_usernames(token: str, hashtag: str, max_results: int) -> set[str]:
    posts = run_actor(
        token,
        HASHTAG_ACTOR,
        {
            "hashtags": [hashtag],
            "resultsLimit": max_results,
            "addParentData": False,
        },
    )
    return {post["ownerUsername"] for post in posts if post.get("ownerUsername")}


# ── Step 2: username → 프로필 상세 + 이미지 다운로드 ──────
def fetch_profiles(token: str, usernames: list[str]) -> list[dict]:
    results = []
    chunk_size = 50
    for i in range(0, len(usernames), chunk_size):
        chunk = usernames[i : i + chunk_size]
        print(f"   프로필 조회 중: {len(chunk)}개")
        profiles = run_actor(
            token, PROFILE_ACTOR, {"usernames": chunk}, timeout_sec=300
        )

        for p in profiles:
            handle = p.get("username") or p.get("handle", "")
            followers = p.get("followersCount") or p.get("followers", 0) or 0
            if not handle:
                continue

            # 이미지 다운로드 (HD 우선)
            pic_url = p.get("profilePicUrlHD") or p.get("profilePicUrl") or ""
            print(f"   📸 이미지 다운로드: @{handle}")
            local_image = download_image(pic_url, handle)

            results.append(
                {
                    "id": f"ig_{handle}",
                    "platform": "instagram",
                    "handle": handle,
                    "name": p.get("fullName") or handle,
                    "profile_url": f"https://www.instagram.com/{handle}/",
                    "profile_image": local_image,
                    "followers": followers,
                    "following": p.get("followsCount") or 0,
                    "posts_count": p.get("postsCount") or 0,
                    "engagement_rate": None,
                    "bio": (p.get("biography") or "")[:100],
                    "is_verified": p.get("verified") or False,
                    "category": ["skincare", "beauty"],
                    "tier": get_tier(followers),
                    "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                }
            )
    return results


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

    existing = load_existing(OUTPUT_PATH)
    print(f"📂 기존 계정 {len(existing)}개 로드됨\n")

    # Step 1: 해시태그 → 신규 username 수집
    all_usernames: set[str] = set()
    for tag in hashtags:
        print(f"🔍 #{tag} 해시태그 크롤링 중...")
        usernames = collect_usernames(token, tag, max_results)
        new_in_tag = usernames - set(existing.keys()) - all_usernames
        all_usernames.update(new_in_tag)
        print(
            f"   → 신규 {len(new_in_tag)}개 / 중복 {len(usernames) - len(new_in_tag)}개 스킵\n"
        )

    print(f"📊 신규 발굴 username: {len(all_usernames)}개")

    # Step 2: 신규 username만 프로필 + 이미지 수집
    new_accounts: list[dict] = []
    if all_usernames:
        print("\n👤 프로필 상세 정보 + 이미지 다운로드 중...")
        new_accounts = fetch_profiles(token, list(all_usernames))
        print(f"   → {len(new_accounts)}개 완료")

    # 병합 + 필터
    merged = {**existing, **{acc["handle"]: acc for acc in new_accounts}}
    filtered = [acc for acc in merged.values() if min_f <= acc["followers"] <= max_f]
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
