"""
fetch_tiktok.py

역할:
  1. data/config.json 에서 TikTok 해시태그 목록과 필터 조건을 읽는다.
  2. Apify TikTok Hashtag Scraper로 해시태그별 영상 + 작성자 정보를 수집한다.
     (Instagram과 달리 1단계로 팔로워 수까지 바로 가져올 수 있음)
  3. 신규 계정만 처리 (기존 tiktok.json 에 있는 핸들은 스킵)
  4. 필터 적용 후 기존 + 신규 병합해서 tiktok.json 저장

환경변수:
  APIFY_API_TOKEN : Apify API 토큰 (GitHub Secret으로 주입)

Apify Actor:
  clockworks/tiktok-hashtag-scraper
  https://apify.com/clockworks/tiktok-hashtag-scraper

실행:
  APIFY_API_TOKEN=xxx uv run python scripts/fetch_tiktok.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from utils import detect_language

load_dotenv()

ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "data" / "config.json"
OUTPUT_PATH = ROOT / "data" / "tiktok.json"

APIFY_BASE = "https://api.apify.com/v2"
HASHTAG_ACTOR = "clockworks~tiktok-hashtag-scraper"


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


# ── Apify Actor 실행 ───────────────────────────────────────
def run_actor(token: str, hashtag: str, max_results: int) -> list[dict]:
    run_resp = requests.post(
        f"{APIFY_BASE}/acts/{HASHTAG_ACTOR}/runs",
        params={"token": token},
        json={
            "hashtags": [hashtag],
            "resultsPerPage": max_results,
            "maxRequestRetries": 3,
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
        print("   ⚠️ 타임아웃 (5분)", file=sys.stderr)
        return []

    items = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=30,
    )
    items.raise_for_status()
    return items.json()


# ── 영상 → 작성자 계정 추출 ───────────────────────────────
def extract_accounts(videos: list[dict]) -> dict[str, dict]:
    """
    TikTok Hashtag Scraper 응답에서 작성자 계정 정보를 추출한다.
    핸들 기준으로 중복 제거.
    """
    accounts: dict[str, dict] = {}
    for v in videos:
        author = v.get("authorMeta") or v.get("author") or {}
        handle = author.get("name") or author.get("uniqueId") or ""
        if not handle or handle in accounts:
            continue

        followers = (
            author.get("fans")
            or author.get("followers")
            or author.get("followersCount")
            or 0
        )
        following = author.get("following") or author.get("followingCount") or 0
        heart = author.get("heart") or author.get("digg") or 0
        video_cnt = author.get("video") or author.get("videoCount") or 0
        nick = author.get("nickName") or author.get("nickname") or handle
        avatar = author.get("avatar") or author.get("avatarLarger") or ""
        verified = author.get("verified") or False

        # region 코드 → country (KR/JP 매핑, 없으면 언어 감지로 보완)
        region = (author.get("region") or "").upper()
        if region in ("KR", "JP"):
            country = region
        else:
            sig = (author.get("signature") or "") + " " + nick
            country = detect_language(sig, handle=handle)

        accounts[handle] = {
            "id": f"tt_{handle}",
            "platform": "tiktok",
            "handle": handle,
            "name": nick,
            "profile_url": f"https://www.tiktok.com/@{handle}",
            "profile_image": avatar,
            "followers": followers,
            "following": following,
            "total_likes": heart,
            "video_count": video_cnt,
            "engagement_rate": None,
            "country": country,
            "is_verified": verified,
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

    tt_config = config.get("tiktok", {})
    hashtags = tt_config.get("hashtags", [])
    max_results = tt_config.get("max_results_per_hashtag", 30)
    filters = config.get("filters", {})
    min_f = filters.get("min_followers", 1_000)

    if not hashtags:
        print("⚠️ config.json에 tiktok.hashtags 가 없습니다.", file=sys.stderr)
        sys.exit(1)

    existing = load_existing(OUTPUT_PATH)
    print(f"📂 기존 계정 {len(existing)}개 로드됨\n")

    newly_found: dict[str, dict] = {}

    for tag in hashtags:
        print(f"🔍 #{tag} 크롤링 중... (최대 {max_results}개 영상)")
        videos = run_actor(token, tag, max_results)
        accounts = extract_accounts(videos)
        new_in_tag = {
            h: a
            for h, a in accounts.items()
            if h not in existing and h not in newly_found
        }
        newly_found.update(new_in_tag)
        skipped = len(accounts) - len(new_in_tag)
        print(f"   → 신규 {len(new_in_tag)}개 발굴 / {skipped}개 중복 스킵\n")

    print(f"📊 이번 실행 신규 발굴: {len(newly_found)}개")

    # 병합 + 필터
    merged = {**existing, **newly_found}
    allowed = [c.upper() for c in filters.get("allowed_countries", [])]
    filtered = [
        acc
        for acc in merged.values()
        if acc["followers"] >= min_f
        and (not allowed or acc.get("country", "") in allowed)
    ]
    filtered.sort(key=lambda x: x["followers"], reverse=True)

    print(f"✅ 필터 통과: {len(filtered)}개 (팔로워 {min_f:,})")

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
