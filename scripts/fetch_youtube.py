"""
fetch_youtube.py

역할:
  1. data/config.json 에서 검색 키워드와 필터 조건을 읽는다.
  2. YouTube Search API 로 키워드별 채널을 검색해 발굴한다.
  3. YouTube Channels API 로 각 채널의 상세 정보를 가져온다.
  4. 필터(구독자 수 범위, 최소 영상 수)를 적용한다.
  5. 결과를 data/youtube.json 으로 저장한다.

환경변수:
  YOUTUBE_API_KEY : YouTube Data API v3 키 (GitHub Secret 으로 주입)

YouTube API Quota 소모 예시 (기본 한도: 10,000 유닛/일):
  - Search:   100 유닛/요청
  - Channels:   1 유닛/요청
  키워드 5개 × max_results 20 = Search 500 유닛
  채널 상세 조회 (50개씩): ~3 유닛
  → 총 약 503 유닛 소모 (여유 충분)

실행:
  YOUTUBE_API_KEY=xxx uv run python scripts/fetch_youtube.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()  # 로컬 실행 시 .env 파일에서 환경변수 로드 (CI에서는 무시됨)

# ── 경로 설정 ──────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "data" / "config.json"
OUTPUT_PATH = ROOT / "data" / "youtube.json"

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


# ── Tier 분류 ──────────────────────────────────────────────
def get_tier(subscribers: int) -> str:
    if subscribers >= 1_000_000:
        return "mega"
    if subscribers >= 100_000:
        return "macro"
    if subscribers >= 50_000:
        return "mid"
    if subscribers >= 10_000:
        return "micro"
    return "nano"


# ── YouTube API 호출 헬퍼 ──────────────────────────────────
def yt_get(endpoint: str, api_key: str, params: dict) -> dict:
    params["key"] = api_key
    resp = requests.get(f"{YOUTUBE_API_BASE}/{endpoint}", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ── 1단계: 키워드로 채널 ID 검색 ──────────────────────────
def search_channel_ids(api_key: str, keyword: str, max_results: int) -> list[str]:
    """키워드로 YouTube 채널을 검색해 채널 ID 목록을 반환한다."""
    data = yt_get(
        "search",
        api_key,
        {
            "part": "snippet",
            "q": keyword,
            "type": "channel",
            "maxResults": min(max_results, 50),
            "relevanceLanguage": "ko",
        },
    )
    return [item["snippet"]["channelId"] for item in data.get("items", [])]


# ── 2단계: 채널 ID → 상세 정보 ────────────────────────────
def fetch_channel_details(api_key: str, channel_ids: list[str]) -> list[dict]:
    """채널 ID 목록을 받아 상세 정보를 반환한다 (50개씩 배치 처리)."""
    results = []
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i : i + 50]
        data = yt_get(
            "channels",
            api_key,
            {
                "part": "snippet,statistics,brandingSettings",
                "id": ",".join(chunk),
            },
        )
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            branding = item.get("brandingSettings", {}).get("channel", {})

            subscribers = int(stats.get("subscriberCount", 0))
            view_count = int(stats.get("viewCount", 0))
            video_count = int(stats.get("videoCount", 0))

            thumbnails = snippet.get("thumbnails", {})
            profile_image = (
                thumbnails.get("high", {}).get("url")
                or thumbnails.get("medium", {}).get("url")
                or thumbnails.get("default", {}).get("url")
                or ""
            )

            channel_id = item["id"]
            custom_url = snippet.get("customUrl", "")

            results.append(
                {
                    "id": f"yt_{channel_id}",
                    "platform": "youtube",
                    "channel_id": channel_id,
                    "handle": custom_url or channel_id,
                    "name": snippet.get("title", ""),
                    "description": snippet.get("description", "")[:200],
                    "profile_url": f"https://www.youtube.com/channel/{channel_id}",
                    "profile_image": profile_image,
                    "country": snippet.get("country", ""),
                    "language": snippet.get("defaultLanguage", ""),
                    "keywords": branding.get("keywords", ""),
                    "followers": subscribers,
                    "view_count": view_count,
                    "video_count": video_count,
                    "avg_views_per_video": (
                        round(view_count / video_count) if video_count > 0 else 0
                    ),
                    "engagement_rate": None,  # YouTube 공식 API 미제공
                    "tier": get_tier(subscribers),
                    "category": ["skincare", "beauty"],
                    "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                }
            )
    return results


# ── 3단계: 필터 적용 ──────────────────────────────────────
def apply_filters(channels: list[dict], filters: dict) -> list[dict]:
    min_sub = filters.get("min_followers", 1_000)
    min_videos = filters.get("min_videos", 10)
    allowed = [c.upper() for c in filters.get("allowed_countries", [])]

    return [
        ch
        for ch in channels
        if ch["followers"] >= min_sub
        and ch["video_count"] >= min_videos
        and (not allowed or ch.get("country", "").upper() in allowed)
    ]


# ── 메인 ──────────────────────────────────────────────────
def main() -> None:
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        print("❌ YOUTUBE_API_KEY 환경변수가 설정되지 않았습니다.", file=sys.stderr)
        sys.exit(1)

    # config 로드
    print(f"📂 config.json 로드: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    yt_config = config.get("youtube", {})
    keywords: list[str] = yt_config.get("keywords", [])
    max_results: int = yt_config.get("max_results_per_keyword", 20)
    filters: dict = config.get("filters", {})

    # 키워드별 채널 ID 수집 (중복 제거)
    all_channel_ids: set[str] = set()
    for kw in keywords:
        print(f"🔍 검색 중: '{kw}'")
        ids = search_channel_ids(api_key, kw, max_results)
        all_channel_ids.update(ids)
        time.sleep(0.3)  # API 과부하 방지

    print(f"\n📊 총 발굴된 채널 (중복 제거): {len(all_channel_ids)}개")

    # 채널 상세 정보 fetch
    print("🎬 채널 상세 정보 조회 중...")
    channels = fetch_channel_details(api_key, list(all_channel_ids))

    # 필터 적용
    filtered = apply_filters(channels, filters)
    print(
        f"✅ 필터 통과: {len(filtered)}개 (구독자 {filters.get('min_followers', 0):,})"
    )

    # 구독자 수 내림차순 정렬
    filtered.sort(key=lambda x: x["followers"], reverse=True)

    # 저장
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
