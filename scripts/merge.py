"""
merge.py

역할:
  data/youtube.json + data/instagram.json 을 읽어
  data/influencers.json 으로 병합한다.

  fetch_youtube.py, fetch_instagram.py 실행 후 마지막으로 실행한다.

실행:
  uv run python scripts/merge.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent

SOURCES = [
    ROOT / "data" / "youtube.json",
    ROOT / "data" / "instagram.json",
    ROOT / "data" / "tiktok.json",
]
OUTPUT_PATH = ROOT / "data" / "influencers.json"


def main() -> None:
    all_influencers: list[dict] = []
    seen_ids: set[str] = set()

    for path in SOURCES:
        if not path.exists():
            print(f"⚠️  {path.name} 없음, 스킵")
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("influencers", [])
        before = len(all_influencers)
        for item in items:
            if item["id"] not in seen_ids:
                seen_ids.add(item["id"])
                all_influencers.append(item)
        print(f"✅ {path.name}: {len(all_influencers) - before}개 추가")

    # 구독자/팔로워 내림차순 정렬
    all_influencers.sort(key=lambda x: x["followers"], reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "count": len(all_influencers),
                "influencers": all_influencers,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"\n💾 병합 완료: {OUTPUT_PATH}  (총 {len(all_influencers)}명)")


if __name__ == "__main__":
    main()
