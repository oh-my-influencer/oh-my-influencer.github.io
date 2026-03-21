"""
filter_existing.py

역할:
  기존에 수집된 instagram.json / tiktok.json 에
  국가 필터(allowed_countries)를 소급 적용한다.

  - YouTube는 country 코드가 명확하므로 소급 불필요
  - Instagram: bio + 이름으로 언어 감지 → country 필드 채움
  - TikTok   : region 필드 우선, 없으면 언어 감지 → country 필드 채움
  - 필터 통과 못한 항목은 제거하고 파일을 덮어씀
  - 이후 merge.py 를 다시 실행하면 influencers.json 에 반영됨

실행:
  uv run python scripts/filter_existing.py

  # 드라이런 (실제 파일 수정 없이 결과만 미리 확인)
  DRY_RUN=1 uv run python scripts/filter_existing.py
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "data" / "config.json"

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"


import sys as _sys

_sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.utils import detect_language


# ── country 필드 채우기 ────────────────────────────────────
def fill_country_instagram(acc: dict) -> dict:
    """country 필드가 없거나 비어있으면 언어 감지로 채운다."""
    if acc.get("country"):
        return acc
    bio = acc.get("bio", "")
    name = acc.get("name", "")
    acc["country"] = detect_language(bio + " " + name, handle=acc.get("handle", ""))
    return acc


def fill_country_tiktok(acc: dict) -> dict:
    """country 필드가 없거나 비어있으면 언어 감지로 채운다."""
    if acc.get("country"):
        return acc
    name = acc.get("name", "")
    acc["country"] = detect_language(name, handle=acc.get("handle", ""))
    return acc


# ── 파일 처리 ─────────────────────────────────────────────
def process_file(path: Path, fill_fn, allowed: list[str]) -> None:
    if not path.exists():
        print(f"⚠️  {path.name} 없음, 스킵")
        return

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    original = data.get("influencers", [])

    # country 필드 채우기
    filled = [fill_fn(acc) for acc in original]

    # 필터 적용
    if allowed:
        filtered = [acc for acc in filled if acc.get("country", "") in allowed]
    else:
        filtered = filled

    removed = len(original) - len(filtered)

    print(f"\n📂 {path.name}")
    print(f"   전체: {len(original)}개")
    print(f"   통과: {len(filtered)}개")
    print(f"   제거: {removed}개")

    # 제거된 항목 미리보기 (최대 10개)
    removed_items = [acc for acc in filled if acc not in filtered]
    if removed_items:
        print("   제거 목록 (상위 10개):")
        for acc in removed_items[:10]:
            print(
                f"     - @{acc['handle']} | country: '{acc.get('country', '')}' | 팔로워: {acc['followers']:,}"
            )
        if len(removed_items) > 10:
            print(f"     ... 외 {len(removed_items) - 10}개")

    if DRY_RUN:
        print("   ⏭️  DRY_RUN 모드: 파일 수정 안 함")
        return

    data["influencers"] = filtered
    data["count"] = len(filtered)
    data["generated_at"] = datetime.now(timezone.utc).isoformat()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("   💾 저장 완료")


# ── 메인 ──────────────────────────────────────────────────
def main() -> None:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    allowed = [
        c.upper() for c in config.get("filters", {}).get("allowed_countries", [])
    ]

    if not allowed:
        print("ℹ️  allowed_countries 가 비어있어 필터 없이 country 필드만 채웁니다.")
    else:
        print(f"🌏 허용 국가: {', '.join(allowed)}")

    if DRY_RUN:
        print("🔍 DRY_RUN 모드: 실제 파일은 수정되지 않습니다.\n")

    process_file(ROOT / "data" / "instagram.json", fill_country_instagram, allowed)
    process_file(ROOT / "data" / "tiktok.json", fill_country_tiktok, allowed)

    if not DRY_RUN:
        print("\n✅ 완료! 이제 merge.py 를 실행해 influencers.json 을 갱신하세요.")
        print("   uv run python scripts/merge.py")


if __name__ == "__main__":
    main()
