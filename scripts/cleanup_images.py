"""
cleanup_images.py

역할:
  data/images/ 에 있는 이미지 중 instagram.json 에 없는
  댕글링 파일을 찾아 삭제한다.

실행:
  uv run python scripts/cleanup_images.py

  # 드라이런 (실제 삭제 없이 목록만 확인)
  DRY_RUN=1 uv run python scripts/cleanup_images.py
"""

import json
import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
IMAGES_DIR = ROOT / "data" / "images"
IG_PATH = ROOT / "data" / "instagram.json"
TT_PATH = ROOT / "data" / "tiktok.json"

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"


def main() -> None:
    if not IMAGES_DIR.exists():
        print("⚠️  data/images/ 디렉토리가 없습니다.")
        return

    # instagram.json + tiktok.json에서 유효한 로컬 이미지 경로 수집
    valid_files: set[str] = set()
    for json_path in [IG_PATH, TT_PATH]:
        if json_path.exists():
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)
            for acc in data.get("influencers", []):
                img = acc.get("profile_image", "")
                if img:
                    valid_files.add(Path(img).name)

    # data/images/ 의 실제 파일 목록 (ig_ + tt_ 모두)
    all_files = list(IMAGES_DIR.glob("ig_*.jpg")) + list(IMAGES_DIR.glob("tt_*.jpg"))

    dangling = [f for f in all_files if f.name not in valid_files]
    kept = len(all_files) - len(dangling)

    print(f"📂 전체 이미지: {len(all_files)}개")
    print(f"✅ 유효 이미지: {kept}개")
    print(f"🗑️  댕글링 이미지: {len(dangling)}개")

    if not dangling:
        print("\n정리할 파일이 없어요!")
        return

    print("\n삭제 목록:")
    for f in sorted(dangling):
        print(f"  - {f.name}")

    if DRY_RUN:
        print("\n⏭️  DRY_RUN 모드: 실제 삭제 안 함")
        return

    for f in dangling:
        f.unlink()
    print(f"\n💾 {len(dangling)}개 삭제 완료")


if __name__ == "__main__":
    main()
