"""
utils.py

공통 유틸리티 모듈.
언어/국가 감지 로직을 한 곳에서 관리한다.
"""

import re

# ── 한국어 관련 패턴 ───────────────────────────────────────
_KR_HASHTAGS = re.compile(
    r"#(스킨케어|피부|화장품|뷰티|피부관리|데일리|글로우|수분|세럼|토너|선크림|메이크업|클렌징|에센스|앰플|크림|마스크팩|기초케어|올리브영)"
)
_KR_HANDLE = re.compile(r"([\._]kr$|korea|korean|한국)", re.IGNORECASE)
_KR_WORDS = re.compile(r"(안녕|피부|뷰티|스킨|리뷰|추천|일상|데일리)")

# ── 일본어 관련 패턴 ───────────────────────────────────────
_JP_HASHTAGS = re.compile(
    r"#(スキンケア|美容|コスメ|美肌|保湿|クレンジング|化粧水|美白|日焼け止め|メイク)"
)
_JP_HANDLE = re.compile(r"([\._]jp$|japan|japanese)", re.IGNORECASE)
_JP_WORDS = re.compile(r"(こんにちは|おすすめ|美容|スキン|コスメ)")


def detect_language(text: str, handle: str = "") -> str:
    """
    텍스트(bio, 이름 등)와 핸들로 국가 코드를 추정한다.

    감지 우선순위:
      1. 한글/일본어 유니코드 문자 수
      2. 언어별 해시태그 패턴
      3. 핸들 패턴 (_kr, _jp 등)
      4. 자주 쓰이는 단어 패턴

    Returns:
      "KR" | "JP" | ""
    """
    combined = text + " " + handle

    # 1. 유니코드 문자 카운트
    ko = jp = 0
    for ch in combined:
        cp = ord(ch)
        if 0xAC00 <= cp <= 0xD7A3 or 0x1100 <= cp <= 0x11FF:
            ko += 1
        elif 0x3040 <= cp <= 0x30FF or 0x4E00 <= cp <= 0x9FFF:
            jp += 1

    if ko >= 2 and ko > jp:
        return "KR"
    if jp >= 2 and jp > ko:
        return "JP"

    # 2. 해시태그 패턴
    if _KR_HASHTAGS.search(combined):
        return "KR"
    if _JP_HASHTAGS.search(combined):
        return "JP"

    # 3. 핸들 패턴
    if _KR_HANDLE.search(handle):
        return "KR"
    if _JP_HANDLE.search(handle):
        return "JP"

    # 4. 자주 쓰이는 단어
    if _KR_WORDS.search(combined):
        return "KR"
    if _JP_WORDS.search(combined):
        return "JP"

    return ""
