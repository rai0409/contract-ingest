from __future__ import annotations

import re
import unicodedata

_FULLWIDTH_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")
_WHITESPACE_RE = re.compile(r"[\s\u3000]+")
_REPLACEMENT_CHAR = "\uFFFD"

_KANJI_DIGITS = {
    "〇": 0,
    "零": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}

_ARTICLE_HEADING_RE = re.compile(r"^\s*第[0-9０-９一二三四五六七八九十百千〇零]+\s*条")
_ANNOTATION_BRACKET_RE = re.compile(r"^\s*\[[^\]]{1,20}\]\s*$")
_PLACEHOLDER_DATE_RE = re.compile(r"^\s*[0０]{2,4}年\s*[0０]{1,2}月\s*[0０]{1,2}日\s*$")
_PLACEHOLDER_LABEL_RE = re.compile(r"^\s*[（(]\s*(住所|代表者名)\s*[）)]\s*$")
_FRAGMENT_TOKEN_RE = re.compile(r"^[\[\]【】()（）0-9０-９①-⑳]+$")
_PAGE_NUMBER_RE = re.compile(
    r"^\s*(?:page\s*)?[0-9０-９]{1,4}\s*(?:/\s*[0-9０-９]{1,4})?\s*(?:ページ|頁)?\s*$",
    re.IGNORECASE,
)
_ROMAN_PAGE_NUMBER_RE = re.compile(r"^\s*[IVXLCDM]{1,8}\s*$")



def normalize_digits(text: str) -> str:
    return text.translate(_FULLWIDTH_DIGITS)



def strip_control_chars(text: str) -> str:
    return _CONTROL_CHAR_RE.sub("", text)



def normalize_whitespace(text: str) -> str:
    cleaned = strip_control_chars(text)
    return _WHITESPACE_RE.sub(" ", cleaned).strip()



def normalize_text(text: str) -> str:
    return normalize_whitespace(unicodedata.normalize("NFKC", text))



def garbled_ratio(text: str) -> float:
    if not text:
        return 0.0
    replacement_count = text.count(_REPLACEMENT_CHAR)
    control_count = len(_CONTROL_CHAR_RE.findall(text))
    weird_count = len(re.findall(r"[\u0000-\u001F\u007F]", text))
    score = replacement_count + control_count + weird_count
    return min(1.0, score / max(len(text), 1))



def is_noise_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return True
    if re.fullmatch(r"[0-9]+", normalized):
        return True
    if len(normalized) <= 2 and re.fullmatch(r"[\W_]+", normalized):
        return True
    if re.fullmatch(r"[-_=]{3,}", normalized):
        return True
    return False



def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def is_article_heading_text(text: str) -> bool:
    normalized = normalize_text(text)
    return bool(_ARTICLE_HEADING_RE.match(normalized))


def is_page_number_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    if is_article_heading_text(normalized):
        return False
    return bool(_PAGE_NUMBER_RE.fullmatch(normalized) or _ROMAN_PAGE_NUMBER_RE.fullmatch(normalized))


def is_annotation_like_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    if is_article_heading_text(normalized):
        return False
    if _ANNOTATION_BRACKET_RE.fullmatch(normalized):
        return True
    if _PLACEHOLDER_DATE_RE.fullmatch(normalized):
        return True
    if _PLACEHOLDER_LABEL_RE.fullmatch(normalized):
        return True
    if normalized.startswith("[解説") or normalized.startswith("【解説"):
        return True
    strong_markers = ("コメントの追加", "コメント", "解説編", "解説", "ひな形", "記入例", "オプション条項")
    if any(marker in normalized for marker in strong_markers):
        return True
    short_markers = ("適宜", "参照")
    return len(normalized) <= 20 and any(marker in normalized for marker in short_markers)


def is_fragment_like_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return True
    if normalized in {"甲", "乙"}:
        return False
    if is_article_heading_text(normalized):
        return False
    if len(normalized) <= 2:
        return True
    if _FRAGMENT_TOKEN_RE.fullmatch(normalized):
        return True
    return False



def parse_article_number(clause_no: str | None) -> int | None:
    if clause_no is None:
        return None
    match = re.search(r"第([0-9０-９一二三四五六七八九十百千〇零]+)条", clause_no)
    if not match:
        return None
    token = normalize_digits(match.group(1))
    if token.isdigit():
        return int(token)
    return _kanji_number_to_int(token)



def _kanji_number_to_int(token: str) -> int | None:
    if not token:
        return None

    total = 0
    current = 0

    for ch in token:
        if ch in _KANJI_DIGITS:
            current = _KANJI_DIGITS[ch]
            continue
        if ch == "十":
            current = max(current, 1)
            total += current * 10
            current = 0
            continue
        if ch == "百":
            current = max(current, 1)
            total += current * 100
            current = 0
            continue
        if ch == "千":
            current = max(current, 1)
            total += current * 1000
            current = 0
            continue
        return None

    return total + current
