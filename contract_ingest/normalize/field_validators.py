from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re

from contract_ingest.utils.text import normalize_digits, normalize_text, unique_preserve_order

_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DATE_TOKEN_RE = re.compile(r"(?:令和\s*[0-9０-９元○◯]+年|[0-9０-９]{4}\s*年|[0-9０-９]{4}\s*[/.]\s*[0-9０-９]{1,2})")
_PLACEHOLDER_TERM_RE = re.compile(
    r"(?:令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日|○○年|○○月|○○日|年\s*月\s*日)"
)
_RELATIVE_EFFECTIVE_TERM_RE = re.compile(
    r"(?:本契約締結日|契約締結日|契約締結の日|締結日|発効日|効力発生日)[^。\n]{0,24}(?:から|より)[^。\n]{0,24}(?:日|週|か月|ヶ月|ヵ月|年)間"
)
_RELATIVE_EXPIRATION_TERM_RE = re.compile(
    r"(?:契約締結日|契約締結の日|締結日|発効日|効力発生日)[^。\n]{0,24}(?:から|より)[^。\n]{0,24}(?:日|週|か月|ヶ月|ヵ月|年)間"
)
_RENEWABLE_TERM_RE = re.compile(r"(?:自動更新|同一条件で更新|更新するものとする|更新される|更新拒絶)")
_RELATIVE_JURISDICTION_RE = re.compile(
    r"(?:(?:甲|乙|被告|原告|相手方|当事者)\s*の\s*)?(?:所在地|住所|本店所在地|本店所在地等|本店)\s*を\s*管轄する裁判所"
)
_ENGLISH_EFFECTIVE_ANCHOR_ONLY_RE = re.compile(
    r"(?:^|[^a-z])(?:(?:on|as of|upon)\s+)?(?:the\s+)?(?:execution date|date of execution)(?:$|[^a-z])",
    re.IGNORECASE,
)
_ENGLISH_MONTH_DATE_RE = re.compile(
    r"^(?P<month>Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|"
    r"Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})$",
    re.IGNORECASE,
)
_ENGLISH_DAY_MONTH_DATE_RE = re.compile(
    r"^(?P<day>\d{1,2})\s+(?P<month>Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|"
    r"Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+(?P<year>\d{4})$",
    re.IGNORECASE,
)
_ENGLISH_GOVERNING_LAW_CONTEXT_RE = re.compile(
    r"(?:governed by|construed in accordance with|in accordance with|under the laws? of)",
    re.IGNORECASE,
)
_ENGLISH_GOVERNING_LAW_OF_RE = re.compile(
    r"(?:(?:governed by(?: and construed in accordance with)?)|(?:construed in accordance with)|"
    r"(?:in accordance with)|(?:under))\s+(?P<law>(?:the\s+)?(?:internal\s+|substantive\s+)?laws? of\s+[A-Za-z][A-Za-z&,' .-]{1,80})",
    re.IGNORECASE,
)
_ENGLISH_GOVERNING_BARE_LAW_RE = re.compile(
    r"(?:governed by|construed in accordance with)\s+(?P<law>[A-Za-z][A-Za-z&,' .-]{1,80}\s+law)",
    re.IGNORECASE,
)
_ZERO_LENGTH_EFFECTIVE_PERIOD_RE = re.compile(
    r"(?:本契約締結日|契約締結日|契約締結の日|締結日|発効日|効力発生日)[^。\n]{0,24}(?:から|より)\s*[0０OoＯ]+\s*"
    r"(?:日間?|週間?|か月|ヶ月|ヵ月|年(?:間)?)"
)
_JAPANESE_GOVERNING_LAW_WEAK_CONTEXT_RE = re.compile(
    r"(?:規程による|誠実に協議|別途協議|法令遵守|適用法令)"
)
_ENTITY_TYPE_ONLY_COUNTERPARTY_VALUES = {
    "株式会社",
    "合同会社",
    "有限会社",
    "公益財団法人",
    "一般社団法人",
    "国立大学法人",
    "学校法人",
}
_PLACEHOLDER_COUNTERPARTY_RE = re.compile(r"^[□■◻◼○◯〇]+$")


@dataclass(frozen=True)
class FieldValidationResult:
    accepted: bool
    normalized_value: str | bool | list[str] | None
    reason: str
    confidence: float | None
    raw_value: str | bool | list[str] | None = None
    quality_flags: list[str] = field(default_factory=list)
    anchor_only: bool = False


def validate_jurisdiction(
    value: str | bool | list[str] | None,
    *,
    reason: str | None = None,
    confidence: float | None = None,
) -> FieldValidationResult:
    if not isinstance(value, str):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="jurisdiction_not_string",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction"],
        )

    text = normalize_text(value)
    if not text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="empty_jurisdiction",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction"],
        )

    generic_values = {
        "裁判所",
        "地方裁判所",
        "簡易裁判所",
        "家庭裁判所",
        "高等裁判所",
        "管轄裁判所",
        "合意管轄裁判所",
        "専属的合意管轄裁判所",
    }
    if text in generic_values:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="generic_jurisdiction_value",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction"],
        )

    if "番の専属的合意管轄裁判所" in text or re.search(r"^\s*[0-9０-９]+番", text):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="broken_jurisdiction_fragment",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction", "fragment_like"],
        )

    if _RELATIVE_JURISDICTION_RE.search(text):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="relative_jurisdiction_expression",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction", "relative_jurisdiction_expression"],
        )

    court_match = re.search(r"([^\s、。]{1,18}(?:地方|簡易|家庭|高等)?裁判所)", text)
    if not court_match:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="court_name_not_found",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction"],
        )

    court_name = normalize_text(court_match.group(1))
    if court_name in generic_values or len(court_name) <= 3:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="court_name_too_generic",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_jurisdiction"],
        )

    base = confidence if confidence is not None else 0.82
    if "専属" in text and "合意管轄" in text:
        base += 0.06
    return FieldValidationResult(
        accepted=True,
        normalized_value=court_name,
        reason="validated_jurisdiction",
        confidence=max(0.0, min(0.99, base)),
        raw_value=value,
        quality_flags=[],
    )


def validate_governing_law(
    value: str | bool | list[str] | None,
    *,
    reason: str | None = None,
    confidence: float | None = None,
) -> FieldValidationResult:
    if not isinstance(value, str):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="governing_law_not_string",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law"],
        )

    text = normalize_text(value)
    if not text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="empty_governing_law",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law"],
        )

    if text in {"法", "準拠", "準拠法", "本契約", "契約"}:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="generic_governing_law_value",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law"],
        )

    if _JAPANESE_GOVERNING_LAW_WEAK_CONTEXT_RE.search(text):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="ambiguous_governing_law_statement",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law", "ambiguous"],
        )

    if "協議" in text and "日本法" not in text and "日本国法" not in text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="ambiguous_governing_law_statement",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law", "ambiguous"],
        )
    if re.search(r"(?:研究開発法|開発法)$", text) and "日本法" not in text and "日本国法" not in text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="organization_like_governing_law_token",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law", "ambiguous"],
        )

    if "日本法" in text or "日本国法" in text:
        normalized = "日本法"
        base = confidence if confidence is not None else 0.88
        return FieldValidationResult(
            accepted=True,
            normalized_value=normalized,
            reason="validated_governing_law",
            confidence=max(0.0, min(0.99, base)),
            raw_value=value,
        )

    if (
        re.search(r"[A-Za-z]{2,}", text)
        and re.search(r"[ぁ-んァ-ヶ一-龥]", text)
        and not _ENGLISH_GOVERNING_LAW_CONTEXT_RE.search(text)
    ):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="mixed_language_non_governing_context",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law", "ambiguous"],
        )

    english_normalized = _normalize_english_governing_law(text, reason=reason)
    if english_normalized is not None:
        base = confidence if confidence is not None else 0.84
        return FieldValidationResult(
            accepted=True,
            normalized_value=english_normalized,
            reason="validated_governing_law",
            confidence=max(0.0, min(0.99, base)),
            raw_value=value,
        )

    match = re.search(r"([^\s、。]{2,10}法)", text)
    if not match:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="governing_law_token_not_found",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law"],
        )

    normalized = normalize_text(match.group(1))
    if normalized in {"法", "本法", "準拠法"}:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="governing_law_token_too_generic",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_governing_law"],
        )

    base = confidence if confidence is not None else 0.74
    if "準拠" in text:
        base += 0.07
    return FieldValidationResult(
        accepted=True,
        normalized_value=normalized,
        reason="validated_governing_law",
        confidence=max(0.0, min(0.99, base)),
        raw_value=value,
    )


def validate_effective_date(
    value: str | bool | list[str] | None,
    *,
    reason: str | None = None,
    confidence: float | None = None,
) -> FieldValidationResult:
    if not isinstance(value, str):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="effective_date_not_string",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_effective_date"],
        )

    text = normalize_text(value)
    if not text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="empty_effective_date",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_effective_date"],
        )

    if _ZERO_LENGTH_EFFECTIVE_PERIOD_RE.search(text):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="zero_length_effective_period",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_effective_date", "ocr_numeric_anomaly"],
        )

    semantic_type = classify_effective_date_semantics(text)
    normalized_absolute = _normalize_english_explicit_date(text)

    if semantic_type == "absolute" and normalized_absolute is not None:
        base = confidence if confidence is not None else 0.92
        return FieldValidationResult(
            accepted=True,
            normalized_value=normalized_absolute,
            reason="validated_effective_date_absolute",
            confidence=max(0.0, min(0.99, base)),
            raw_value=value,
            quality_flags=["absolute_date", _semantic_flag("absolute")],
            anchor_only=False,
        )

    if semantic_type == "anchor_only":
        base = confidence if confidence is not None else 0.72
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_effective_date_anchor_only",
            confidence=min(0.72, max(0.0, base)),
            raw_value=value,
            quality_flags=["anchor_only_effective_date", _semantic_flag("anchor_only")],
            anchor_only=True,
        )

    if semantic_type == "relative_term":
        base = confidence if confidence is not None else 0.66
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_effective_date_relative",
            confidence=min(0.68, max(0.0, base)),
            raw_value=value,
            quality_flags=["relative_period_only", _semantic_flag("relative_term")],
        )

    if semantic_type == "placeholder_term":
        base = confidence if confidence is not None else 0.60
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_effective_date_placeholder",
            confidence=min(0.62, max(0.0, base)),
            raw_value=value,
            quality_flags=["text_date", "placeholder_date", _semantic_flag("placeholder_term")],
        )

    if semantic_type == "renewable_term":
        base = confidence if confidence is not None else 0.60
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_effective_date_renewable",
            confidence=min(0.62, max(0.0, base)),
            raw_value=value,
            quality_flags=["text_date", "renewable_term", _semantic_flag("renewable_term")],
        )

    if _DATE_TOKEN_RE.search(text):
        base = confidence if confidence is not None else 0.78
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_effective_date_text",
            confidence=max(0.0, min(0.99, base)),
            raw_value=value,
            quality_flags=["text_date", _semantic_flag("absolute")],
        )

    return FieldValidationResult(
        accepted=False,
        normalized_value=None,
        reason="unrecognized_effective_date_value",
        confidence=None,
        raw_value=value,
        quality_flags=["low_quality_effective_date"],
    )


def validate_expiration_date(
    value: str | bool | list[str] | None,
    *,
    reason: str | None = None,
    confidence: float | None = None,
) -> FieldValidationResult:
    if not isinstance(value, str):
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="expiration_date_not_string",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_expiration_date"],
        )

    text = normalize_text(value)
    if not text:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="empty_expiration_date",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_expiration_date"],
        )

    semantic_type = classify_expiration_date_semantics(text)
    normalized_digits = normalize_digits(text)
    if semantic_type == "absolute" and _ISO_DATE_RE.fullmatch(normalized_digits):
        base = confidence if confidence is not None else 0.90
        return FieldValidationResult(
            accepted=True,
            normalized_value=normalized_digits,
            reason="validated_expiration_date_absolute",
            confidence=max(0.0, min(0.99, base)),
            raw_value=value,
            quality_flags=["absolute_date", _semantic_flag("absolute")],
        )

    if semantic_type == "relative_term":
        base = confidence if confidence is not None else 0.64
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_expiration_date_relative",
            confidence=min(0.66, max(0.0, base)),
            raw_value=value,
            quality_flags=["relative_period_only", _semantic_flag("relative_term")],
        )

    if semantic_type == "placeholder_term":
        base = confidence if confidence is not None else 0.60
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_expiration_date_placeholder",
            confidence=min(0.62, max(0.0, base)),
            raw_value=value,
            quality_flags=["text_date", "placeholder_date", _semantic_flag("placeholder_term")],
        )

    if semantic_type == "renewable_term":
        base = confidence if confidence is not None else 0.62
        return FieldValidationResult(
            accepted=True,
            normalized_value=text,
            reason="validated_expiration_date_renewable",
            confidence=min(0.64, max(0.0, base)),
            raw_value=value,
            quality_flags=["text_date", "renewable_term", _semantic_flag("renewable_term")],
        )

    if text in {"満了日", "終了日", "まで", "満了"} or len(text) <= 4:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="expiration_date_fragment",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_expiration_date", "fragment_like"],
        )

    base = confidence if confidence is not None else 0.70
    return FieldValidationResult(
        accepted=True,
        normalized_value=text,
        reason="validated_expiration_date_text",
        confidence=max(0.0, min(0.99, base)),
        raw_value=value,
        quality_flags=["text_date", _semantic_flag("relative_term")],
    )


def validate_counterparties(
    value: str | bool | list[str] | None,
    *,
    reason: str | None = None,
    confidence: float | None = None,
) -> FieldValidationResult:
    if value is None:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="counterparties_empty",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_counterparty"],
        )

    raw_candidates = value if isinstance(value, list) else [str(value)]
    accepted: list[str] = []
    placeholder_candidates: list[str] = []
    rejected_candidates: list[str] = []
    quality_flags: list[str] = []
    allow_placeholder = bool(reason and ("matched_party_role" in reason or "finder_counterparty" in reason))

    for raw in raw_candidates:
        candidate = _normalize_counterparty_candidate(raw)
        if not candidate:
            continue
        if _is_placeholder_counterparty(candidate):
            if allow_placeholder:
                placeholder_candidates.append(candidate)
            else:
                rejected_candidates.append(candidate)
            continue
        if _is_bad_counterparty_fragment(candidate):
            salvaged = _extract_company_tail(candidate)
            if salvaged and _is_valid_counterparty_entity(salvaged):
                accepted.append(salvaged)
                quality_flags.append("counterparty_partial_accept")
            else:
                rejected_candidates.append(candidate)
            continue
        if _is_valid_counterparty_entity(candidate):
            accepted.append(candidate)
            continue
        rejected_candidates.append(candidate)

    if accepted and placeholder_candidates:
        accepted.extend(placeholder_candidates[: max(0, 2 - len(accepted))])
        quality_flags.append("counterparty_placeholder")

    normalized = unique_preserve_order(accepted)
    if not normalized:
        return FieldValidationResult(
            accepted=False,
            normalized_value=None,
            reason="counterparty_candidates_rejected",
            confidence=None,
            raw_value=value,
            quality_flags=["low_quality_counterparty", "fragment_like"],
        )

    base = confidence if confidence is not None else (0.88 if len(normalized) >= 2 else 0.72)
    if rejected_candidates or len(normalized) < len(raw_candidates):
        quality_flags.append("counterparty_partial_accept")
        base = min(base, 0.70)
        result_reason = "validated_counterparties_partial"
    else:
        result_reason = "validated_counterparties"

    return FieldValidationResult(
        accepted=True,
        normalized_value=normalized,
        reason=result_reason,
        confidence=max(0.0, min(0.99, base)),
        raw_value=value,
        quality_flags=unique_preserve_order(quality_flags),
    )


def _normalize_counterparty_candidate(value: str) -> str:
    text = normalize_text(value)
    text = re.sub(r"(?:以下\s*[「『]?[甲乙].*)$", "", text)
    text = re.sub(r"(?:という。?\)?\s*(?:と|と、)?\s*)$", "", text)
    text = re.sub(r"^[,:：;；、，]+", "", text)
    text = text.strip(" 　,:：;；、，。()（）[]【】『』「」")
    text = text.rstrip("と、のはにを")
    return text


def _is_bad_counterparty_fragment(text: str) -> bool:
    if not text:
        return True
    if any(token in text for token in ["という。", "という。)", "ひな形", "コメント", "参照"]):
        return True
    if text in {"以下甲", "以下乙", "甲", "乙", "委託者", "受託者"}:
        return True
    if text.endswith(("。", "、", "，", "；", ":", "：")) and len(text) <= 16:
        return True
    if text.endswith(("と", "と、", "及び", "または", "又は", "の")):
        return True
    if text.startswith(("という", "及び", "と、", "と")):
        return True
    if text in {"株式会社", "合同会社", "有限会社", "公立大学法人", "国立大学法人", "独立行政法人"}:
        return True
    if text.count("(") != text.count(")") or text.count("（") != text.count("）"):
        return True
    symbol_count = sum(1 for ch in text if ch in "[]{}<>|/\\*#@")
    if len(text) <= 16 and symbol_count >= 2:
        return True
    if len(text) <= 4 and "会社" not in text and "法人" not in text:
        return True
    return False


def _is_valid_counterparty_entity(text: str) -> bool:
    if not text:
        return False
    if len(text) < 2 or len(text) > 80:
        return False
    if re.fullmatch(r"[0-9]+", normalize_digits(text)):
        return False
    if text in {"甲", "乙", "当事者", "本契約"}:
        return False
    if _is_entity_type_only_counterparty(text):
        return False
    if text.endswith(("という", "という。", "と", "の", "は")):
        return False
    entity_markers = (
        "株式会社",
        "合同会社",
        "有限会社",
        "法人",
        "大学",
        "研究所",
        "銀行",
        "組合",
        "協会",
        "機構",
        "センター",
    )
    if any(marker in text for marker in entity_markers):
        return True
    kanji_kana = sum(1 for ch in text if ("\u3040" <= ch <= "\u30FF") or ("\u4E00" <= ch <= "\u9FFF"))
    return len(text) >= 4 and kanji_kana >= 3 and not _is_bad_counterparty_fragment(text)


def _extract_company_tail(text: str) -> str | None:
    tail_pattern = re.compile(
        r"([○◯A-Za-z0-9ぁ-んァ-ヶ一-龥・\-\s]{1,40}(?:株式会社|合同会社|有限会社|法人|大学|研究所))$"
    )
    match = tail_pattern.search(text)
    if not match:
        return None
    candidate = _normalize_counterparty_candidate(match.group(1))
    if _is_entity_type_only_counterparty(candidate):
        return None
    return candidate


def classify_effective_date_semantics(text: str) -> str:
    normalized = normalize_text(text)
    normalized_digits = normalize_digits(normalized)
    if _ISO_DATE_RE.fullmatch(normalized_digits):
        return "absolute"
    if _normalize_english_explicit_date(normalized) is not None:
        return "absolute"
    if re.search(r"(?:本契約締結日|契約締結日|締結日)\s*(?:から|より)$", normalized):
        return "anchor_only"
    if _ENGLISH_EFFECTIVE_ANCHOR_ONLY_RE.search(normalized):
        return "anchor_only"
    if _RELATIVE_EFFECTIVE_TERM_RE.search(normalized):
        return "relative_term"
    if _PLACEHOLDER_TERM_RE.search(normalized):
        return "placeholder_term"
    if _RENEWABLE_TERM_RE.search(normalized):
        return "renewable_term"
    if _DATE_TOKEN_RE.search(normalized):
        return "absolute"
    return "relative_term"


def classify_expiration_date_semantics(text: str) -> str:
    normalized = normalize_text(text)
    normalized_digits = normalize_digits(normalized)
    if _ISO_DATE_RE.fullmatch(normalized_digits):
        return "absolute"
    if _RENEWABLE_TERM_RE.search(normalized):
        return "renewable_term"
    if _PLACEHOLDER_TERM_RE.search(normalized):
        return "placeholder_term"
    if _RELATIVE_EXPIRATION_TERM_RE.search(normalized) or "有効期間" in normalized or "契約期間" in normalized:
        return "relative_term"
    if _DATE_TOKEN_RE.search(normalized):
        return "absolute"
    return "relative_term"


def _semantic_flag(semantic_type: str) -> str:
    return f"semantic_type:{semantic_type}"


def _normalize_english_explicit_date(text: str) -> str | None:
    normalized = normalize_digits(normalize_text(text))
    if _ISO_DATE_RE.fullmatch(normalized):
        return normalized

    month_map = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    month_first = _ENGLISH_MONTH_DATE_RE.fullmatch(normalized)
    if month_first is not None:
        month = month_map[month_first.group("month").lower()]
        day = int(month_first.group("day"))
        year = int(month_first.group("year"))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    day_first = _ENGLISH_DAY_MONTH_DATE_RE.fullmatch(normalized)
    if day_first is not None:
        month = month_map[day_first.group("month").lower()]
        day = int(day_first.group("day"))
        year = int(day_first.group("year"))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    return None


def _normalize_english_governing_law(text: str, *, reason: str | None = None) -> str | None:
    normalized = normalize_text(text)
    if not re.search(r"[A-Za-z]", normalized):
        return None

    direct_law_name = _normalize_english_law_name(normalized)
    if direct_law_name is not None and _reason_indicates_governing_law_clause(reason):
        return direct_law_name

    if not _ENGLISH_GOVERNING_LAW_CONTEXT_RE.search(normalized):
        return None

    for pattern in (_ENGLISH_GOVERNING_LAW_OF_RE, _ENGLISH_GOVERNING_BARE_LAW_RE):
        match = pattern.search(normalized)
        if match is None:
            continue
        normalized_law = _normalize_english_law_name(match.group("law"))
        if normalized_law is not None:
            return normalized_law

    return None


def _normalize_english_law_name(text: str) -> str | None:
    normalized = normalize_text(text).strip(" .,:;")
    normalized = re.sub(r"\s+", " ", normalized)

    if re.fullmatch(r"Japanese law", normalized, re.IGNORECASE):
        return "日本法"

    lower = normalized.lower()
    if not (
        lower.startswith("the laws of ")
        or lower.startswith("laws of ")
        or lower.startswith("the law of ")
        or lower.startswith("law of ")
        or lower.startswith("the internal laws of ")
        or lower.startswith("internal laws of ")
        or lower.startswith("the substantive laws of ")
        or lower.startswith("substantive laws of ")
        or lower.endswith(" law")
    ):
        return None

    jurisdiction = normalized
    if lower.startswith("the "):
        jurisdiction = jurisdiction[4:]
        lower = jurisdiction.lower()
    if lower.startswith("internal laws of "):
        jurisdiction = jurisdiction[len("internal laws of ") :]
    elif lower.startswith("substantive laws of "):
        jurisdiction = jurisdiction[len("substantive laws of ") :]
    elif lower.startswith("laws of "):
        jurisdiction = jurisdiction[len("laws of ") :]
    elif lower.startswith("law of "):
        jurisdiction = jurisdiction[len("law of ") :]
    elif lower.endswith(" law"):
        jurisdiction = jurisdiction[:-4]

    jurisdiction = re.sub(r"^(?:the\s+)", "", jurisdiction, flags=re.IGNORECASE)
    jurisdiction = re.split(r",\s*(?:without regard|excluding|except|provided that)\b", jurisdiction, maxsplit=1, flags=re.IGNORECASE)[0]
    jurisdiction = jurisdiction.strip(" .,:;")
    if not jurisdiction:
        return None

    if re.fullmatch(r"Japan|Japanese", jurisdiction, re.IGNORECASE):
        return "日本法"
    if re.fullmatch(r"applicable|internal|substantive", jurisdiction, re.IGNORECASE):
        return None
    if re.search(r"\b(?:discuss|comply|consider|facts?)\b", jurisdiction, re.IGNORECASE):
        return None

    return f"{jurisdiction} law"


def _reason_indicates_governing_law_clause(reason: str | None) -> bool:
    if not reason:
        return False
    return "governing_law" in reason


def _is_placeholder_counterparty(text: str) -> bool:
    compact = re.sub(r"\s+", "", normalize_text(text))
    if len(compact) < 2:
        return False
    return _PLACEHOLDER_COUNTERPARTY_RE.fullmatch(compact) is not None


def _is_entity_type_only_counterparty(text: str) -> bool:
    normalized = normalize_text(text)
    return normalized in _ENTITY_TYPE_ONLY_COUNTERPARTY_VALUES
