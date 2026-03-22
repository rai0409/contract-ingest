from __future__ import annotations

from dataclasses import dataclass
import re

from contract_ingest.domain.models import ClauseUnit, EvidenceBlock, EvidenceRef
from contract_ingest.utils.text import normalize_text

_TITLE_KEYWORD_RE = re.compile(
    r"(?:秘密保持契約書|機密保持契約書|業務委託(?:基本)?契約書|契約書(?:\s*[（(]案[）)])?|覚書|\bNDA\b)",
    re.IGNORECASE,
)
_NOISE_PREFIX_RE = re.compile(r"^\s*(?:第[0-9０-９一二三四五六七八九十百千〇零]+条|[0-9０-９]+\s*[.．、])")


@dataclass(frozen=True)
class TitleExtractionResult:
    title: str | None
    reason: str
    evidence_ref: EvidenceRef | None


def extract_document_title(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    contract_type_hint: str | None = None,
) -> TitleExtractionResult:
    if not blocks:
        return TitleExtractionResult(title=None, reason="title_not_found", evidence_ref=None)

    ordered = sorted(blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0, b.reading_order))
    best_score = -1.0
    best_title: str | None = None
    best_ref: EvidenceRef | None = None

    for block in ordered:
        if block.page > 2:
            break
        text = normalize_text(block.text)
        if not text:
            continue
        for line in _split_title_candidates(text):
            if not line or _NOISE_PREFIX_RE.match(line):
                continue
            score = _score_title_candidate(line=line, block=block)
            if score <= best_score:
                continue
            best_score = score
            best_title = line
            best_ref = _to_ref(block)

    if best_title is not None:
        return TitleExtractionResult(title=best_title, reason="matched_cover_title_rule", evidence_ref=best_ref)

    if contract_type_hint and "契約" in normalize_text(contract_type_hint):
        return TitleExtractionResult(
            title=normalize_text(contract_type_hint),
            reason="fallback_contract_type_title",
            evidence_ref=None,
        )

    if clauses:
        for clause in clauses[:3]:
            candidate = normalize_text(f"{clause.clause_no or ''} {clause.clause_title or ''}")
            if _TITLE_KEYWORD_RE.search(candidate):
                ref = clause.evidence_refs[0] if clause.evidence_refs else None
                return TitleExtractionResult(title=candidate, reason="fallback_clause_heading_title", evidence_ref=ref)

    return TitleExtractionResult(title=None, reason="title_not_found", evidence_ref=None)


def _split_title_candidates(text: str) -> list[str]:
    raw_parts = re.split(r"[\n]", text)
    candidates: list[str] = []
    for raw in raw_parts:
        line = normalize_text(raw)
        if not line:
            continue
        keyword_spans = list(
            re.finditer(
                r"([^\n]{0,64}?(?:秘密保持契約書|機密保持契約書|業務委託(?:基本)?契約書|契約書(?:\s*[（(]案[）)])?|覚書|NDA))",
                line,
                flags=re.IGNORECASE,
            )
        )
        for span in keyword_spans:
            candidate = normalize_text(span.group(1))
            if candidate:
                candidates.append(candidate)
        for pure in re.finditer(
            r"(秘密保持契約書|機密保持契約書|業務委託(?:基本)?契約書|契約書(?:\s*[（(]案[）)])?|覚書|NDA)",
            line,
            flags=re.IGNORECASE,
        ):
            candidate = normalize_text(pure.group(1))
            if candidate:
                candidates.append(candidate)
        # Long sentence-like lines are usually body text, not title.
        if len(line) > 80 and "契約書" not in line:
            continue
        if len(line) <= 1:
            continue
        candidates.append(line)
    return candidates


def _score_title_candidate(line: str, block: EvidenceBlock) -> float:
    score = 0.0
    normalized = normalize_text(line)
    if _TITLE_KEYWORD_RE.search(normalized):
        score += 2.4
    if "契約書" in normalized:
        score += 0.8
    if block.page == 1:
        score += 0.5
    if block.bbox.y0 <= 220.0:
        score += 0.4
    if len(normalized) <= 40:
        score += 0.2
    if len(normalized) > 55:
        score -= 0.8
    if "第" in normalized and "条" in normalized:
        score -= 1.0
    if any(token in normalized for token in ["仕様書", "別紙", "記載要領", "通知書", "届出書"]):
        score -= 0.6
    if any(token in normalized for token in ["は、", "ものとする", "以下", "甲", "乙"]):
        score -= 0.8
    return score


def _to_ref(block: EvidenceBlock) -> EvidenceRef:
    return EvidenceRef(
        page=block.page,
        block_id=block.block_id,
        bbox=block.bbox,
        confidence=block.confidence,
        engine=block.engine,
    )
