from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re

from contract_ingest.domain.models import ClauseUnit, EvidenceBlock, EvidenceRef
from contract_ingest.utils.text import normalize_digits, normalize_text, unique_preserve_order

_ABSOLUTE_DATE_RE = re.compile(
    r"(令和\s*[0-9０-９元]{1,2}年\s*[0-9０-９]{1,2}月\s*[0-9０-９]{1,2}日|"
    r"[0-9０-９]{4}\s*年\s*[0-9０-９]{1,2}\s*月\s*[0-9０-９]{1,2}\s*日|"
    r"[0-9０-９]{4}\s*[/.]\s*[0-9０-９]{1,2}\s*[/.]\s*[0-9０-９]{1,2})"
)
_PLACEHOLDER_DATE_TOKEN_RE = re.compile(r"令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日")
_GOVERNING_LAW_HEADING_KEYWORDS = (
    "準拠法",
    "適用法",
    "準拠法等",
    "管轄及び準拠法",
    "裁判管轄及び準拠法",
)
_GOVERNING_LAW_HEADING_RE = re.compile("|".join(re.escape(token) for token in _GOVERNING_LAW_HEADING_KEYWORDS))


@dataclass(frozen=True)
class TailFieldCandidate:
    value: str
    confidence: float
    reason: str
    evidence_ref: EvidenceRef
    snippet: str


def find_tail_governing_law_candidates(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    *,
    route: str = "UNKNOWN",
) -> list[TailFieldCandidate]:
    candidates: list[TailFieldCandidate] = []
    scopes = _combine_scopes(
        _iter_tail_context_scopes(
            blocks,
            clauses,
            trigger_tokens=_GOVERNING_LAW_HEADING_KEYWORDS + (
                "日本法",
                "日本国法",
                "適用する",
                "よる",
                "成立",
                "効力",
                "履行",
                "解釈",
                "紛争には",
            ),
        ),
        _iter_global_heading_scopes(
            blocks,
            trigger_tokens=_GOVERNING_LAW_HEADING_KEYWORDS + ("日本法", "日本国法"),
            require_clause_marker=True,
        ),
    )
    scopes = _combine_scopes(
        scopes,
        _iter_global_heading_scopes(
            blocks,
            trigger_tokens=_GOVERNING_LAW_HEADING_KEYWORDS,
            require_clause_marker=False,
            max_scopes=8,
        ),
    )
    scopes = _combine_scopes(scopes, find_governing_law_clause_spans(blocks, clauses))
    for text, ref in scopes:
        compact = _compact(text)
        if "準拠法" not in compact and "日本法" not in compact and "日本国法" not in compact:
            continue
        law: str | None = None
        if re.search(
            r"本契約[^。]{0,96}(?:成立|効力|履行|解釈)[^。]{0,96}(日本法|日本国法)[^。]{0,40}(?:による|に準拠|を適用)",
            compact,
        ):
            law = "日本法"
        elif re.search(
            r"本契約[^。]{0,96}(?:成立及び効力|成立および効力)[^。]{0,96}(日本法|日本国法)[^。]{0,40}(?:による|に準拠|を適用)",
            compact,
        ):
            law = "日本法"
        elif re.search(r"本契約に関する紛争には[^。]{0,40}(日本法|日本国法)[^。]{0,20}を適用", compact):
            law = "日本法"
        elif re.search(r"(日本法|日本国法)[^。]{0,32}(?:に準拠|を適用|による)", compact):
            law = "日本法"
        elif re.search(r"(?:準拠法|適用法|準拠法等)[^。]{0,40}(?:は|を|:|：)?(?:日本法|日本国法)", compact):
            law = "日本法"
        elif re.search(r"(?:日本法|日本国法)を準拠法とする", compact):
            law = "日本法"
        else:
            match = re.search(r"準拠法[^。]{0,24}(?:は|を|:|：)?([一-龥ぁ-んァ-ヶ]{2,10}法)", compact)
            if not match:
                match = re.search(r"適用法[^。]{0,24}(?:は|を|:|：)?([一-龥ぁ-んァ-ヶ]{2,10}法)", compact)
            if match:
                law = normalize_governing_law_text(match.group(1))
        if law is None:
            continue
        if law in {"法", "準拠法", "契約"}:
            continue
        confidence = score_governing_law_candidate(compact, route=route)
        candidates.append(
            TailFieldCandidate(
                value=law,
                confidence=min(0.95, confidence),
                reason="tail_clause_governing_law_span",
                evidence_ref=ref,
                snippet=text[:160],
            )
        )
    return _dedupe_candidates(candidates)


def find_tail_jurisdiction_candidates(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    *,
    route: str = "UNKNOWN",
) -> list[TailFieldCandidate]:
    candidates: list[TailFieldCandidate] = []
    scopes = _combine_scopes(
        _iter_tail_context_scopes(
            blocks,
            clauses,
            trigger_tokens=("裁判所", "管轄", "合意管轄", "第一審", "紛争", "訴え"),
        ),
        _iter_global_heading_scopes(
            blocks,
            trigger_tokens=("管轄", "裁判所", "合意管轄", "第一審"),
            require_clause_marker=True,
        ),
    )
    for text, ref in scopes:
        compact = _compact(text)
        if not any(token in compact for token in ["裁判所", "管轄", "合意管轄", "第一審"]):
            continue

        strict_court = _extract_specific_court_name(compact)
        if strict_court is not None and (
            "専属的合意管轄" in compact or "合意管轄" in compact or "第一審" in compact or "管轄裁判所" in compact
        ):
            confidence = 0.84 + (0.04 if route in {"SERVICE", "LICENSE_OR_ITAKU"} else 0.0)
            candidates.append(
                TailFieldCandidate(
                    value=strict_court,
                    confidence=min(0.96, confidence),
                    reason="tail_clause_jurisdiction_span",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue
        recovered = re.search(
            r"(?:紛争|訴え|管轄)[^。]{0,80}([一-龥々A-Za-z0-9○◯]{1,12}(?:地方|簡易|家庭|高等)裁判所)[^。]{0,60}(?:合意管轄|第一審|管轄裁判所|専属)",
            compact,
        )
        if recovered:
            candidates.append(
                TailFieldCandidate(
                    value=normalize_text(recovered.group(1)),
                    confidence=0.84 + _route_bonus(route, "jurisdiction"),
                    reason="tail_clause_jurisdiction_recovered_span",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        if re.search(r"(?:管轄|訴え|紛争)", compact):
            fallback_court = _extract_specific_court_name(compact)
            if fallback_court is not None:
                candidates.append(
                    TailFieldCandidate(
                        value=fallback_court,
                        confidence=0.80 + _route_bonus(route, "jurisdiction"),
                        reason="tail_clause_jurisdiction_fallback",
                        evidence_ref=ref,
                        snippet=text[:160],
                    )
                )
    return _dedupe_candidates(candidates)


def find_tail_expiration_candidates(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    *,
    route: str = "UNKNOWN",
) -> list[TailFieldCandidate]:
    candidates: list[TailFieldCandidate] = []
    scopes = _combine_scopes(
        _iter_tail_context_scopes(
            blocks,
            clauses,
            trigger_tokens=("有効期間", "契約期間", "委託期間", "履行期間", "満了", "終了", "更新", "まで", "契約締結"),
        ),
        _iter_global_heading_scopes(
            blocks,
            trigger_tokens=("有効期間", "契約期間", "委託期間", "履行期間", "満了", "更新"),
            require_clause_marker=True,
        ),
    )
    for text, ref in scopes:
        compact = _compact(text)
        if not any(token in compact for token in ["有効期間", "契約期間", "委託期間", "履行期間", "満了", "終了", "更新", "まで"]):
            continue

        absolute = _find_first_absolute_date(compact)
        if absolute is not None:
            candidates.append(
                TailFieldCandidate(
                    value=absolute,
                    confidence=0.82 + _route_bonus(route, "expiration_date"),
                    reason="tail_clause_expiration_absolute",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        relative = re.search(
            r"(?:契約締結日|契約締結の日|締結日|発効日|効力発生日)[^。]{0,24}(?:から|より)[^。]{0,24}(?:日|週|か月|ヶ月|ヵ月|年)間",
            compact,
        )
        if relative:
            candidates.append(
                TailFieldCandidate(
                    value=normalize_text(relative.group(0)),
                    confidence=0.66 + _route_bonus(route, "expiration_date"),
                    reason="tail_clause_expiration_relative",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        placeholder_dates = _PLACEHOLDER_DATE_TOKEN_RE.findall(compact)
        if len(placeholder_dates) >= 2 and "から" in compact and "まで" in compact:
            value = f"{normalize_text(placeholder_dates[0])}から{normalize_text(placeholder_dates[1])}まで"
            candidates.append(
                TailFieldCandidate(
                    value=value,
                    confidence=0.61 + _route_bonus(route, "expiration_date"),
                    reason="tail_clause_expiration_placeholder_range",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        renewable = re.search(
            r"(?:有効期間|契約期間|委託期間|履行期間|満了|終了)[^。]{0,100}(?:到来する日まで|更新|延長|短縮|自動更新|同一条件)",
            compact,
        )
        if renewable:
            candidates.append(
                TailFieldCandidate(
                    value=normalize_text(renewable.group(0)),
                    confidence=0.60 + _route_bonus(route, "expiration_date"),
                    reason="tail_clause_expiration_term_text",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
    return _dedupe_candidates(candidates)


def find_tail_effective_date_candidates(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    *,
    route: str = "UNKNOWN",
) -> list[TailFieldCandidate]:
    candidates: list[TailFieldCandidate] = []
    scopes = _combine_scopes(
        _iter_tail_context_scopes(
            blocks,
            clauses,
            trigger_tokens=("発効", "効力", "契約締結日", "契約締結の日", "締結日", "履行期間"),
        ),
        _iter_global_heading_scopes(
            blocks,
            trigger_tokens=("発効日", "効力発生日", "契約締結日", "契約締結の日", "履行期間"),
            require_clause_marker=True,
        ),
    )
    for text, ref in scopes:
        compact = _compact(text)
        has_effective_hint = any(token in compact for token in ["発効", "効力", "効力発生日", "発効日"])
        has_anchor_hint = any(token in compact for token in ["本契約締結日", "契約締結日", "契約締結の日", "締結日"])
        if not has_effective_hint and not has_anchor_hint:
            continue

        absolute = _find_first_absolute_date(compact)
        if absolute is not None and has_effective_hint:
            candidates.append(
                TailFieldCandidate(
                    value=absolute,
                    confidence=0.80 + _route_bonus(route, "effective_date"),
                    reason="tail_clause_effective_absolute",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        relative_effective = re.search(
            r"(?:本契約|効力|発効)[^。]{0,40}(?:契約締結日|契約締結の日|締結日)[^。]{0,24}(?:から|より)[^。]{0,24}(?:日|週|か月|ヶ月|ヵ月|年)間",
            compact,
        )
        if relative_effective:
            candidates.append(
                TailFieldCandidate(
                    value=normalize_text(relative_effective.group(0)).replace("契約締結の日", "契約締結日"),
                    confidence=0.64 + _route_bonus(route, "effective_date"),
                    reason="tail_clause_effective_relative",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        anchor = re.search(r"((?:本契約締結日|契約締結日|契約締結の日|締結日)\s*(?:から|より))", compact)
        if anchor:
            normalized_anchor = normalize_text(anchor.group(1)).replace("契約締結の日", "契約締結日")
            candidates.append(
                TailFieldCandidate(
                    value=normalized_anchor,
                    confidence=0.68 + _route_bonus(route, "effective_date"),
                    reason="tail_clause_effective_anchor",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
            continue

        placeholder_effective = re.search(
            r"(令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)[^。]{0,24}(?:より|から)[^。]{0,16}(?:効力|発効)",
            compact,
        )
        if placeholder_effective:
            candidates.append(
                TailFieldCandidate(
                    value=normalize_text(placeholder_effective.group(1)),
                    confidence=0.60 + _route_bonus(route, "effective_date"),
                    reason="tail_clause_effective_placeholder",
                    evidence_ref=ref,
                    snippet=text[:160],
                )
            )
    return _dedupe_candidates(candidates)


def find_governing_law_clause_spans(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None,
) -> list[tuple[str, EvidenceRef]]:
    scopes: list[tuple[str, EvidenceRef]] = []
    if clauses:
        for clause in clauses:
            if not clause.evidence_refs:
                continue
            title = normalize_text(f"{clause.clause_no or ''} {clause.clause_title or ''}")
            body = normalize_text(clause.text)
            compact_body = _compact(body)
            if _GOVERNING_LAW_HEADING_RE.search(_compact(title)) or "日本法" in compact_body or "日本国法" in compact_body:
                scopes.append((body, clause.evidence_refs[0]))

    ordered = sorted(blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))
    for idx, block in enumerate(ordered):
        compact = _compact(block.text)
        if not _GOVERNING_LAW_HEADING_RE.search(compact):
            continue
        span_blocks = [block]
        if idx > 0 and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[idx - 1]):
            span_blocks.append(ordered[idx - 1])
        if idx + 1 < len(ordered) and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[idx + 1]):
            span_blocks.append(ordered[idx + 1])
        if idx + 2 < len(ordered) and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[idx + 2]):
            span_blocks.append(ordered[idx + 2])
        merged_text = normalize_text(" ".join(item.text for item in sorted(span_blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))))
        if merged_text:
            scopes.append((merged_text, _to_ref(block)))
    return scopes


def score_governing_law_candidate(text: str, *, route: str) -> float:
    score = 0.80 + _route_bonus(route, "governing_law")
    if "準拠法" in text or "適用法" in text:
        score += 0.04
    if "日本法" in text or "日本国法" in text:
        score += 0.03
    if "成立" in text and "効力" in text and "解釈" in text:
        score += 0.03
    return min(0.96, score)


def normalize_governing_law_text(raw_text: str) -> str | None:
    normalized = normalize_text(raw_text)
    if "日本法" in normalized or "日本国法" in normalized:
        return "日本法"
    if normalized in {"法", "準拠法", "適用法"}:
        return None
    return normalized


def _iter_tail_scopes(blocks: list[EvidenceBlock], clauses: list[ClauseUnit] | None) -> list[tuple[str, EvidenceRef]]:
    scopes: list[tuple[str, EvidenceRef]] = []

    tail_blocks = _tail_blocks(blocks)
    for block in tail_blocks:
        scopes.append((normalize_text(block.text), _to_ref(block)))

    if clauses:
        tail_pages = {block.page for block in tail_blocks}
        for clause in clauses:
            if clause.page_end not in tail_pages and clause.page_start not in tail_pages:
                continue
            if not clause.evidence_refs:
                continue
            scopes.append((normalize_text(clause.text), clause.evidence_refs[0]))
    return scopes


def _iter_tail_context_scopes(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None,
    *,
    trigger_tokens: tuple[str, ...],
) -> list[tuple[str, EvidenceRef]]:
    scopes = list(_iter_tail_scopes(blocks, clauses))
    if not blocks:
        return scopes

    tail_blocks = _tail_blocks(blocks)
    by_page: dict[int, list[EvidenceBlock]] = {}
    for block in tail_blocks:
        by_page.setdefault(block.page, []).append(block)

    for page_blocks in by_page.values():
        ordered = sorted(page_blocks, key=lambda b: (b.bbox.y0, b.bbox.x0))
        for idx, block in enumerate(ordered):
            anchor_text = _compact(block.text)
            if not any(token in anchor_text for token in trigger_tokens):
                continue
            span_blocks = [block]
            for offset in (1, 2):
                prev_idx = idx - offset
                next_idx = idx + offset
                if prev_idx >= 0 and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[prev_idx]):
                    span_blocks.append(ordered[prev_idx])
                if next_idx < len(ordered) and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[next_idx]):
                    span_blocks.append(ordered[next_idx])
            if len(span_blocks) <= 1:
                continue
            span_blocks = sorted(span_blocks, key=lambda b: (b.bbox.y0, b.bbox.x0))
            merged_text = normalize_text(" ".join(item.text for item in span_blocks))
            if merged_text:
                scopes.append((merged_text, _to_ref(block)))
    return scopes


def _iter_global_heading_scopes(
    blocks: list[EvidenceBlock],
    *,
    trigger_tokens: tuple[str, ...],
    require_clause_marker: bool,
    max_scopes: int = 12,
) -> list[tuple[str, EvidenceRef]]:
    scopes: list[tuple[str, EvidenceRef]] = []
    if not blocks:
        return scopes

    ordered = sorted(blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))
    for idx, block in enumerate(ordered):
        compact = _compact(block.text)
        if not any(token in compact for token in trigger_tokens):
            continue
        if require_clause_marker and not re.search(r"第\s*[0-9０-９一二三四五六七八九十百千〇零]+\s*条", compact):
            continue
        span_blocks = [block]
        if idx > 0 and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[idx - 1]):
            span_blocks.append(ordered[idx - 1])
        if idx + 1 < len(ordered) and _is_neighbor_merge_candidate(anchor=block, neighbor=ordered[idx + 1]):
            span_blocks.append(ordered[idx + 1])
        span_blocks = sorted(span_blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))
        merged_text = normalize_text(" ".join(item.text for item in span_blocks))
        if not merged_text:
            continue
        scopes.append((merged_text, _to_ref(block)))
        if len(scopes) >= max_scopes:
            break
    return scopes


def _combine_scopes(
    primary: list[tuple[str, EvidenceRef]],
    secondary: list[tuple[str, EvidenceRef]],
) -> list[tuple[str, EvidenceRef]]:
    merged = list(primary)
    seen: set[tuple[int, str, str]] = {
        (ref.page, ref.block_id, normalize_text(text))
        for text, ref in primary
    }
    for text, ref in secondary:
        key = (ref.page, ref.block_id, normalize_text(text))
        if key in seen:
            continue
        seen.add(key)
        merged.append((text, ref))
    return merged


def _tail_blocks(blocks: list[EvidenceBlock], tail_ratio: float = 0.30, min_blocks: int = 10) -> list[EvidenceBlock]:
    if not blocks:
        return []
    ordered = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
    start_idx = max(0, int(len(ordered) * (1.0 - tail_ratio)))
    start_idx = min(start_idx, max(0, len(ordered) - min_blocks))
    tail = ordered[start_idx:]

    max_page = max(block.page for block in ordered)
    recent_pages = {max_page, max_page - 1}
    tail.extend(block for block in ordered if block.page in recent_pages)

    unique_ids: set[str] = set()
    result: list[EvidenceBlock] = []
    for block in tail:
        if block.block_id in unique_ids:
            continue
        unique_ids.add(block.block_id)
        result.append(block)
    return sorted(result, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))


def _find_first_absolute_date(text: str) -> str | None:
    for match in _ABSOLUTE_DATE_RE.finditer(text):
        token = normalize_text(match.group(1))
        iso = _normalize_date_token(token)
        if iso is not None:
            return iso
        # keep textual date when parser cannot normalize exactly
        return token
    return None


def _normalize_date_token(raw_date: str) -> str | None:
    normalized = normalize_digits(normalize_text(raw_date))

    reiwa = re.search(r"令和\s*(元|[0-9]{1,2})年\s*([0-9]{1,2})月\s*([0-9]{1,2})日", normalized)
    if reiwa:
        year_token = reiwa.group(1)
        year = 2019 if year_token == "元" else 2018 + int(year_token)
        month = int(reiwa.group(2))
        day = int(reiwa.group(3))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    ymd = re.search(r"([0-9]{4})\s*年\s*([0-9]{1,2})\s*月\s*([0-9]{1,2})\s*日", normalized)
    if ymd:
        year = int(ymd.group(1))
        month = int(ymd.group(2))
        day = int(ymd.group(3))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    slash = re.search(r"([0-9]{4})\s*[/.]\s*([0-9]{1,2})\s*[/.]\s*([0-9]{1,2})", normalized)
    if slash:
        year = int(slash.group(1))
        month = int(slash.group(2))
        day = int(slash.group(3))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    return None


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", normalize_text(text))


def _to_ref(block: EvidenceBlock) -> EvidenceRef:
    return EvidenceRef(
        page=block.page,
        block_id=block.block_id,
        bbox=block.bbox,
        confidence=block.confidence,
        engine=block.engine,
    )


def _dedupe_candidates(candidates: list[TailFieldCandidate]) -> list[TailFieldCandidate]:
    deduped: list[TailFieldCandidate] = []
    seen_values = set()
    for candidate in sorted(candidates, key=lambda c: (-c.confidence, c.evidence_ref.page, c.evidence_ref.block_id)):
        value = normalize_text(candidate.value)
        if not value or value in seen_values:
            continue
        seen_values.add(value)
        deduped.append(candidate)
    return deduped


def _is_neighbor_merge_candidate(anchor: EvidenceBlock, neighbor: EvidenceBlock) -> bool:
    if neighbor.page != anchor.page:
        return False
    vertical_gap = min(abs(neighbor.bbox.y0 - anchor.bbox.y1), abs(anchor.bbox.y0 - neighbor.bbox.y1))
    if vertical_gap > 180.0:
        return False
    x_gap = abs(neighbor.bbox.x0 - anchor.bbox.x0)
    if x_gap > 140.0 and neighbor.bbox.width < anchor.bbox.width * 0.45:
        return False
    return True


def _route_bonus(route: str, field_name: str) -> float:
    route_key = normalize_text(route).upper()
    if route_key == "NDA":
        if field_name in {"governing_law", "jurisdiction", "expiration_date"}:
            return 0.03
        return 0.0
    if route_key == "SERVICE":
        if field_name in {"effective_date", "expiration_date", "jurisdiction"}:
            return 0.02
        return 0.0
    if route_key == "LICENSE_OR_ITAKU":
        if field_name in {"expiration_date", "jurisdiction", "effective_date"}:
            return 0.02
        return 0.0
    return 0.0


def _extract_specific_court_name(text: str) -> str | None:
    generic = {"裁判所", "地方裁判所", "簡易裁判所", "家庭裁判所", "高等裁判所"}

    typed_hits = list(re.finditer(r"([一-龥々A-Za-z0-9○◯]{1,12}(?:地方|簡易|家庭|高等)裁判所)", text))
    if typed_hits:
        return normalize_text(typed_hits[-1].group(1))

    generic_hits = list(re.finditer(r"([一-龥々A-Za-z0-9○◯]{2,12}裁判所)", text))
    for match in reversed(generic_hits):
        value = normalize_text(match.group(1))
        if value in generic:
            continue
        if any(token in value for token in ["管轄", "専属", "第一審", "訴え", "紛争", "は", "を", "とする"]):
            continue
        return value
    return None
