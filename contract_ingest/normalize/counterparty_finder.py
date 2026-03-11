from __future__ import annotations

from dataclasses import dataclass
import re

from contract_ingest.domain.models import EvidenceBlock, EvidenceRef
from contract_ingest.utils.text import normalize_text, unique_preserve_order


@dataclass(frozen=True)
class CounterpartyCandidate:
    name: str
    role: str | None
    confidence: float
    source: str
    evidence_ref: EvidenceRef
    snippet: str


def find_preamble_counterparties(blocks: list[EvidenceBlock]) -> list[CounterpartyCandidate]:
    candidates: list[CounterpartyCandidate] = []
    preamble_blocks = _preamble_blocks(blocks)
    for block in preamble_blocks:
        text = normalize_text(block.text)
        if not text:
            continue

        # Name before role declaration: "...(以下「甲」という。)"
        for match in re.finditer(r"(?P<name>[^。\n]{2,120}?)\s*[（(]?\s*以下\s*[「『]?(?P<role>甲|乙|委託者|受託者)[」』]?", text):
            name = _normalize_name(match.group("name"))
            role = _normalize_role(match.group("role"))
            if _is_candidate_name(name):
                candidates.append(
                    CounterpartyCandidate(
                        name=name,
                        role=role,
                        confidence=0.90,
                        source="preamble_role_declaration",
                        evidence_ref=_to_ref(block),
                        snippet=text[:160],
                    )
                )

        # "甲: 株式会社..." style
        for match in re.finditer(r"(?P<role>甲|乙|委託者|受託者)\s*[:：]\s*(?P<name>[^\n]{2,120})", text):
            name = _normalize_name(match.group("name"))
            role = _normalize_role(match.group("role"))
            if _is_candidate_name(name):
                candidates.append(
                    CounterpartyCandidate(
                        name=name,
                        role=role,
                        confidence=0.85,
                        source="preamble_role_label",
                        evidence_ref=_to_ref(block),
                        snippet=text[:160],
                    )
                )

        for name in _extract_entity_like_names(text):
            if _is_candidate_name(name):
                candidates.append(
                    CounterpartyCandidate(
                        name=name,
                        role=None,
                        confidence=0.72,
                        source="preamble_entity_scan",
                        evidence_ref=_to_ref(block),
                        snippet=text[:160],
                    )
                )

    return _dedupe_counterparty_candidates(candidates)


def find_signature_counterparties(blocks: list[EvidenceBlock]) -> list[CounterpartyCandidate]:
    candidates: list[CounterpartyCandidate] = []
    for block in _signature_blocks(blocks):
        text = normalize_text(block.text)
        if not text:
            continue

        for match in re.finditer(r"(?P<role>委託者|受託者|甲|乙)\s*(?:[（(]?(?:甲|乙)?[）)])?\s*[:：]?\s*(?P<name>[^\n]{0,120})", text):
            role = _normalize_role(match.group("role"))
            name = _normalize_name(match.group("name"))
            for entity in [name, *_extract_entity_like_names(name)]:
                if _is_candidate_name(entity):
                    candidates.append(
                        CounterpartyCandidate(
                            name=entity,
                            role=role,
                            confidence=0.82,
                            source="signature_role_label",
                            evidence_ref=_to_ref(block),
                            snippet=text[:160],
                        )
                    )

        for entity in _extract_entity_like_names(text):
            if _is_candidate_name(entity):
                candidates.append(
                    CounterpartyCandidate(
                        name=entity,
                        role=None,
                        confidence=0.70,
                        source="signature_entity_scan",
                        evidence_ref=_to_ref(block),
                        snippet=text[:160],
                    )
                )

    return _dedupe_counterparty_candidates(candidates)


def merge_counterparty_candidates(
    preamble_candidates: list[CounterpartyCandidate],
    signature_candidates: list[CounterpartyCandidate],
) -> list[CounterpartyCandidate]:
    merged = _dedupe_counterparty_candidates([*preamble_candidates, *signature_candidates])
    if not merged:
        return []

    role_best: dict[str, CounterpartyCandidate] = {}
    unassigned: list[CounterpartyCandidate] = []
    for candidate in merged:
        if candidate.role in {"甲", "乙"}:
            current = role_best.get(candidate.role)
            if current is None or candidate.confidence > current.confidence:
                role_best[candidate.role] = candidate
        else:
            unassigned.append(candidate)

    ordered: list[CounterpartyCandidate] = []
    if "甲" in role_best:
        ordered.append(role_best["甲"])
    if "乙" in role_best:
        ordered.append(role_best["乙"])

    used = {candidate.name for candidate in ordered}
    for candidate in sorted(unassigned, key=lambda c: (-c.confidence, c.name)):
        if candidate.name in used:
            continue
        if any(
            (candidate.name.startswith(existing) and len(candidate.name) - len(existing) <= 8)
            or (existing.startswith(candidate.name) and len(existing) - len(candidate.name) <= 8)
            for existing in used
        ):
            continue
        ordered.append(candidate)
        used.add(candidate.name)

    # Keep compact output for downstream validator
    return ordered[:4]


def _normalize_name(value: str) -> str:
    text = normalize_text(value)
    text = text.replace("㈱", "株式会社")
    text = re.sub(r"(?:以下\s*[「『]?[甲乙].*)$", "", text)
    text = re.sub(r"^(?:委託者|受託者|甲|乙)\s*[:：]?", "", text)
    text = re.sub(r"^(?:と|及び|または|又は)\s*[、,]?\s*", "", text)
    if re.search(r"氏\s*名", text):
        text = re.split(r"氏\s*名", text, maxsplit=1)[-1]
    entity_from_text = _extract_named_entity(text)
    if entity_from_text:
        text = entity_from_text
    else:
        text = re.split(r"(?:との間|の間に|について|により|ものとする|こととする)", text, maxsplit=1)[0]
        for marker in ["規程", "規定", "協議", "紛争", "管轄", "条", "項", "号", "契約", "予算", "決算", "事務"]:
            if marker in text:
                text = text.split(marker, 1)[0]
    text = re.sub(r"^\s*という。?\)?\s*(?:と|と、)?", "", text)
    entity_from_text = _extract_named_entity(text)
    if entity_from_text:
        text = entity_from_text
    for marker in ["規程", "規定", "予算", "決算", "事務", "取扱", "(平成", "平成", "第"]:
        if marker in text:
            text = text.split(marker, 1)[0]
    if re.search(r"(?:住\s*所|所在地)", text):
        entity_matches = list(
            re.finditer(
                r"([^\s、。]{2,50}(?:株式会社|合同会社|有限会社)|(?:公立大学法人|国立大学法人|学校法人)[^\s、。]{1,30}|[^\s、。]{2,40}研究所|[^\s、。]{2,40}大学)",
                text,
            )
        )
        if entity_matches:
            text = entity_matches[-1].group(1)
    text = text.strip(" 　,:：;；、，。()（）[]【】")
    text = re.sub(r"^(?:と|及び|または|又は)\s*[、,]?\s*", "", text)
    text = re.sub(r"[（(]\s*$", "", text).strip()
    return text


def _extract_entity_like_names(text: str) -> list[str]:
    results: list[str] = []
    patterns = [
        r"([^\s、。]{2,50}(?:株式会社|合同会社|有限会社))",
        r"((?:公立大学法人|国立大学法人|学校法人)[^\s、。]{1,30})",
        r"((?:一般社団法人|一般財団法人|公益社団法人|公益財団法人|医療法人|社会福祉法人)[^\s、。]{1,20})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            results.append(_normalize_name(match.group(1)))
    return unique_preserve_order([candidate for candidate in results if candidate])


def _extract_named_entity(text: str) -> str | None:
    patterns = [
        r"([^\s、。]{2,50}(?:株式会社|合同会社|有限会社))",
        r"((?:公立大学法人|国立大学法人|学校法人)[^\s、。]{1,30})",
        r"((?:一般社団法人|一般財団法人|公益社団法人|公益財団法人|医療法人|社会福祉法人)[^\s、。]{1,20})",
        r"([^\s、。]{2,40}(?:研究所|機構|協会))",
    ]
    matches: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            candidate = normalize_text(match.group(1)).strip(" 　,:：;；、，。()（）[]【】")
            if candidate:
                matches.append(candidate)
    if not matches:
        return None
    return matches[-1]


def _is_candidate_name(name: str) -> bool:
    if not name:
        return False
    if len(name) < 2 or len(name) > 90:
        return False
    if any(token in name for token in ["という。", "コメント", "解説", "参照", "本契約", "締結する"]):
        return False
    if any(token in name for token in ["業務委託", "契約書", "大学案内", "作成", "仕様書", "別紙"]):
        return False
    if any(token in name for token in ["の間", "協議", "紛争", "管轄", "条", "項", "号", "規程", "規定", "ものとする", "次の各", "各号"]):
        return False
    if name[0] in {"は", "が", "を", "に", "で", "と", "の"}:
        return False
    if name in {"甲", "乙", "委託者", "受託者"}:
        return False
    if name.endswith(("と", "と、", "の", "は")):
        return False
    entity_markers = ("株式会社", "合同会社", "有限会社", "法人", "大学", "研究所", "機構", "協会", "組合", "銀行")
    if not any(marker in name for marker in entity_markers):
        if len(name) > 16:
            return False
        kanji_kana = sum(1 for ch in name if ("\u3040" <= ch <= "\u30FF") or ("\u4E00" <= ch <= "\u9FFF"))
        if kanji_kana < 3:
            return False
    return True


def _normalize_role(role: str | None) -> str | None:
    if role is None:
        return None
    normalized = normalize_text(role)
    if normalized in {"甲", "委託者", "ライセンサー"}:
        return "甲"
    if normalized in {"乙", "受託者", "ライセンシー"}:
        return "乙"
    return None


def _preamble_blocks(blocks: list[EvidenceBlock]) -> list[EvidenceBlock]:
    if not blocks:
        return []
    ordered = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
    max_page = max(block.page for block in ordered)
    preamble = [block for block in ordered if block.page <= min(2, max_page) and block.reading_order <= 30]
    if len(preamble) < 6:
        preamble = ordered[: max(10, len(ordered) // 3)]
    return preamble


def _signature_blocks(blocks: list[EvidenceBlock]) -> list[EvidenceBlock]:
    if not blocks:
        return []
    ordered = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
    max_page = max(block.page for block in ordered)
    results: list[EvidenceBlock] = []
    for block in ordered:
        text = normalize_text(block.text)
        near_tail = block.page >= max_page - 1
        signature_hint = any(token in text for token in ["記名押印", "署名", "住 所", "住所", "氏名", "代表者", "委託者", "受託者"])
        if near_tail and signature_hint:
            results.append(block)
    return results or ordered[max(0, len(ordered) - 12) :]


def _to_ref(block: EvidenceBlock) -> EvidenceRef:
    return EvidenceRef(
        page=block.page,
        block_id=block.block_id,
        bbox=block.bbox,
        confidence=block.confidence,
        engine=block.engine,
    )


def _dedupe_counterparty_candidates(candidates: list[CounterpartyCandidate]) -> list[CounterpartyCandidate]:
    best_by_name: dict[str, CounterpartyCandidate] = {}
    for candidate in candidates:
        key = normalize_text(candidate.name)
        if not key:
            continue
        previous = best_by_name.get(key)
        if previous is None or candidate.confidence > previous.confidence:
            best_by_name[key] = candidate
    return sorted(best_by_name.values(), key=lambda c: (-c.confidence, c.name))
