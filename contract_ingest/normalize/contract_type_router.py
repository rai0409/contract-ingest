from __future__ import annotations

from contract_ingest.domain.models import ClauseUnit, EvidenceBlock
from contract_ingest.utils.text import normalize_text

ROUTE_NDA = "NDA"
ROUTE_SERVICE = "SERVICE"
ROUTE_LICENSE_OR_ITAKU = "LICENSE_OR_ITAKU"
ROUTE_UNKNOWN = "UNKNOWN"


def infer_contract_type(
    blocks: list[EvidenceBlock],
    clauses: list[ClauseUnit] | None = None,
    *,
    hinted_contract_type: str | None = None,
) -> str:
    scores = {
        ROUTE_NDA: 0.0,
        ROUTE_SERVICE: 0.0,
        ROUTE_LICENSE_OR_ITAKU: 0.0,
    }

    snippets: list[str] = []
    ordered = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
    snippets.extend(normalize_text(block.text) for block in ordered[:40] if normalize_text(block.text))
    if clauses:
        snippets.extend(normalize_text(f"{clause.clause_no or ''} {clause.clause_title or ''} {clause.text}") for clause in clauses[:20])

    for text in snippets:
        compact = text.replace(" ", "")
        # NDA indicators
        if any(token in compact for token in ["秘密保持", "秘密情報", "開示", "目的外利用", "守秘義務"]):
            scores[ROUTE_NDA] += 1.05
        # SERVICE indicators
        if any(token in compact for token in ["業務委託", "成果物", "検収", "再委託", "委託業務"]):
            scores[ROUTE_SERVICE] += 1.0
        # LICENSE/ITAKU indicators
        if any(token in compact for token in ["委託", "使用許諾", "利用許諾", "ライセンス", "許諾"]):
            scores[ROUTE_LICENSE_OR_ITAKU] += 0.85

    hinted = normalize_text(hinted_contract_type or "")
    if hinted:
        hint_compact = hinted.replace(" ", "")
        if any(token in hint_compact for token in ["秘密保持", "NDA"]):
            scores[ROUTE_NDA] += 1.6
        if any(token in hint_compact for token in ["業務委託", "準委任"]):
            scores[ROUTE_SERVICE] += 1.2
        if any(token in hint_compact for token in ["許諾", "ライセンス", "委託"]):
            scores[ROUTE_LICENSE_OR_ITAKU] += 1.0

    best_route, best_score = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0]
    if best_score < 1.2:
        return ROUTE_UNKNOWN
    return best_route


def route_field_bias(route: str, field_name: str) -> float:
    route_key = normalize_text(route).upper()
    if route_key == ROUTE_NDA:
        if field_name in {"governing_law", "jurisdiction", "expiration_date", "effective_date"}:
            return 0.03
        return 0.0
    if route_key == ROUTE_SERVICE:
        if field_name in {"effective_date", "expiration_date", "counterparties", "governing_law"}:
            return 0.03
        return 0.0
    if route_key == ROUTE_LICENSE_OR_ITAKU:
        if field_name in {"counterparties", "jurisdiction", "expiration_date", "governing_law", "effective_date"}:
            return 0.03
        return 0.0
    return 0.0
