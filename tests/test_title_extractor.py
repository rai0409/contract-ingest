from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.models import BBox, EvidenceBlock
from contract_ingest.normalize.title_extractor import extract_document_title


def _make_block(page: int, order: int, text: str) -> EvidenceBlock:
    y0 = 20.0 + order * 18.0
    return EvidenceBlock(
        page=page,
        block_id=f"p{page}_b{order:03d}",
        block_type=BlockType.TEXT,
        bbox=BBox(x0=20.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=0.95,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def test_title_extractor_prefers_cover_title_over_body_heading() -> None:
    blocks = [
        _make_block(1, 1, "業務委託契約書"),
        _make_block(1, 2, "第1条（目的）甲は乙に業務を委託する。"),
    ]

    result = extract_document_title(blocks, contract_type_hint="業務委託契約書")

    assert result.title == "業務委託契約書"
    assert result.reason == "matched_cover_title_rule"


def test_title_extractor_falls_back_to_contract_type_hint() -> None:
    blocks = [
        _make_block(1, 1, "第1条（目的）本契約の目的は次のとおりとする。"),
    ]

    result = extract_document_title(blocks, contract_type_hint="秘密保持契約書")

    assert result.title == "秘密保持契約書"
    assert result.reason == "fallback_contract_type_title"
