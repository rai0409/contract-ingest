from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.models import BBox, EvidenceBlock
from contract_ingest.normalize.counterparty_finder import (
    find_preamble_counterparties,
    find_signature_counterparties,
    merge_counterparty_candidates,
)


def _make_block(page: int, order: int, text: str, block_type: BlockType = BlockType.TEXT) -> EvidenceBlock:
    y0 = 10.0 + order * 18.0
    return EvidenceBlock(
        page=page,
        block_id=f"p{page}_b{order:03d}",
        block_type=block_type,
        bbox=BBox(x0=20.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=0.94,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def test_counterparty_finder_extracts_preamble_candidates_with_roles() -> None:
    blocks = [
        _make_block(
            1,
            1,
            "公立大学法人福島県立医科大学(以下「甲」という。)と、○○株式会社(以下「乙」という。)は、次のとおり契約を締結する。",
        )
    ]

    candidates = find_preamble_counterparties(blocks)
    by_role = {candidate.role: candidate.name for candidate in candidates if candidate.role}

    assert by_role["甲"] == "公立大学法人福島県立医科大学"
    assert by_role["乙"] == "○○株式会社"


def test_counterparty_finder_extracts_signature_candidates_and_merges_without_fragments() -> None:
    blocks = [
        _make_block(
            1,
            1,
            "国立研究開発法人産業技術総合研究所(以下「甲」という。)と、という。)と、○○株式会社の間で締結する。",
        ),
        _make_block(
            5,
            80,
            "委託者(甲) 住 所 東京都千代田区 氏 名 国立研究開発法人産業技術総合研究所",
            block_type=BlockType.SIGNATURE_AREA,
        ),
        _make_block(
            5,
            81,
            "受託者(乙) 住 所  氏 名 ○○株式会社",
            block_type=BlockType.SIGNATURE_AREA,
        ),
    ]

    preamble_candidates = find_preamble_counterparties(blocks)
    signature_candidates = find_signature_counterparties(blocks)
    merged = merge_counterparty_candidates(preamble_candidates, signature_candidates)

    names = [candidate.name for candidate in merged]
    assert "国立研究開発法人産業技術総合研究所" in names
    assert "○○株式会社" in names
    assert all("という。" not in name for name in names)
