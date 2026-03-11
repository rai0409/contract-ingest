from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.enums import ReasonCode
from contract_ingest.domain.models import BBox, ClauseUnit, EvidenceBlock
from contract_ingest.normalize.clause_splitter import ClauseSplitter


def _make_block(
    order: int,
    text: str,
    block_type: BlockType = BlockType.TEXT,
    *,
    searchable: bool = True,
) -> EvidenceBlock:
    y0 = 10.0 + (order - 1) * 20.0
    return EvidenceBlock(
        page=1,
        block_id=f"p1_b{order:03d}",
        block_type=block_type,
        bbox=BBox(x0=10.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=None,
        searchable=searchable,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def test_clause_splitter_handles_strong_article_and_following_parenthesized_title() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "（委託期間）"),
        _make_block(3, "委託業務の委託期間は、契約締結日から令和5年6月30日までとする。"),
        _make_block(4, "第2条"),
        _make_block(5, "（委託料）"),
        _make_block(6, "委託料は別途協議のうえ定める。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[0].clause_no == "第1条"
    assert result.clauses[0].clause_title == "委託期間"
    assert result.clauses[1].clause_no == "第2条"
    assert result.clauses[1].clause_title == "委託料"


def test_clause_splitter_does_not_promote_item_markers_to_article_headings() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "(1)"),
        _make_block(3, "①"),
        _make_block(4, "2"),
        _make_block(5, "本条の本文テキスト。"),
        _make_block(6, "第2条"),
        _make_block(7, "次条本文。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[0].clause_no == "第1条"
    assert result.clauses[1].clause_no == "第2条"


def test_clause_splitter_splits_embedded_next_article_to_prevent_leakage() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条 本文A。第2条 本文B。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[0].clause_no == "第1条"
    assert "第2条" not in result.clauses[0].text
    assert result.clauses[1].clause_no == "第2条"


def test_clause_splitter_does_not_embedded_split_on_subtitle_only_fragment() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条（目的）本契約の目的を定める。（委託料）"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 1
    assert result.clauses[0].clause_no == "第1条"


def test_clause_splitter_avoids_annotation_like_block_as_core_material() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "解説編"),
        _make_block(3, "本契約は日本法に準拠する。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 1
    assert "解説編" not in result.clauses[0].text


def test_clause_splitter_attaches_preceding_parenthesized_subtitle_to_next_article() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "本契約は当事者間の基本条件を定める。"),
        _make_block(3, "（委託料）"),
        _make_block(4, "第2条"),
        _make_block(5, "委託料は別途協議の上定める。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[1].clause_no == "第2条"
    assert result.clauses[1].clause_title == "委託料"
    assert "（委託料）" not in result.clauses[0].text


def test_clause_splitter_excludes_annotation_placeholders_from_clause_material() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "コメントの追加"),
        _make_block(3, "[A1]"),
        _make_block(4, "(住所)"),
        _make_block(5, "本契約は日本法に準拠する。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 1
    assert "コメントの追加" not in result.clauses[0].text
    assert "[A1]" not in result.clauses[0].text
    assert "(住所)" not in result.clauses[0].text


def test_clause_splitter_repairs_short_reversed_clause_number_fragment() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第2条"),
        _make_block(2, "本契約の有効期間は当事者間の合意による。"),
        _make_block(3, "第1条 補足として前条の条件を再確認する。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 1
    assert result.clauses[0].clause_no == "第2条"
    assert "repaired_reversed_clause_number" in result.clauses[0].flags
    assert all(issue.reason_code != ReasonCode.REVERSED_CLAUSE_NUMBER for issue in result.issues)


def test_clause_splitter_attaches_orphan_paragraph_clause_to_previous_clause() -> None:
    splitter = ClauseSplitter()
    prev_block = _make_block(1, "第1条 本契約に関する基本条件を定める。")
    orphan_block = _make_block(2, "のとおり当事者は誠実に協議する。")
    previous = ClauseUnit(
        clause_id="clause_001",
        clause_no="第1条",
        clause_title=None,
        text="第1条 本契約に関する基本条件を定める。",
        page_start=1,
        page_end=1,
        block_ids=[prev_block.block_id],
        evidence_refs=[splitter._to_ref(prev_block)],
        flags=[],
    )
    orphan = ClauseUnit(
        clause_id="clause_002",
        clause_no=None,
        clause_title=None,
        text="のとおり当事者は誠実に協議する。",
        page_start=1,
        page_end=1,
        block_ids=[orphan_block.block_id],
        evidence_refs=[splitter._to_ref(orphan_block)],
        flags=[],
    )

    merged = splitter._attach_orphan_paragraphs([previous, orphan])

    assert len(merged) == 1
    assert "attached_orphan_paragraph" in merged[0].flags
    assert "のとおり当事者は誠実に協議する。" in merged[0].text


def test_clause_splitter_does_not_promote_non_searchable_short_heading_fragment() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "本契約は当事者間の基本条件を定める。"),
        _make_block(3, "第2条", searchable=False),
        _make_block(4, "追加の本文断片。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 1
    assert result.clauses[0].clause_no == "第1条"


def test_clause_splitter_emits_unstable_review_for_heading_only_clauses() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条"),
        _make_block(2, "第2条"),
        _make_block(3, "第3条"),
    ]

    result = splitter.split(blocks)

    assert any(issue.reason_code == ReasonCode.UNSTABLE_CLAUSE_SPLIT for issue in result.issues)
