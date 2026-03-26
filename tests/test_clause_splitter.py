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


def test_clause_splitter_separates_preamble_from_first_article_and_assigns_section_types() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "業務委託契約書"),
        _make_block(2, "甲と乙は次のとおり契約を締結する。"),
        _make_block(3, "第1条"),
        _make_block(4, "（目的）"),
        _make_block(5, "甲は乙に業務を委託する。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) >= 2
    assert result.clauses[0].section_type.value == "preamble"
    assert result.clauses[0].clause_no is None
    assert result.clauses[1].clause_no == "第1条"
    assert result.clauses[1].section_type.value == "main_contract"


def test_clause_splitter_separates_form_and_instruction_from_main_contract() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条（目的）本契約の目的を定める。"),
        _make_block(2, "第2条（委託業務）乙は本業務を履行する。"),
        _make_block(3, "別紙1 仕様書"),
        _make_block(4, "様式第1号 請求書"),
        _make_block(5, "記載要領 1. 記入方法"),
    ]

    result = splitter.split(blocks)
    section_types = [clause.section_type.value for clause in result.clauses]

    assert "main_contract" in section_types
    assert "appendix" in section_types
    assert "form" in section_types
    assert "instruction" in section_types


def test_split_embedded_headings_rejects_law_reference_split_point() -> None:
    segments = ClauseSplitter._split_embedded_headings("本契約は民法 第467条第1項に従って履行する。")

    assert len(segments) == 1
    assert "第467条第1項" in segments[0]


def test_should_start_new_clause_heading_rejects_article_citation_tail() -> None:
    block = _make_block(1, "第7条第2号に定める事項に従う。")
    heading = ClauseSplitter._detect_heading(block.text)

    assert heading is not None
    assert (
        ClauseSplitter._should_start_new_clause_heading(
            current=None,
            heading_text=block.text,
            heading=heading,
            previous_article_number=6,
            block=block,
        )
        is False
    )


def test_clause_splitter_keeps_in_body_article_references_inside_parent_clause() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条（目的）本契約の目的を定める。"),
        _make_block(2, "前項に定める事項は第7条第2号による。"),
        _make_block(3, "通知は民法 第467条に基づき行う。"),
        _make_block(4, "第2条（委託料）委託料を定める。"),
    ]

    result = splitter.split(blocks)
    clause_nos = [clause.clause_no for clause in result.clauses]

    assert clause_nos.count("第1条") == 1
    assert clause_nos.count("第2条") == 1
    assert "第467条" not in clause_nos
    assert "第7条第2号" in result.clauses[0].text
    assert "民法 第467条" in result.clauses[0].text


def test_clause_splitter_merges_spurious_high_article_citation_clause() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第10条（通知）通知方法を定める。"),
        _make_block(2, "第467条または民法の特例法第4条第2項に定める要件に従う。"),
        _make_block(4, "第11条（委託料）委託料を定める。"),
    ]

    result = splitter.split(blocks)
    clause_nos = [clause.clause_no for clause in result.clauses]

    assert "第467条" not in clause_nos
    assert clause_nos.count("第10条") == 1
    clause10 = next(clause for clause in result.clauses if clause.clause_no == "第10条")
    assert "第467条または" in clause10.text


def test_clause_splitter_dedupe_clause_heading_prefix_compact_repeat() -> None:
    clause = ClauseUnit(
        clause_id="clause_015",
        clause_no="第15条",
        clause_title=None,
        text="第15条第15条 乙は本契約に従う。",
        page_start=1,
        page_end=1,
        block_ids=["p1_b001"],
        evidence_refs=[],
        flags=[],
    )

    deduped = ClauseSplitter._dedupe_clause_heading_prefix(clause)

    assert deduped.text.startswith("第15条 ")
    assert "第15条第15条" not in deduped.text


def test_clause_splitter_separates_execution_signature_tail_from_main_contract() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第10条（準拠法）本契約は日本法に準拠する。"),
        _make_block(2, "第11条（管轄）福岡地方裁判所を第一審の専属的合意管轄裁判所とする。"),
        _make_block(3, "上記契約の成立を証するため、この契約書は2通作成し、各自1通を保有する。"),
        _make_block(4, "本契約は電磁的記録として作成し、電子署名を行う。"),
    ]

    result = splitter.split(blocks)
    main_clauses = [clause for clause in result.clauses if clause.section_type.value == "main_contract"]
    signature_clauses = [clause for clause in result.clauses if clause.section_type.value == "signature"]

    assert len(main_clauses) >= 2
    assert signature_clauses
    assert all("上記契約の成立を証するため" not in clause.text for clause in main_clauses)
    assert any("上記契約の成立を証するため" in clause.text for clause in signature_clauses)
