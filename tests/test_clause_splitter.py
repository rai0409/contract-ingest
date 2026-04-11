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


def test_clause_splitter_ignores_footer_form_marker_for_boundary_transition() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条 受託者は業務を履行する。"),
        _make_block(2, "(様式-業務委託契約書(競争あり,個人情報あり)2023_01_01)", BlockType.FOOTER),
        _make_block(3, "第75号)の規定により選任された管財人。"),
        _make_block(4, "第2条 発注者は対価を支払う。"),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[0].clause_no == "第1条"
    assert "第75号)の規定により選任された管財人。" in result.clauses[0].text
    assert result.clauses[1].clause_no == "第2条"


def test_clause_splitter_keeps_signature_continuation_without_main_transition() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条 発注者は受託者に業務を委託する。"),
        _make_block(2, "上記契約の成立を証するため、次に記名押印する。", BlockType.SIGNATURE_AREA),
        _make_block(3, "する。", BlockType.OTHER, searchable=False),
        _make_block(4, "令和○○年○○月○○日"),
        _make_block(5, "発注者 福岡市西区元岡744 国立大学法人九州大学"),
        _make_block(6, "[氏 名] [印]", BlockType.SIGNATURE_AREA, searchable=False),
    ]

    result = splitter.split(blocks)

    assert len(result.clauses) == 2
    assert result.clauses[0].clause_no == "第1条"
    assert result.clauses[1].section_type.value == "signature"
    assert "令和○○年○○月○○日" in result.clauses[1].text
    assert "発注者 福岡市西区元岡744 国立大学法人九州大学" in result.clauses[1].text


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


def test_clause_splitter_avoids_main_clause_restart_spawn_in_tail_restart_context() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第8条 乙は委託業務の成果を甲に報告する。"),
        _make_block(2, "第9条 甲及び乙は存続条項を遵守する。"),
        _make_block(3, "本契約の締結を証するため、契約書2通を作成し、双方記名押印の上、各1通を保有する。"),
        _make_block(4, "2.業務委託契約約款 (1)約款本文"),
        _make_block(5, "第1条 乙は実施計画書に従って委託業務を実施する。"),
        _make_block(6, "第2条 乙は再委託に関する手続きを履行する。"),
    ]

    result = splitter.split(blocks)
    main_clause_nos = [clause.clause_no for clause in result.clauses if clause.section_type.value == "main_contract"]
    appendix_texts = [clause.text for clause in result.clauses if clause.section_type.value == "appendix"]

    assert "第1条" not in main_clause_nos
    assert "第2条" not in main_clause_nos
    assert any("約款本文" in text for text in appendix_texts)
    assert all(issue.reason_code != ReasonCode.REVERSED_CLAUSE_NUMBER for issue in result.issues)


def test_clause_splitter_treats_tail_restart_marker_as_appendix_boundary_signal() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第10条 甲は成果物の検査を行う。"),
        _make_block(2, "上記契約の成立を証するため、この契約書は2通作成し各自1通を保有する。"),
        _make_block(3, "業務委託契約約款"),
        _make_block(4, "約款本文"),
    ]

    result = splitter.split(blocks)
    section_types = [clause.section_type.value for clause in result.clauses]

    assert "signature" in section_types
    assert "appendix" in section_types


def test_clause_splitter_collapses_duplicate_heading_like_fragment() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第6章 雑則 第6章 雑則 （契約変更） （契約変更）"),
        _make_block(2, "第1条（目的）本契約の目的を定める。"),
    ]

    result = splitter.split(blocks)
    preamble = next(clause for clause in result.clauses if clause.section_type.value == "preamble")

    assert "第6章 雑則 第6章 雑則" not in preamble.text
    assert "（契約変更） （契約変更）" not in preamble.text
    assert "第6章 雑則" in preamble.text


def test_clause_splitter_rescues_numbered_prose_continuation_from_form_misclassification() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第6条（委託代金）委託代金の支払条件を定める。"),
        _make_block(2, "2 委託代金は、受託者の請求書を受領後30日以内に支払うものとする。"),
        _make_block(3, "第7条（通知）通知方法を定める。"),
    ]

    result = splitter.split(blocks)
    clause6 = next(clause for clause in result.clauses if clause.clause_no == "第6条")

    assert clause6.section_type.value == "main_contract"
    assert "2 委託代金は、受託者の請求書を受領後30日以内に支払うものとする。" in clause6.text
    assert all(clause.section_type.value != "form" for clause in result.clauses if clause.clause_no == "第6条")


def test_clause_splitter_rescues_numbered_prose_continuation_from_signature_misclassification() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第41条（損害賠償）損害賠償について定める。"),
        _make_block(2, "6 甲は、代表者が不正等の事実を認めたときは、直ちに乙へ通知するものとする。"),
        _make_block(3, "第42条（協議）本契約に関する疑義は協議して解決する。"),
    ]

    result = splitter.split(blocks)
    clause41 = next(clause for clause in result.clauses if clause.clause_no == "第41条")

    assert clause41.section_type.value == "main_contract"
    assert "6 甲は、代表者が不正等の事実を認めたときは、直ちに乙へ通知するものとする。" in clause41.text
    assert all(clause.section_type.value != "signature" for clause in result.clauses if clause.clause_no == "第41条")


def test_clause_splitter_rescues_page24_style_local_prose_continuation_from_signature_and_form() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第24条（成果報告書・中間年報の提出）乙は成果報告書を提出する。"),
        _make_block(
            2,
            "印刷・製本された成果報告書を甲に提出することができる。この場合においても、",
            block_type=BlockType.SIGNATURE_AREA,
        ),
        _make_block(
            3,
            "要約書は電子ファイル化されたものを提出しなければならない。"
            "第1項に規定する様式第10による委託業務成果報告届出書とともに提出する。",
        ),
        _make_block(4, "第25条（知的財産）知的財産の取扱いを定める。"),
    ]

    result = splitter.split(blocks)
    clause24 = next(clause for clause in result.clauses if clause.clause_no == "第24条")
    related_clauses = [
        clause for clause in result.clauses if "印刷・製本された成果報告書" in clause.text or "要約書は電子ファイル化されたもの" in clause.text
    ]

    assert clause24.section_type.value == "main_contract"
    assert "印刷・製本された成果報告書を甲に提出することができる。この場合においても、" in clause24.text
    assert "要約書は電子ファイル化されたものを提出しなければならない。" in clause24.text
    assert len(related_clauses) == 1
    assert related_clauses[0].clause_no == "第24条"
    assert all(clause.section_type.value == "main_contract" for clause in related_clauses)


def test_clause_splitter_keeps_article_reference_chain_inside_parent_clause() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第10条（存続）本契約の存続条項を定める。"),
        _make_block(2, "第3条、第19条、第20条第3項及び第75号)の規定により、乙は資料を返還する。"),
        _make_block(3, "第11条（通知）通知方法を定める。"),
    ]

    result = splitter.split(blocks)
    clause_nos = [clause.clause_no for clause in result.clauses]
    clause10 = next(clause for clause in result.clauses if clause.clause_no == "第10条")

    assert clause_nos.count("第10条") == 1
    assert clause_nos.count("第11条") == 1
    assert clause_nos.count("第3条") == 0
    assert "第3条、第19条、第20条第3項及び第75号)の規定により" in clause10.text


def test_clause_splitter_section_boundary_uncertain_details_include_signature_breakdown() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条（目的）本契約の目的を定める。"),
        _make_block(2, "住所 東京都千代田区"),
        _make_block(3, "当事者は本契約を誠実に履行する。"),
    ]

    result = splitter.split(blocks)
    issue = next(issue for issue in result.issues if issue.reason_code == ReasonCode.SECTION_BOUNDARY_UNCERTAIN)
    details = issue.details
    category_counts = details.get("category_counts", {})
    samples = details.get("samples", [])

    assert details.get("boundary_count", 0) >= 1
    assert category_counts.get("signature_boundary", 0) >= 1
    assert any(
        sample.get("from_section_type") == "signature" and sample.get("to_section_type") == "main_contract"
        for sample in samples
        if isinstance(sample, dict)
    )


def test_clause_splitter_section_boundary_uncertain_details_include_form_instruction_breakdown() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第1条（目的）本契約の目的を定める。"),
        _make_block(2, "様式第1号 請求書"),
        _make_block(3, "委託料の支払条件は別途定める。"),
    ]

    result = splitter.split(blocks)
    issue = next(issue for issue in result.issues if issue.reason_code == ReasonCode.SECTION_BOUNDARY_UNCERTAIN)
    category_counts = issue.details.get("category_counts", {})
    samples = issue.details.get("samples", [])

    assert category_counts.get("form_or_instruction_boundary", 0) >= 1
    assert any(
        sample.get("from_section_type") in {"form", "instruction"} and sample.get("to_section_type") == "main_contract"
        for sample in samples
        if isinstance(sample, dict)
    )


def test_clause_splitter_section_boundary_uncertain_details_include_tail_restart_breakdown() -> None:
    splitter = ClauseSplitter()
    blocks = [
        _make_block(1, "第8条 甲は成果物を提出する。"),
        _make_block(2, "業務委託契約約款"),
        _make_block(3, "当事者は誠実に協議する。"),
    ]

    result = splitter.split(blocks)
    issue = next(issue for issue in result.issues if issue.reason_code == ReasonCode.SECTION_BOUNDARY_UNCERTAIN)
    category_counts = issue.details.get("category_counts", {})

    assert category_counts.get("tail_restart_boundary", 0) >= 1
