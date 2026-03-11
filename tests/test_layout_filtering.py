from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.models import BBox, NativeTextBlock
from contract_ingest.extract.layout import LayoutAnalyzer, infer_block_type


def _make_native_block(
    page: int,
    block_id: str,
    text: str,
    *,
    y0: float,
    y1: float,
    searchable: bool,
    block_type: BlockType = BlockType.TEXT,
) -> NativeTextBlock:
    return NativeTextBlock(
        page=page,
        block_id=block_id,
        bbox=BBox(x0=10.0, y0=y0, x1=580.0, y1=y1),
        text=text,
        raw_text=text,
        char_count=len(text),
        garbled_ratio=0.0,
        extract_method=ExtractMethod.NATIVE_TEXT,
        searchable=searchable,
        block_type=block_type,
        metadata={},
    )


def test_layout_filters_template_and_annotation_noise_but_keeps_signature_candidate() -> None:
    analyzer = LayoutAnalyzer()
    blocks = [
        _make_native_block(1, "n001", "コメントの追加", y0=120.0, y1=140.0, searchable=False),
        _make_native_block(1, "n002", "[A1]", y0=145.0, y1=160.0, searchable=False),
        _make_native_block(1, "n003", "解説編", y0=165.0, y1=180.0, searchable=False),
        _make_native_block(1, "n004", "したひな形です", y0=185.0, y1=200.0, searchable=False),
        _make_native_block(1, "n005", "オプション条項", y0=205.0, y1=220.0, searchable=False),
        _make_native_block(1, "n006", "適宜", y0=225.0, y1=240.0, searchable=False),
        _make_native_block(1, "n007", "参照", y0=245.0, y1=260.0, searchable=False),
        _make_native_block(1, "n008", "00年0月0日", y0=265.0, y1=280.0, searchable=False),
        _make_native_block(1, "n009", "(住所)", y0=285.0, y1=300.0, searchable=False),
        _make_native_block(1, "n010", "(代表者名)", y0=305.0, y1=320.0, searchable=False),
        _make_native_block(1, "n011", "記名押印 甲：株式会社テスト", y0=820.0, y1=860.0, searchable=False),
    ]

    regions = analyzer._weak_native_regions(blocks, page_height=1000.0, repeated_margin_texts=set())

    assert [region.source_block_id for region in regions] == ["n011"]


def test_layout_repeated_margin_text_detection() -> None:
    page_map = {
        1: [
            _make_native_block(1, "p1_h", "機密文書", y0=10.0, y1=25.0, searchable=True),
            _make_native_block(1, "p1_b", "本文テキスト", y0=500.0, y1=530.0, searchable=True),
        ],
        2: [
            _make_native_block(2, "p2_h", "機密文書", y0=12.0, y1=26.0, searchable=True),
            _make_native_block(2, "p2_b", "別の本文", y0=510.0, y1=540.0, searchable=True),
        ],
    }

    repeated = LayoutAnalyzer._collect_repeated_margin_texts(page_map)

    assert "機密文書" in repeated


def test_infer_block_type_keeps_article_heading_and_marks_annotation() -> None:
    article = infer_block_type("第1条（目的）", BBox(10.0, 20.0, 200.0, 40.0), page_height=1000.0)
    annotation = infer_block_type("[A1]", BBox(10.0, 30.0, 100.0, 50.0), page_height=1000.0)

    assert article == BlockType.TEXT
    assert annotation == BlockType.OTHER


def test_layout_does_not_overclassify_footer_body_continuation() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="本契約に関する紛争は当事者間で誠実に協議して解決する。",
        bbox=BBox(10.0, 930.0, 580.0, 980.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )

    assert role == "body"


def test_layout_classifies_short_isolated_fragment_as_annotation() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="参照",
        bbox=BBox(50.0, 420.0, 120.0, 445.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )

    assert role == "annotation"


def test_layout_does_not_overclassify_top_continuation_as_header() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="前ページからの続きとして、本条の条件を次のとおり定める。",
        bbox=BBox(10.0, 32.0, 580.0, 66.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )

    assert role == "body"


def test_layout_does_not_overclassify_bottom_body_with_party_tokens_as_signature() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="甲および乙は本契約に基づき誠実に協議して解決するものとする。",
        bbox=BBox(10.0, 760.0, 580.0, 812.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )
    inferred = infer_block_type(
        "甲および乙は本契約に基づき誠実に協議して解決するものとする。",
        BBox(10.0, 760.0, 580.0, 812.0),
        page_height=1000.0,
    )

    assert role == "body"
    assert inferred == BlockType.TEXT


def test_layout_bottom_representative_sentence_is_not_signature_without_signature_cues() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="代表者は本契約に基づく通知を受領し、速やかに相手方へ連絡するものとする。",
        bbox=BBox(10.0, 770.0, 580.0, 820.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )
    inferred = infer_block_type(
        "代表者は本契約に基づく通知を受領し、速やかに相手方へ連絡するものとする。",
        BBox(10.0, 770.0, 580.0, 820.0),
        page_height=1000.0,
    )

    assert role == "body"
    assert inferred == BlockType.TEXT


def test_layout_top_continuation_sentence_without_period_is_not_header() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="本契約に基づき当事者は誠実に協議し解決するものとする",
        bbox=BBox(10.0, 28.0, 580.0, 58.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )

    assert role == "body"


def test_layout_bottom_sentence_with_signature_word_is_not_signature_area() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="当事者は別紙の署名手続について協議し必要書類を提出するものとする。",
        bbox=BBox(10.0, 780.0, 580.0, 832.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )
    inferred = infer_block_type(
        "当事者は別紙の署名手続について協議し必要書類を提出するものとする。",
        BBox(10.0, 780.0, 580.0, 832.0),
        page_height=1000.0,
    )

    assert role == "body"
    assert inferred == BlockType.TEXT


def test_layout_drops_symbol_heavy_short_fragment_from_body_candidates() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="A1]/#",
        bbox=BBox(10.0, 420.0, 120.0, 440.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )
    inferred = infer_block_type("A1]/#", BBox(10.0, 420.0, 120.0, 440.0), page_height=1000.0)

    assert role == "annotation"
    assert inferred == BlockType.OTHER


def test_layout_top_continuation_is_rescued_by_neighbor_geometry_context() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="本契約に準拠するものとし",
        bbox=BBox(12.0, 30.0, 560.0, 52.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
        next_text="乙はこれを遵守するものとする。",
        next_bbox=BBox(13.0, 55.0, 562.0, 79.0),
    )

    assert role == "body"


def test_layout_bottom_continuation_is_rescued_by_neighbor_geometry_context() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="本契約に準拠するものとし",
        bbox=BBox(12.0, 914.0, 560.0, 938.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
        prev_text="甲は法令を遵守し信義に従い行動するものとする。",
        prev_bbox=BBox(10.0, 886.0, 559.0, 910.0),
    )

    assert role == "body"


def test_layout_drops_particle_started_truncated_fragment() -> None:
    role = LayoutAnalyzer._classify_text_role(
        text="に準拠し",
        bbox=BBox(10.0, 420.0, 120.0, 440.0),
        page_height=1000.0,
        repeated_margin_texts=set(),
    )
    inferred = infer_block_type("に準拠し", BBox(10.0, 420.0, 120.0, 440.0), page_height=1000.0)

    assert role == "annotation"
    assert inferred == BlockType.OTHER


def test_layout_repeated_right_side_note_text_is_not_body() -> None:
    page_map = {
        1: [
            NativeTextBlock(
                page=1,
                block_id="p1_r",
                bbox=BBox(x0=505.0, y0=320.0, x1=580.0, y1=340.0),
                text="オプション条項",
                raw_text="オプション条項",
                char_count=len("オプション条項"),
                garbled_ratio=0.0,
                extract_method=ExtractMethod.NATIVE_TEXT,
                searchable=False,
                block_type=BlockType.TEXT,
                metadata={},
            ),
            _make_native_block(1, "p1_b", "本文", y0=500.0, y1=525.0, searchable=True),
        ],
        2: [
            NativeTextBlock(
                page=2,
                block_id="p2_r",
                bbox=BBox(x0=507.0, y0=322.0, x1=580.0, y1=342.0),
                text="オプション条項",
                raw_text="オプション条項",
                char_count=len("オプション条項"),
                garbled_ratio=0.0,
                extract_method=ExtractMethod.NATIVE_TEXT,
                searchable=False,
                block_type=BlockType.TEXT,
                metadata={},
            ),
            _make_native_block(2, "p2_b", "本文", y0=510.0, y1=535.0, searchable=True),
        ],
    }

    repeated = LayoutAnalyzer._collect_repeated_margin_texts(page_map)
    role = LayoutAnalyzer._classify_text_role(
        text="オプション条項",
        bbox=BBox(560.0, 320.0, 690.0, 340.0),
        page_height=1000.0,
        repeated_margin_texts=repeated,
    )

    assert "オプション条項" in repeated
    assert role in {"annotation", "header_footer"}
