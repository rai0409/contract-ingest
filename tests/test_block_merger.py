from __future__ import annotations

from contract_ingest.domain.enums import BlockType, DocumentKind, ExtractMethod
from contract_ingest.domain.models import (
    BBox,
    LayoutAnalysisResult,
    LayoutRegion,
    NativeExtractionResult,
    NativePageMetrics,
    NativeTextBlock,
    OCRBlock,
    OCRExtractionResult,
    PageLayoutDecision,
)
from contract_ingest.extract.block_merger import BlockMerger


def _native_block(
    block_id: str,
    text: str,
    *,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    char_count: int | None = None,
    searchable: bool = True,
) -> NativeTextBlock:
    return NativeTextBlock(
        page=1,
        block_id=block_id,
        bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1),
        text=text,
        raw_text=text,
        char_count=len(text) if char_count is None else char_count,
        garbled_ratio=0.0,
        extract_method=ExtractMethod.NATIVE_TEXT,
        searchable=searchable,
        block_type=BlockType.TEXT,
        metadata={},
    )


def _single_page_native_result(blocks: list[NativeTextBlock]) -> NativeExtractionResult:
    return NativeExtractionResult(
        pages=[
            NativePageMetrics(
                page=1,
                native_text_char_count=sum(block.char_count for block in blocks),
                text_block_count=len(blocks),
                text_coverage=0.4,
                garbled_ratio=0.0,
                empty=not blocks,
            )
        ],
        blocks=blocks,
    )


def _layout_result(regions: list[LayoutRegion]) -> LayoutAnalysisResult:
    return LayoutAnalysisResult(
        pages=[
            PageLayoutDecision(
                page=1,
                page_kind=DocumentKind.HYBRID,
                native_sufficient=False,
                classification_reason="test",
                ocr_ratio=0.5,
                ocr_regions=regions,
            )
        ],
        issues=[],
    )


def test_merger_does_not_replace_weak_native_with_annotation_like_ocr() -> None:
    native = _native_block("p1_n001", "第1条 目的", x0=20.0, y0=100.0, x1=580.0, y1=130.0, char_count=5)
    native_result = _single_page_native_result([native])

    region = LayoutRegion(
        page=1,
        region_id="r001",
        bbox=BBox(20.0, 100.0, 580.0, 130.0),
        reason="weak_native_text",
        source_block_id="p1_n001",
        priority=20,
        is_image_region=False,
    )
    layout_result = _layout_result([region])

    ocr_result = OCRExtractionResult(
        blocks=[
            OCRBlock(
                page=1,
                block_id="p1_o001",
                region_id="r001",
                bbox=BBox(20.0, 100.0, 580.0, 130.0),
                text="[A1]",
                confidence=0.99,
                engine="paddleocr",
                extract_method=ExtractMethod.OCR,
                block_type=BlockType.TEXT,
                searchable=True,
                metadata={},
            )
        ],
        issues=[],
    )

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 1
    assert merged.blocks[0].text == "第1条 目的"


def test_merger_drops_low_conf_short_ocr_fragment_from_body_flow() -> None:
    native = _native_block("p1_n001", "本文テキスト", x0=20.0, y0=100.0, x1=580.0, y1=130.0)
    native_result = _single_page_native_result([native])

    layout_result = _layout_result(
        [
            LayoutRegion(
                page=1,
                region_id="r001",
                bbox=BBox(20.0, 140.0, 120.0, 160.0),
                reason="image_region",
                source_block_id=None,
                priority=30,
                is_image_region=True,
            )
        ]
    )

    ocr_result = OCRExtractionResult(
        blocks=[
            OCRBlock(
                page=1,
                block_id="p1_o001",
                region_id="r001",
                bbox=BBox(20.0, 140.0, 120.0, 160.0),
                text="参照",
                confidence=0.60,
                engine="paddleocr",
                extract_method=ExtractMethod.OCR,
                block_type=BlockType.TEXT,
                searchable=True,
                metadata={},
            )
        ],
        issues=[],
    )

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 1
    assert merged.blocks[0].text == "本文テキスト"


def test_merger_preserves_signature_block_and_drops_annotation_ocr_blocks() -> None:
    native_result = _single_page_native_result([])

    layout_result = _layout_result(
        [
            LayoutRegion(
                page=1,
                region_id="r_sig",
                bbox=BBox(20.0, 700.0, 580.0, 760.0),
                reason="image_region",
                source_block_id=None,
                priority=30,
                is_image_region=True,
            ),
            LayoutRegion(
                page=1,
                region_id="r_ann",
                bbox=BBox(20.0, 200.0, 280.0, 240.0),
                reason="image_region",
                source_block_id=None,
                priority=30,
                is_image_region=True,
            ),
        ]
    )

    ocr_result = OCRExtractionResult(
        blocks=[
            OCRBlock(
                page=1,
                block_id="p1_o_sig",
                region_id="r_sig",
                bbox=BBox(20.0, 700.0, 580.0, 760.0),
                text="記名押印 乙：株式会社サンプル",
                confidence=0.90,
                engine="paddleocr",
                extract_method=ExtractMethod.OCR,
                block_type=BlockType.TEXT,
                searchable=True,
                metadata={},
            ),
            OCRBlock(
                page=1,
                block_id="p1_o_ann",
                region_id="r_ann",
                bbox=BBox(20.0, 200.0, 280.0, 240.0),
                text="(住所)",
                confidence=0.95,
                engine="paddleocr",
                extract_method=ExtractMethod.OCR,
                block_type=BlockType.TEXT,
                searchable=True,
                metadata={},
            ),
        ],
        issues=[],
    )

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 1
    assert merged.blocks[0].block_type == BlockType.SIGNATURE_AREA
    assert merged.blocks[0].searchable is False
    assert "記名押印" in merged.blocks[0].text


def test_merger_merges_only_compatible_body_blocks() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "本契約の目的は次のとおりとする", x0=20.0, y0=100.0, x1=560.0, y1=125.0),
            _native_block("p1_n002", "甲は乙に業務を委託する", x0=22.0, y0=129.0, x1=562.0, y1=154.0),
            _native_block("p1_n003", "別列テキスト", x0=300.0, y0=158.0, x1=560.0, y1=183.0),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 2
    assert "本契約の目的は次のとおりとする\n甲は乙に業務を委託する" == merged.blocks[0].text
    assert merged.blocks[1].text == "別列テキスト"


def test_can_merge_body_rejects_annotation_signature_header_and_short_ocr_noise() -> None:
    prev = {
        "bbox": BBox(20.0, 100.0, 560.0, 125.0),
        "text": "本契約の目的は次のとおりとする",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "body",
        "block_type": BlockType.TEXT,
        "confidence": 0.96,
    }

    short_ocr_noise = {
        "bbox": BBox(21.0, 128.0, 180.0, 150.0),
        "text": "参照",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "body",
        "block_type": BlockType.TEXT,
        "confidence": 0.98,
    }
    annotation_like = {
        "bbox": BBox(21.0, 128.0, 300.0, 150.0),
        "text": "コメントの追加",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "annotation",
        "block_type": BlockType.OTHER,
        "confidence": 0.98,
    }
    signature_like = {
        "bbox": BBox(21.0, 128.0, 560.0, 165.0),
        "text": "記名押印 甲：株式会社テスト",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "signature",
        "block_type": BlockType.SIGNATURE_AREA,
        "confidence": 0.98,
    }
    header_like = {
        "bbox": BBox(21.0, 128.0, 180.0, 150.0),
        "text": "1/5",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "header_footer",
        "block_type": BlockType.HEADER,
        "confidence": 0.98,
    }
    footer_type_mismatch = {
        "bbox": BBox(21.0, 128.0, 560.0, 150.0),
        "text": "本契約に関する通知は書面による。",
        "extract_method": ExtractMethod.OCR,
        "candidate_kind": "body",
        "block_type": BlockType.FOOTER,
        "confidence": 0.98,
    }

    assert BlockMerger._can_merge_body(prev, short_ocr_noise) is False
    assert BlockMerger._can_merge_body(prev, annotation_like) is False
    assert BlockMerger._can_merge_body(prev, signature_like) is False
    assert BlockMerger._can_merge_body(prev, header_like) is False
    assert BlockMerger._can_merge_body(prev, footer_type_mismatch) is False


def test_low_value_ocr_fragment_rejects_entity_type_only_and_zero_length_noise_but_keeps_legal_markers() -> None:
    assert BlockMerger._is_low_value_ocr_fragment("0年間", 0.99) is True
    assert BlockMerger._is_low_value_ocr_fragment("O年間", 0.99) is True
    assert BlockMerger._is_low_value_ocr_fragment("公益財団法人", 0.99) is True
    assert BlockMerger._is_low_value_ocr_fragment("国立大学法人", 0.99) is True
    assert BlockMerger._is_low_value_ocr_fragment("日本法", 0.99) is False
    assert BlockMerger._is_low_value_ocr_fragment("契約締結日", 0.99) is False


def test_classify_candidate_kind_does_not_mark_bottom_body_as_signature_without_composite_cues() -> None:
    kind = BlockMerger._classify_candidate_kind(
        text="代表者は本契約に基づく通知を受領し相手方へ連絡するものとする。",
        bbox=BBox(20.0, 760.0, 580.0, 810.0),
        page_height=1000.0,
        block_type=BlockType.TEXT,
        repeated_margin_texts=set(),
    )

    assert kind == "body"


def test_classify_candidate_kind_keeps_english_effective_phrase_as_body() -> None:
    kind = BlockMerger._classify_candidate_kind(
        text="as of the date first written above",
        bbox=BBox(20.0, 920.0, 580.0, 948.0),
        page_height=1000.0,
        block_type=BlockType.TEXT,
        repeated_margin_texts=set(),
    )

    assert kind == "body"


def test_merger_keeps_annotation_block_separate_from_body() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "本契約に関する通知は書面で行うものとする。", x0=20.0, y0=100.0, x1=560.0, y1=126.0),
            _native_block("p1_n002", "[A1]", x0=22.0, y0=128.0, x1=120.0, y1=146.0, searchable=False),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 2
    assert merged.blocks[0].block_type == BlockType.TEXT
    assert merged.blocks[1].block_type == BlockType.OTHER
    assert merged.blocks[1].searchable is False


def test_merger_does_not_merge_table_like_block_into_body() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "本契約に基づく委託料は次のとおりとする。", x0=20.0, y0=100.0, x1=560.0, y1=126.0),
            _native_block("p1_n002", "項目 数量 単価 金額", x0=20.0, y0=128.0, x1=560.0, y1=154.0),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 2
    assert merged.blocks[0].block_type == BlockType.TEXT
    assert merged.blocks[1].block_type == BlockType.TABLE
    assert merged.blocks[1].searchable is False


def test_merger_merges_appendix_heading_with_appendix_text_when_geometry_is_continuous() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "別紙1（仕様書）", x0=20.0, y0=180.0, x1=360.0, y1=205.0),
            _native_block("p1_n002", "別紙1仕様を次のとおり定める。", x0=22.0, y0=208.0, x1=362.0, y1=235.0),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 1
    assert merged.blocks[0].block_type == BlockType.TEXT
    assert "別紙1（仕様書）\n別紙1仕様を次のとおり定める。" == merged.blocks[0].text


def test_merger_does_not_merge_body_with_close_footer_like_block() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "本契約に関する紛争は協議により解決する。", x0=20.0, y0=920.0, x1=560.0, y1=944.0),
            _native_block("p1_n002", "1/5", x0=24.0, y0=946.0, x1=90.0, y1=962.0),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 2
    assert merged.blocks[0].block_type == BlockType.TEXT
    assert merged.blocks[1].block_type == BlockType.FOOTER


def test_merger_does_not_merge_main_body_with_appendix_heading_even_when_geometry_close() -> None:
    native_result = _single_page_native_result(
        [
            _native_block("p1_n001", "第8条 本契約の条件を定める。", x0=20.0, y0=520.0, x1=560.0, y1=544.0),
            _native_block("p1_n002", "別紙1 仕様書", x0=21.0, y0=546.0, x1=560.0, y1=570.0),
        ]
    )
    layout_result = _layout_result([])
    ocr_result = OCRExtractionResult(blocks=[], issues=[])

    merged = BlockMerger().merge(native_result=native_result, layout_result=layout_result, ocr_result=ocr_result)

    assert len(merged.blocks) == 2
    assert merged.blocks[0].section_type.value == "main_contract"
    assert merged.blocks[1].section_type.value == "appendix"
