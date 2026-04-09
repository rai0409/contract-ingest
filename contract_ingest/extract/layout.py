from __future__ import annotations

from pathlib import Path
import re

import fitz

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, DocumentKind, ErrorSeverity, ReasonCode
from contract_ingest.domain.models import (
    BBox,
    LayoutAnalysisResult,
    LayoutRegion,
    NativeExtractionResult,
    OCRRequest,
    PDFClassificationResult,
    PageLayoutDecision,
    ProcessingIssue,
)
from contract_ingest.utils.image import clip_bbox_to_rect, crop_image_by_pdf_bbox, render_page_to_array
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import (
    is_annotation_like_text,
    is_article_heading_text,
    is_fragment_like_text,
    is_noise_text,
    is_page_number_text,
    normalize_text,
)

_APPENDIX_HEADING_RE = re.compile(
    r"^\s*(?:別紙|別表|別添|付属資料|仕様書)"
    r"(?:\s*[0-9０-９一二三四五六七八九十百千〇零]+)?"
    r"(?:\s|$|[:：（(])"
)


class LayoutAnalyzerError(RuntimeError):
    """Raised when layout analysis cannot continue."""


class LayoutAnalyzer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)

    def analyze(
        self,
        pdf_path: Path,
        classification: PDFClassificationResult,
        native_result: NativeExtractionResult,
    ) -> LayoutAnalysisResult:
        if not pdf_path.exists():
            raise LayoutAnalyzerError(f"PDF not found: {pdf_path}")

        page_to_native = {page.page: page for page in native_result.pages}
        blocks_by_page: dict[int, list] = {}
        for block in native_result.blocks:
            blocks_by_page.setdefault(block.page, []).append(block)
        repeated_margin_texts = self._collect_repeated_margin_texts(blocks_by_page)

        issues: list[ProcessingIssue] = []
        decisions: list[PageLayoutDecision] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            raise LayoutAnalyzerError(f"Failed to open PDF: {pdf_path}") from exc

        try:
            for page_cls in classification.pages:
                page_no = page_cls.page
                page = doc.load_page(page_no - 1)
                page_rect = page.rect
                page_area = max(page_rect.width * page_rect.height, 1.0)

                native_blocks = blocks_by_page.get(page_no, [])
                regions = self._weak_native_regions(
                    native_blocks=native_blocks,
                    page_height=float(page.rect.height),
                    repeated_margin_texts=repeated_margin_texts,
                )
                regions.extend(self._image_regions(page=page, page_no=page_no))

                if page_cls.page_kind == DocumentKind.SCANNED and not regions:
                    regions.append(
                        LayoutRegion(
                            page=page_no,
                            region_id=f"p{page_no}_full_scan",
                            bbox=BBox(
                                x0=page_rect.x0,
                                y0=page_rect.y0,
                                x1=page_rect.x1,
                                y1=page_rect.y1,
                            ),
                            reason="scanned_page_full_ocr",
                            source_block_id=None,
                            priority=10,
                            is_image_region=True,
                        )
                    )

                deduped = self._dedupe_regions(regions)
                ocr_area = sum(region.bbox.area for region in deduped)
                ocr_ratio = min(1.0, ocr_area / page_area)

                native_page = page_to_native.get(page_no)
                native_chars = native_page.native_text_char_count if native_page else 0
                native_sufficient = (
                    native_chars >= self.settings.min_native_text_chars
                    and ocr_ratio <= 0.10
                    and page_cls.page_kind == DocumentKind.TEXT_NATIVE
                )

                reason = page_cls.classification_reason
                if deduped:
                    reason = f"{reason};ocr_regions={len(deduped)}"

                decisions.append(
                    PageLayoutDecision(
                        page=page_no,
                        page_kind=page_cls.page_kind,
                        native_sufficient=native_sufficient,
                        classification_reason=reason,
                        ocr_ratio=ocr_ratio,
                        ocr_regions=deduped,
                    )
                )

                if ocr_ratio >= self.settings.high_ocr_ratio_threshold:
                    issues.append(
                        ProcessingIssue(
                            severity=ErrorSeverity.REVIEW,
                            reason_code=ReasonCode.HIGH_OCR_RATIO,
                            message="high OCR ratio detected on page",
                            page=page_no,
                            details={"ocr_ratio": ocr_ratio},
                        )
                    )
        except Exception as exc:
            raise LayoutAnalyzerError("Failed while analyzing page layout") from exc
        finally:
            doc.close()

        return LayoutAnalysisResult(pages=decisions, issues=issues)

    def build_ocr_requests(
        self,
        pdf_path: Path,
        layout_result: LayoutAnalysisResult,
    ) -> tuple[list[OCRRequest], list[ProcessingIssue]]:
        if not pdf_path.exists():
            raise LayoutAnalyzerError(f"PDF not found: {pdf_path}")

        requests: list[OCRRequest] = []
        issues: list[ProcessingIssue] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            raise LayoutAnalyzerError(f"Failed to open PDF: {pdf_path}") from exc

        try:
            for page_layout in layout_result.pages:
                if not page_layout.ocr_regions:
                    continue

                page = doc.load_page(page_layout.page - 1)
                page_image = render_page_to_array(page=page, dpi=self.settings.render_dpi)

                for region in page_layout.ocr_regions:
                    try:
                        clipped = clip_bbox_to_rect(region.bbox, page.rect)
                        crop = crop_image_by_pdf_bbox(
                            page_image=page_image,
                            page_rect=page.rect,
                            bbox=clipped,
                        )
                    except Exception as exc:
                        issues.append(
                            ProcessingIssue(
                                severity=ErrorSeverity.RECOVERABLE,
                                reason_code=ReasonCode.OCR_FAILURE,
                                message="failed to build OCR region crop",
                                page=region.page,
                                block_id=region.source_block_id,
                                details={"region_id": region.region_id, "error": str(exc)},
                            )
                        )
                        continue

                    requests.append(
                        OCRRequest(
                            page=region.page,
                            region_id=region.region_id,
                            bbox=clipped,
                            image=crop,
                        )
                    )
        finally:
            doc.close()

        return requests, issues

    def _weak_native_regions(
        self,
        native_blocks: list,
        page_height: float,
        repeated_margin_texts: set[str],
    ) -> list[LayoutRegion]:
        regions: list[LayoutRegion] = []
        ordered_blocks = sorted(native_blocks, key=lambda b: (b.bbox.y0, b.bbox.x0))
        for idx, block in enumerate(ordered_blocks):
            prev_block = ordered_blocks[idx - 1] if idx > 0 else None
            next_block = ordered_blocks[idx + 1] if idx + 1 < len(ordered_blocks) else None
            text_role = self._classify_text_role(
                text=block.text,
                bbox=block.bbox,
                page_height=page_height,
                repeated_margin_texts=repeated_margin_texts,
                prev_text=prev_block.text if prev_block is not None else None,
                prev_bbox=prev_block.bbox if prev_block is not None else None,
                next_text=next_block.text if next_block is not None else None,
                next_bbox=next_block.bbox if next_block is not None else None,
            )
            weak = (
                block.char_count < self.settings.min_block_text_chars
                or block.garbled_ratio > self.settings.max_garbled_ratio
                or not block.searchable
                or is_noise_text(block.text)
            )
            if not weak:
                continue
            if text_role in {"annotation", "header_footer"}:
                continue

            reason = "weak_native_text"
            if block.char_count == 0:
                reason = "empty_native_text"
            elif block.garbled_ratio > self.settings.max_garbled_ratio:
                reason = "garbled_native_text"
            elif text_role == "signature":
                reason = "weak_signature_text"

            regions.append(
                LayoutRegion(
                    page=block.page,
                    region_id=f"{block.block_id}_fallback",
                    bbox=block.bbox,
                    reason=reason,
                    source_block_id=block.block_id,
                    priority=20,
                    is_image_region=False,
                )
            )
        return regions

    @staticmethod
    def _collect_repeated_margin_texts(blocks_by_page: dict[int, list]) -> set[str]:
        page_max_y: dict[int, float] = {}
        page_max_x: dict[int, float] = {}
        for page_no, blocks in blocks_by_page.items():
            if not blocks:
                continue
            page_max_y[page_no] = max(float(block.bbox.y1) for block in blocks)
            page_max_x[page_no] = max(float(block.bbox.x1) for block in blocks)

        counts: dict[str, set[int]] = {}
        for page_no, blocks in blocks_by_page.items():
            page_height = page_max_y.get(page_no, 0.0)
            page_width = page_max_x.get(page_no, 0.0)
            if page_height <= 0.0:
                continue
            for block in blocks:
                text = normalize_text(block.text)
                if not text or len(text) > 80 or is_article_heading_text(text):
                    continue
                in_margin = block.bbox.y1 <= page_height * 0.12 or block.bbox.y0 >= page_height * 0.88
                right_side_note = (
                    page_width > 0.0
                    and block.bbox.x0 >= page_width * 0.82
                    and block.bbox.width <= page_width * 0.24
                    and len(text) <= 40
                    and not LayoutAnalyzer._looks_like_sentence_text(text)
                )
                if not in_margin and not right_side_note:
                    continue
                counts.setdefault(text, set()).add(page_no)

        return {text for text, pages in counts.items() if len(pages) >= 2}

    @staticmethod
    def _classify_text_role(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
        prev_text: str | None = None,
        prev_bbox: BBox | None = None,
        next_text: str | None = None,
        next_bbox: BBox | None = None,
    ) -> str:
        normalized = normalize_text(text)
        if not normalized:
            return "annotation"

        structural_heading_role = LayoutAnalyzer._classify_structural_heading_role(normalized)
        if structural_heading_role is not None:
            return structural_heading_role

        margin_role = LayoutAnalyzer._classify_margin_noise_pre_body_role(
            text=normalized,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
        )
        if margin_role is not None:
            return margin_role

        body_rescue_role = LayoutAnalyzer._classify_body_rescue_role(
            text=normalized,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
            prev_text=prev_text,
            prev_bbox=prev_bbox,
            next_text=next_text,
            next_bbox=next_bbox,
        )
        if body_rescue_role is not None:
            return body_rescue_role

        header_footer_role = LayoutAnalyzer._classify_margin_noise_post_body_role(
            text=normalized,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
        )
        if header_footer_role is not None:
            return header_footer_role

        if LayoutAnalyzer._is_low_value_fragment_text(normalized):
            return "annotation"

        structural_non_body_role = LayoutAnalyzer._classify_structural_non_body_role(
            text=normalized,
            bbox=bbox,
            page_height=page_height,
        )
        if structural_non_body_role is not None:
            return structural_non_body_role

        if (
            len(normalized) <= 8
            and not LayoutAnalyzer._looks_like_sentence_text(normalized)
            and not any(token in normalized for token in ["契約", "条", "甲", "乙", "法", "裁判所"])
        ):
            return "annotation"
        symbol_like = sum(1 for ch in normalized if ch in "[]{}<>|/\\*#@")
        if (
            len(normalized) <= 14
            and symbol_like >= 2
            and not any(token in normalized for token in ["契約", "条", "甲", "乙", "法"])
        ):
            return "annotation"
        if is_fragment_like_text(normalized) and not any(
            token in normalized for token in ["甲", "乙", "株式会社", "合同会社", "有限会社", "第", "条"]
        ):
            return "annotation"
        return "body"

    @staticmethod
    def _classify_structural_heading_role(text: str) -> str | None:
        if is_article_heading_text(text):
            return "body"
        if LayoutAnalyzer._is_appendix_heading(text):
            return "body"
        return None

    @staticmethod
    def _classify_margin_noise_pre_body_role(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
    ) -> str | None:
        if is_annotation_like_text(text):
            return "annotation"
        if LayoutAnalyzer._looks_like_annotation_column(text, bbox=bbox, page_height=page_height):
            return "annotation"
        if is_page_number_text(text):
            return "header_footer"
        if text in repeated_margin_texts and bbox.x0 >= 420.0 and bbox.width <= 180.0:
            return "annotation"
        return None

    @staticmethod
    def _classify_body_rescue_role(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
        prev_text: str | None,
        prev_bbox: BBox | None,
        next_text: str | None,
        next_bbox: BBox | None,
    ) -> str | None:
        if LayoutAnalyzer._looks_like_continuation_with_context(
            text=text,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
            prev_text=prev_text,
            prev_bbox=prev_bbox,
            next_text=next_text,
            next_bbox=next_bbox,
        ):
            return "body"
        if LayoutAnalyzer._looks_like_short_critical_clause_line(text):
            return "body"
        if len(text) >= 18 and LayoutAnalyzer._looks_like_sentence_text(text):
            return "body"
        return None

    @staticmethod
    def _classify_margin_noise_post_body_role(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
    ) -> str | None:
        if LayoutAnalyzer._looks_like_header_footer_block(
            text,
            bbox=bbox,
            page_height=page_height,
            repeated_margin_texts=repeated_margin_texts,
        ):
            return "header_footer"
        return None

    @staticmethod
    def _classify_structural_non_body_role(text: str, bbox: BBox, page_height: float) -> str | None:
        if LayoutAnalyzer._is_signature_like_text(text, bbox, page_height):
            return "signature"
        if LayoutAnalyzer._is_table_like_block(text, bbox=bbox, page_height=page_height):
            return "annotation"
        return None

    @staticmethod
    def _looks_like_sentence_text(text: str) -> bool:
        if len(text) >= 16 and any(
            token in text for token in ["。", "、", "ます", "する", "した", "して", "とする", "もの", "こと", "本契約"]
        ):
            return True
        hiragana_count = sum(1 for ch in text if "\u3040" <= ch <= "\u309F")
        particles = sum(1 for token in ["は", "が", "を", "に", "で", "と", "の", "へ", "から", "より"] if token in text)
        if len(text) >= 14 and particles >= 2 and hiragana_count >= 3:
            return True
        if len(text) >= 16 and hiragana_count >= 5:
            return True
        lower_text = text.lower()
        english_words = re.findall(r"[a-z]+", lower_text)
        english_markers = (
            "agreement",
            "governed",
            "effective",
            "execution",
            "signature",
            "date",
            "laws",
            "jurisdiction",
            "construed",
            "entered",
            "commencing",
        )
        if len(english_words) >= 4 and any(marker in lower_text for marker in english_markers):
            return True
        if len(english_words) >= 6 and lower_text.endswith((".", ";")):
            return True
        return len(text) >= 24 and hiragana_count >= 3

    @staticmethod
    def _looks_like_annotation_column(text: str, bbox: BBox, page_height: float) -> bool:
        if not text:
            return False
        if LayoutAnalyzer._looks_like_sentence_text(text):
            return False
        if page_height <= 0:
            return False
        narrow_column = bbox.width <= 170.0
        right_side = bbox.x0 >= 420.0
        shortish = len(text) <= 24
        annotation_token = any(token in text for token in ["注", "備考", "※", "参照", "コメント", "A1", "A2"])
        marker_prefix = bool(re.match(r"^\s*(?:\[[A-Za-z0-9]{1,4}\]|[※＊*・\-])", text))
        return right_side and narrow_column and shortish and (annotation_token or marker_prefix)

    @staticmethod
    def _looks_like_header_footer_block(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
    ) -> bool:
        if not text:
            return False
        if text in repeated_margin_texts:
            return True
        if is_page_number_text(text):
            return True
        if LayoutAnalyzer._looks_like_sentence_text(text):
            return False
        if (
            bbox.y1 <= page_height * 0.06
            and len(text) <= 24
            and not any(token in text for token in ["契約", "条", "甲", "乙"])
        ):
            return True
        if (
            bbox.y0 >= page_height * 0.96
            and len(text) <= 24
            and not any(token in text for token in ["契約", "条", "甲", "乙"])
        ):
            return True
        return False

    @staticmethod
    def _is_table_like_block(text: str, bbox: BBox, page_height: float) -> bool:
        if not text:
            return False
        if "|" in text or "\t" in text:
            return True
        if bbox.width >= 240.0:
            spaced_tokens = [part for part in text.split(" ") if part]
            if len(spaced_tokens) >= 3:
                short_cells = sum(1 for part in spaced_tokens if len(part) <= 8)
                if short_cells >= 3 and not LayoutAnalyzer._looks_like_sentence_text(text):
                    return True
        if "  " in text and len(text) <= 80:
            return True
        if page_height <= 0:
            return False
        segments = [part for part in re.split(r"[ \t]{2,}", text) if part]
        if len(segments) >= 3 and len(text) <= 90:
            short_cells = sum(1 for part in segments if len(part) <= 8)
            if short_cells >= 2:
                return True
        numeric_like = sum(1 for ch in text if ch.isdigit() or ch in ",./:%")
        if len(text) >= 8 and numeric_like / max(len(text), 1) >= 0.45 and bbox.width > 260.0:
            return True
        return False

    @staticmethod
    def _is_appendix_heading(text: str) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        return bool(_APPENDIX_HEADING_RE.match(normalized))

    @staticmethod
    def _looks_like_continuation_with_context(
        text: str,
        bbox: BBox,
        page_height: float,
        repeated_margin_texts: set[str],
        prev_text: str | None,
        prev_bbox: BBox | None,
        next_text: str | None,
        next_bbox: BBox | None,
    ) -> bool:
        is_short_critical = LayoutAnalyzer._looks_like_short_critical_clause_line(text)
        sentence_like = LayoutAnalyzer._looks_like_sentence_text(text)
        starts_with_particle = text[0] in {"の", "て", "に", "を", "が", "で", "と", "も"}
        ends_with_fragment = text[-1] in {"の", "て", "に", "を", "が", "で", "と", "、", "，", "-", "〜", "~", "し", "る"}
        continuation_shape = len(text) <= 28 and (starts_with_particle or ends_with_fragment)
        if not is_short_critical and not sentence_like and not continuation_shape:
            return False
        if text in repeated_margin_texts:
            return False
        if LayoutAnalyzer._looks_like_annotation_column(text, bbox=bbox, page_height=page_height):
            return False

        def _compatible_neighbor(candidate_text: str | None, candidate_bbox: BBox | None, gap: float) -> bool:
            if candidate_text is None or candidate_bbox is None:
                return False
            candidate = normalize_text(candidate_text)
            if not candidate:
                return False
            if candidate in repeated_margin_texts:
                return False
            if is_page_number_text(candidate) or is_annotation_like_text(candidate):
                return False
            if LayoutAnalyzer._looks_like_annotation_column(candidate, bbox=candidate_bbox, page_height=page_height):
                return False
            if LayoutAnalyzer._looks_like_header_footer_block(
                candidate,
                bbox=candidate_bbox,
                page_height=page_height,
                repeated_margin_texts=repeated_margin_texts,
            ):
                return False
            candidate_sentence_like = LayoutAnalyzer._looks_like_sentence_text(candidate)
            candidate_short_critical = LayoutAnalyzer._looks_like_short_critical_clause_line(candidate)
            if (
                LayoutAnalyzer._is_signature_like_text(candidate, candidate_bbox, page_height)
                and not candidate_sentence_like
                and not candidate_short_critical
            ):
                return False
            if LayoutAnalyzer._is_low_value_fragment_text(candidate):
                if not candidate_short_critical:
                    return False
            if len(candidate) < 10 and not candidate_sentence_like:
                if not candidate_short_critical:
                    return False
            min_width = min(bbox.width, candidate_bbox.width)
            max_width = max(bbox.width, candidate_bbox.width, 1.0)
            width_ratio = min_width / max_width
            x0_delta = abs(bbox.x0 - candidate_bbox.x0)
            overlap = max(0.0, min(bbox.x1, candidate_bbox.x1) - max(bbox.x0, candidate_bbox.x0))
            overlap_ratio = overlap / max(min_width, 1.0)
            x0_ok = x0_delta <= max(26.0, min_width * 0.16)
            width_ok = width_ratio >= 0.70
            overlap_ok = overlap_ratio >= 0.72
            gap_ok = -4.0 <= gap <= max(64.0, min(bbox.height, candidate_bbox.height) * 2.80)
            return x0_ok and width_ok and overlap_ok and gap_ok

        prev_ok = _compatible_neighbor(prev_text, prev_bbox, bbox.y0 - prev_bbox.y1) if prev_bbox is not None else False
        next_ok = _compatible_neighbor(next_text, next_bbox, next_bbox.y0 - bbox.y1) if next_bbox is not None else False
        if prev_ok and next_ok:
            return True
        if prev_ok or next_ok:
            near_margin = bbox.y1 <= page_height * 0.15 or bbox.y0 >= page_height * 0.84
            return near_margin and (sentence_like or is_short_critical or continuation_shape)
        return False

    @staticmethod
    def _looks_like_short_critical_clause_line(text: str) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        compact = re.sub(r"\s+", "", normalized)
        if len(compact) > 36:
            return False
        if compact[0] in {"の", "て", "に", "を", "が", "で", "と", "も"}:
            return False
        if len(compact) <= 18 and compact[-1] in {"の", "て", "に", "を", "が", "で", "と", "、", "，", "-", "〜", "~"}:
            return False
        if is_annotation_like_text(compact) or is_page_number_text(compact):
            return False
        if any(token in compact for token in ["記名押印", "署名欄", "代表者", "氏名", "住所", "㊞"]):
            return False
        japanese_markers = (
            "準拠法",
            "適用法",
            "日本法",
            "日本国法",
            "管轄",
            "裁判所",
            "契約締結日",
            "効力発生日",
            "発効日",
            "発効",
            "効力",
            "履行",
            "解釈",
        )
        if any(marker in compact for marker in japanese_markers):
            return True
        lower = normalized.lower()
        english_phrases = (
            "governed by",
            "laws of",
            "effective date",
            "effective as of",
            "entered into as of",
            "dated as of",
            "from and after",
            "on and after",
            "execution date",
            "date of execution",
            "date first written above",
            "date of last signature",
        )
        return any(phrase in lower for phrase in english_phrases)

    @staticmethod
    def _is_low_value_fragment_text(text: str) -> bool:
        if not text:
            return True
        if is_article_heading_text(text):
            return False
        if text in {"甲", "乙"}:
            return False
        legal_markers = (
            "契約",
            "条",
            "法",
            "裁判所",
            "甲",
            "乙",
            "株式会社",
            "合意",
            "準拠",
            "適用法",
            "日本法",
            "契約締結日",
            "発効",
            "効力",
            "履行",
            "解釈",
        )
        has_legal_marker = any(marker in text for marker in legal_markers)
        symbol_like = sum(1 for ch in text if ch in "[]{}<>|/\\*#@")
        ascii_alnum = sum(1 for ch in text if ch.isascii() and ch.isalnum())
        kana_kanji = sum(1 for ch in text if ("\u3040" <= ch <= "\u30FF") or ("\u4E00" <= ch <= "\u9FFF"))
        katakana = sum(1 for ch in text if "\u30A0" <= ch <= "\u30FF")
        sentence_like = LayoutAnalyzer._looks_like_sentence_text(text)
        starts_with_particle = text[0] in {"の", "て", "に", "を", "が", "で", "と", "も"}
        starts_with_connective = any(
            text.startswith(prefix)
            for prefix in ["及び", "および", "又は", "または", "並びに", "ならびに", "ただし", "但し"]
        )
        ends_with_fragment = text[-1] in {"の", "て", "に", "を", "が", "で", "と", "、", "，", "-", "〜", "~", "し", "る"}
        has_terminal_punct = any(token in text for token in ["。", ".", "；", ";", "！", "!", "？", "?"])
        has_predicate = any(
            token in text for token in ["する", "した", "して", "され", "でき", "ます", "です", "もの", "こと", "とする", "である"]
        )
        ocr_noise_like = len(text) <= 18 and katakana >= 2 and (ascii_alnum + symbol_like) >= 3
        ascii_heavy = len(text) <= 12 and ascii_alnum >= max(3, int(len(text) * 0.6)) and kana_kanji <= 2
        fragment_score = 0
        if starts_with_particle:
            fragment_score += 2
        if starts_with_connective and len(text) <= 18:
            fragment_score += 1
        if ends_with_fragment and not has_terminal_punct:
            fragment_score += 1
        if len(text) <= 18 and not sentence_like:
            fragment_score += 1
        if len(text) <= 16 and not has_predicate:
            fragment_score += 1
        if symbol_like >= 2:
            fragment_score += 1
        if ascii_heavy:
            fragment_score += 1
        if ocr_noise_like:
            fragment_score += 1
        if len(text) <= 10 and symbol_like >= 2:
            return True
        if ascii_heavy:
            return True
        if len(text) <= 8 and symbol_like >= 1 and kana_kanji <= 2:
            return True
        if ocr_noise_like:
            return True
        if has_legal_marker and LayoutAnalyzer._looks_like_short_critical_clause_line(text):
            return False
        if has_legal_marker and len(text) <= 20 and fragment_score >= 3:
            return True
        if fragment_score >= 3 and len(text) <= 20:
            return True
        if fragment_score >= 4 and len(text) <= 28 and not sentence_like:
            return True
        return False

    @staticmethod
    def _is_signature_like_text(text: str, bbox: BBox, page_height: float) -> bool:
        sentence_like = LayoutAnalyzer._looks_like_sentence_text(text)
        if len(text) >= 18 and sentence_like:
            return False
        has_signature_terms = any(token in text for token in ["記名押印", "署名欄", "捺印"])
        has_stamp = "㊞" in text or ("押印" in text and len(text) <= 40)
        has_signature_word = "署名" in text
        if len(text) > 72:
            return False
        near_bottom = page_height >= 700.0 and bbox.y0 >= page_height * 0.72
        if not near_bottom:
            return (has_signature_terms or has_stamp) and len(text) <= 40 and not sentence_like
        has_party = any(token in text for token in ["甲", "乙"])
        has_company = any(token in text for token in ["株式会社", "合同会社", "有限会社"])
        has_label = any(token in text for token in ["住所", "氏名", "代表者", "署名日", "会社名"])
        has_date = "年月日" in text or ("年" in text and "月" in text and "日" in text and len(text) <= 24)
        has_signature_delimiter = any(token in text for token in [":", "：", "㊞", "（", "）", "(", ")"])
        structured_short = len(text) <= 48
        if sentence_like and len(text) >= 14:
            return False
        if (has_signature_terms or has_stamp) and structured_short and not sentence_like:
            return True
        if has_signature_word and len(text) <= 18 and not sentence_like and (has_signature_delimiter or has_label):
            return True
        if has_party and has_company and (has_signature_delimiter or has_date) and structured_short:
            return True
        if has_label and has_signature_delimiter and (has_party or has_company or has_date) and structured_short:
            return True
        cues = int(has_party) + int(has_company) + int(has_label) + int(has_date) + int(has_signature_delimiter)
        if structured_short and not sentence_like and cues >= 4:
            return True
        return False

    @staticmethod
    def _image_regions(page: fitz.Page, page_no: int) -> list[LayoutRegion]:
        page_dict = page.get_text("dict")
        results: list[LayoutRegion] = []
        page_width = float(page.rect.width)
        page_height = float(page.rect.height)
        page_area = max(page_width * page_height, 1.0)
        for idx, block in enumerate(page_dict.get("blocks", []), start=1):
            if block.get("type") != 1:
                continue
            x0, y0, x1, y1 = block.get("bbox", (0.0, 0.0, 0.0, 0.0))
            if x1 <= x0 or y1 <= y0:
                continue
            bbox = BBox(x0=float(x0), y0=float(y0), x1=float(x1), y1=float(y1))
            area_ratio = bbox.area / page_area
            near_header_or_side = (
                bbox.y1 <= page_height * 0.15
                or bbox.x1 <= page_width * 0.12
                or bbox.x0 >= page_width * 0.88
            )
            if area_ratio < 0.0015:
                continue
            if near_header_or_side and area_ratio < 0.0060:
                continue
            results.append(
                LayoutRegion(
                    page=page_no,
                    region_id=f"p{page_no}_img{idx:03d}",
                    bbox=bbox,
                    reason="image_region",
                    source_block_id=None,
                    priority=30,
                    is_image_region=True,
                )
            )
        return results

    @staticmethod
    def _dedupe_regions(regions: list[LayoutRegion]) -> list[LayoutRegion]:
        sorted_regions = sorted(regions, key=lambda r: (r.priority, r.page, r.bbox.y0, r.bbox.x0))
        result: list[LayoutRegion] = []
        for region in sorted_regions:
            duplicate = False
            for existing in result:
                if region.page != existing.page:
                    continue
                if region.bbox.iou(existing.bbox) > 0.90:
                    duplicate = True
                    break
            if not duplicate:
                result.append(region)
        return result



def infer_block_type(text: str, bbox: BBox, page_height: float) -> BlockType:
    normalized = normalize_text(text).strip()
    if not normalized:
        return BlockType.OTHER
    if is_article_heading_text(normalized):
        return BlockType.TEXT
    if is_annotation_like_text(normalized):
        return BlockType.OTHER
    if is_page_number_text(normalized):
        return BlockType.FOOTER
    if LayoutAnalyzer._looks_like_header_footer_block(
        normalized,
        bbox=bbox,
        page_height=page_height,
        repeated_margin_texts=set(),
    ):
        if bbox.y1 <= page_height * 0.10:
            return BlockType.HEADER
        return BlockType.FOOTER
    if LayoutAnalyzer._looks_like_annotation_column(normalized, bbox=bbox, page_height=page_height):
        return BlockType.OTHER
    if LayoutAnalyzer._is_appendix_heading(normalized):
        return BlockType.TEXT
    if LayoutAnalyzer._looks_like_short_critical_clause_line(normalized):
        return BlockType.TEXT
    if len(normalized) >= 18 and LayoutAnalyzer._looks_like_sentence_text(normalized):
        return BlockType.TEXT
    if LayoutAnalyzer._is_low_value_fragment_text(normalized):
        return BlockType.OTHER
    if LayoutAnalyzer._is_signature_like_text(normalized, bbox, page_height):
        return BlockType.SIGNATURE_AREA
    if LayoutAnalyzer._is_table_like_block(normalized, bbox=bbox, page_height=page_height):
        return BlockType.TABLE
    if (
        bbox.y1 <= page_height * 0.08
        and len(normalized) <= 28
        and not LayoutAnalyzer._looks_like_sentence_text(normalized)
    ):
        return BlockType.HEADER
    if (
        bbox.y0 >= page_height * 0.95
        and len(normalized) <= 28
        and not LayoutAnalyzer._looks_like_sentence_text(normalized)
    ):
        return BlockType.FOOTER
    if (
        any(token in normalized for token in ["記名押印", "署名欄"])
        and len(normalized) <= 40
        and not LayoutAnalyzer._looks_like_sentence_text(normalized)
    ):
        return BlockType.SIGNATURE_AREA
    if (
        "署名" in normalized
        and len(normalized) <= 18
        and bbox.y0 >= page_height * 0.70
        and not LayoutAnalyzer._looks_like_sentence_text(normalized)
    ):
        return BlockType.SIGNATURE_AREA
    if any(token in normalized for token in ["印", "捺印", "印影"]):
        return BlockType.STAMP_AREA
    return BlockType.TEXT
