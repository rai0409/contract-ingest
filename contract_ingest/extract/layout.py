from __future__ import annotations

from pathlib import Path

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
from contract_ingest.utils.text import is_noise_text


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
                regions = self._weak_native_regions(native_blocks=native_blocks)
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

    def _weak_native_regions(self, native_blocks: list) -> list[LayoutRegion]:
        regions: list[LayoutRegion] = []
        for block in native_blocks:
            weak = (
                block.char_count < self.settings.min_block_text_chars
                or block.garbled_ratio > self.settings.max_garbled_ratio
                or not block.searchable
                or is_noise_text(block.text)
            )
            if not weak:
                continue

            reason = "weak_native_text"
            if block.char_count == 0:
                reason = "empty_native_text"
            elif block.garbled_ratio > self.settings.max_garbled_ratio:
                reason = "garbled_native_text"

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
    def _image_regions(page: fitz.Page, page_no: int) -> list[LayoutRegion]:
        page_dict = page.get_text("dict")
        results: list[LayoutRegion] = []
        for idx, block in enumerate(page_dict.get("blocks", []), start=1):
            if block.get("type") != 1:
                continue
            x0, y0, x1, y1 = block.get("bbox", (0.0, 0.0, 0.0, 0.0))
            if x1 <= x0 or y1 <= y0:
                continue
            bbox = BBox(x0=float(x0), y0=float(y0), x1=float(x1), y1=float(y1))
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
    normalized = text.strip()
    if not normalized:
        return BlockType.OTHER
    if bbox.y1 <= page_height * 0.10 and len(normalized) <= 60:
        return BlockType.HEADER
    if bbox.y0 >= page_height * 0.90 and len(normalized) <= 60:
        return BlockType.FOOTER
    if any(token in normalized for token in ["署名", "記名押印", "署名欄"]):
        return BlockType.SIGNATURE_AREA
    if any(token in normalized for token in ["印", "捺印", "印影"]):
        return BlockType.STAMP_AREA
    if "|" in normalized or "\t" in normalized or "  " in normalized:
        return BlockType.TABLE
    return BlockType.TEXT
