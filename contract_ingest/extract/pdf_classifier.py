from __future__ import annotations

from pathlib import Path

import fitz

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import DocumentKind, ErrorSeverity, ReasonCode
from contract_ingest.domain.models import (
    ClassificationMetrics,
    PDFClassificationResult,
    PageClassification,
    ProcessingIssue,
)
from contract_ingest.utils.image import estimate_page_image_coverage
from contract_ingest.utils.logging import get_logger
from contract_ingest.utils.text import garbled_ratio, normalize_text


class PDFClassificationError(RuntimeError):
    """Raised when PDF classification fails fatally."""


class PDFClassifier:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = get_logger(__name__)

    def classify(self, pdf_path: Path) -> PDFClassificationResult:
        if not pdf_path.exists():
            raise PDFClassificationError(f"PDF not found: {pdf_path}")

        warnings: list[ProcessingIssue] = []
        pages: list[PageClassification] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            raise PDFClassificationError(f"Failed to open PDF: {pdf_path}") from exc

        try:
            for page_idx in range(len(doc)):
                page = doc.load_page(page_idx)
                page_no = page_idx + 1
                classified, page_warning = self._classify_page(page=page, page_no=page_no)
                pages.append(classified)
                warnings.extend(page_warning)
        except Exception as exc:
            raise PDFClassificationError("Failed while classifying pages") from exc
        finally:
            doc.close()

        doc_kind = self._classify_document(pages)
        if doc_kind == DocumentKind.HYBRID:
            warnings.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.PDF_CLASSIFICATION_AMBIGUOUS,
                    message="document contains mixed page kinds and is treated as hybrid",
                )
            )

        return PDFClassificationResult(document_kind=doc_kind, pages=pages, warnings=warnings)

    def _classify_page(
        self,
        page: fitz.Page,
        page_no: int,
    ) -> tuple[PageClassification, list[ProcessingIssue]]:
        full_text = normalize_text(page.get_text("text"))
        char_count = len(full_text)
        garble = garbled_ratio(full_text)

        block_tuples = page.get_text("blocks")
        text_blocks = [t for t in block_tuples if len(t) >= 5 and normalize_text(str(t[4]))]
        text_block_count = len(text_blocks)

        image_coverage = estimate_page_image_coverage(page)
        weak_blocks = 0
        for block in text_blocks:
            text = normalize_text(str(block[4]))
            if len(text) < self.settings.min_block_text_chars or garbled_ratio(text) > self.settings.max_garbled_ratio:
                weak_blocks += 1
        ocr_target_ratio = weak_blocks / max(text_block_count, 1)

        if char_count == 0 and image_coverage >= 0.15:
            kind = DocumentKind.SCANNED
            reason = "no_native_text_with_image_regions"
        elif char_count < self.settings.min_native_text_chars and image_coverage >= 0.35:
            kind = DocumentKind.SCANNED
            reason = "insufficient_native_text_and_high_image_coverage"
        elif (
            char_count >= self.settings.min_native_text_chars * 2
            and garble <= self.settings.max_garbled_ratio
            and ocr_target_ratio <= 0.20
        ):
            kind = DocumentKind.TEXT_NATIVE
            reason = "sufficient_native_text"
        else:
            kind = DocumentKind.HYBRID
            reason = "mixed_native_text_and_ocr_signals"

        warnings: list[ProcessingIssue] = []
        if garble > self.settings.max_garbled_ratio:
            warnings.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.NATIVE_TEXT_GARBLED,
                    message="garbled text ratio exceeded threshold during classification",
                    page=page_no,
                    details={"garbled_ratio": garble},
                )
            )

        metrics = ClassificationMetrics(
            native_text_char_count=char_count,
            garbled_ratio=garble,
            image_coverage=image_coverage,
            text_block_count=text_block_count,
            ocr_target_ratio=ocr_target_ratio,
        )

        return (
            PageClassification(
                page=page_no,
                page_kind=kind,
                classification_reason=reason,
                metrics=metrics,
            ),
            warnings,
        )

    @staticmethod
    def _classify_document(pages: list[PageClassification]) -> DocumentKind:
        kinds = {page.page_kind for page in pages}
        if kinds == {DocumentKind.TEXT_NATIVE}:
            return DocumentKind.TEXT_NATIVE
        if kinds == {DocumentKind.SCANNED}:
            return DocumentKind.SCANNED
        return DocumentKind.HYBRID
