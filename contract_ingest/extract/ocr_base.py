from __future__ import annotations

from collections.abc import Sequence

from contract_ingest.domain.models import OCRExtractionResult, OCRRequest


class OCREngineError(RuntimeError):
    """Base exception for OCR adapter failures."""


class OCRInitializationError(OCREngineError):
    """Raised when OCR engine bootstrap fails."""


class OCRProcessingError(OCREngineError):
    """Raised when OCR processing fails during extraction."""


class OCREngineAdapter:
    @property
    def engine_name(self) -> str:
        return "unknown"

    def extract_regions(self, requests: Sequence[OCRRequest]) -> OCRExtractionResult:
        raise OCRProcessingError("extract_regions must be implemented by concrete OCR adapter")
