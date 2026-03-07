from __future__ import annotations

from enum import Enum


class DocumentKind(str, Enum):
    TEXT_NATIVE = "text-native"
    SCANNED = "scanned"
    HYBRID = "hybrid"


class BlockType(str, Enum):
    TEXT = "text"
    TABLE = "table"
    HEADER = "header"
    FOOTER = "footer"
    IMAGE = "image"
    SIGNATURE_AREA = "signature_area"
    STAMP_AREA = "stamp_area"
    OTHER = "other"


class ExtractMethod(str, Enum):
    NATIVE_TEXT = "native_text"
    OCR = "ocr"
    HYBRID = "hybrid"


class ReviewLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ErrorSeverity(str, Enum):
    FATAL = "fatal"
    RECOVERABLE = "recoverable"
    REVIEW = "review"


class ReasonCode(str, Enum):
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    HIGH_OCR_RATIO = "HIGH_OCR_RATIO"
    UNSTABLE_CLAUSE_SPLIT = "UNSTABLE_CLAUSE_SPLIT"
    MISSING_CONTRACT_TYPE = "MISSING_CONTRACT_TYPE"
    MISSING_EFFECTIVE_DATE = "MISSING_EFFECTIVE_DATE"
    MISSING_EXPIRATION_DATE = "MISSING_EXPIRATION_DATE"
    MISSING_JURISDICTION = "MISSING_JURISDICTION"
    MISSING_GOVERNING_LAW = "MISSING_GOVERNING_LAW"
    OCR_FAILURE = "OCR_FAILURE"
    PARTIAL_EXTRACTION_FAILURE = "PARTIAL_EXTRACTION_FAILURE"
    NATIVE_TEXT_GARBLED = "NATIVE_TEXT_GARBLED"
    EMPTY_PAGE = "EMPTY_PAGE"
    PDF_CLASSIFICATION_AMBIGUOUS = "PDF_CLASSIFICATION_AMBIGUOUS"
    REVERSED_CLAUSE_NUMBER = "REVERSED_CLAUSE_NUMBER"
    SHORT_CLAUSE_TEXT = "SHORT_CLAUSE_TEXT"
    CONSECUTIVE_CLAUSE_HEADINGS = "CONSECUTIVE_CLAUSE_HEADINGS"
    APPENDIX_BOUNDARY_AMBIGUOUS = "APPENDIX_BOUNDARY_AMBIGUOUS"


class ChunkType(str, Enum):
    CLAUSE = "clause"
    SCHEDULE = "schedule"
    TABLE = "table"
    PREAMBLE = "preamble"
    APPENDIX = "appendix"
    OTHER = "other"
