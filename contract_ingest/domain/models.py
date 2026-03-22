from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from contract_ingest.domain.enums import (
    BlockType,
    DocumentKind,
    ErrorSeverity,
    ExtractMethod,
    ReasonCode,
    ReviewLevel,
    SectionType,
)


@dataclass(frozen=True)
class BBox:
    x0: float
    y0: float
    x1: float
    y1: float

    def __post_init__(self) -> None:
        if self.x0 >= self.x1:
            raise ValueError(f"Invalid bbox x range: x0={self.x0}, x1={self.x1}")
        if self.y0 >= self.y1:
            raise ValueError(f"Invalid bbox y range: y0={self.y0}, y1={self.y1}")

    @property
    def width(self) -> float:
        return self.x1 - self.x0

    @property
    def height(self) -> float:
        return self.y1 - self.y0

    @property
    def area(self) -> float:
        return self.width * self.height

    def to_dict(self) -> dict[str, float]:
        return {"x0": self.x0, "y0": self.y0, "x1": self.x1, "y1": self.y1}

    def intersection(self, other: "BBox") -> float:
        xx0 = max(self.x0, other.x0)
        yy0 = max(self.y0, other.y0)
        xx1 = min(self.x1, other.x1)
        yy1 = min(self.y1, other.y1)
        if xx1 <= xx0 or yy1 <= yy0:
            return 0.0
        return (xx1 - xx0) * (yy1 - yy0)

    def iou(self, other: "BBox") -> float:
        inter = self.intersection(other)
        if inter <= 0:
            return 0.0
        union = self.area + other.area - inter
        if union <= 0:
            return 0.0
        return inter / union


@dataclass(slots=True)
class ProcessingIssue:
    severity: ErrorSeverity
    reason_code: ReasonCode | str
    message: str
    page: int | None = None
    block_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ClassificationMetrics:
    native_text_char_count: int
    garbled_ratio: float
    image_coverage: float
    text_block_count: int
    ocr_target_ratio: float


@dataclass(frozen=True)
class PageClassification:
    page: int
    page_kind: DocumentKind
    classification_reason: str
    metrics: ClassificationMetrics


@dataclass(frozen=True)
class PDFClassificationResult:
    document_kind: DocumentKind
    pages: list[PageClassification]
    warnings: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class NativeTextBlock:
    page: int
    block_id: str
    bbox: BBox
    text: str
    raw_text: str
    char_count: int
    garbled_ratio: float
    extract_method: ExtractMethod
    searchable: bool
    block_type: BlockType
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NativePageMetrics:
    page: int
    native_text_char_count: int
    text_block_count: int
    text_coverage: float
    garbled_ratio: float
    empty: bool


@dataclass(frozen=True)
class NativeExtractionResult:
    pages: list[NativePageMetrics]
    blocks: list[NativeTextBlock]
    warnings: list[ProcessingIssue] = field(default_factory=list)
    errors: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class OCRRequest:
    page: int
    region_id: str
    bbox: BBox
    image: np.ndarray


@dataclass(frozen=True)
class OCRBlock:
    page: int
    block_id: str
    region_id: str
    bbox: BBox
    text: str
    confidence: float | None
    engine: str
    extract_method: ExtractMethod
    block_type: BlockType
    searchable: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OCRExtractionResult:
    blocks: list[OCRBlock]
    issues: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class LayoutRegion:
    page: int
    region_id: str
    bbox: BBox
    reason: str
    source_block_id: str | None = None
    priority: int = 100
    is_image_region: bool = False


@dataclass(frozen=True)
class PageLayoutDecision:
    page: int
    page_kind: DocumentKind
    native_sufficient: bool
    classification_reason: str
    ocr_ratio: float
    ocr_regions: list[LayoutRegion]


@dataclass(frozen=True)
class LayoutAnalysisResult:
    pages: list[PageLayoutDecision]
    issues: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class UnifiedBlock:
    page: int
    block_id: str
    block_type: BlockType
    bbox: BBox
    text: str
    engine: str
    extract_method: ExtractMethod
    confidence: float | None
    searchable: bool
    reading_order: int
    source_block_ids: list[str]
    adoption_reason: str
    section_type: SectionType = SectionType.MAIN_CONTRACT


@dataclass(frozen=True)
class MergedPage:
    page: int
    page_kind: DocumentKind
    native_text_char_count: int
    ocr_ratio: float
    classification_reason: str


@dataclass(frozen=True)
class MergeResult:
    pages: list[MergedPage]
    blocks: list[UnifiedBlock]
    warnings: list[ProcessingIssue] = field(default_factory=list)
    errors: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class EvidenceRef:
    page: int
    block_id: str
    bbox: BBox
    confidence: float | None
    engine: str


@dataclass(frozen=True)
class EvidenceBlock:
    page: int
    block_id: str
    block_type: BlockType
    bbox: BBox
    text: str
    engine: str
    extract_method: ExtractMethod
    confidence: float | None
    searchable: bool
    reading_order: int
    source_hash: str
    pipeline_version: str
    section_type: SectionType = SectionType.MAIN_CONTRACT


@dataclass(frozen=True)
class ClauseUnit:
    clause_id: str
    clause_no: str | None
    clause_title: str | None
    text: str
    page_start: int
    page_end: int
    block_ids: list[str]
    evidence_refs: list[EvidenceRef]
    flags: list[str] = field(default_factory=list)
    section_type: SectionType = SectionType.MAIN_CONTRACT


@dataclass(frozen=True)
class ClauseSplitResult:
    clauses: list[ClauseUnit]
    issues: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class ExtractedField:
    field_name: str
    value: str | bool | list[str] | None
    confidence: float | None
    reason: str
    evidence_refs: list[EvidenceRef]
    flags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ContractFields:
    contract_type: ExtractedField
    counterparties: ExtractedField
    effective_date: ExtractedField
    expiration_date: ExtractedField
    auto_renewal: ExtractedField
    termination_notice_period: ExtractedField
    governing_law: ExtractedField
    jurisdiction: ExtractedField


@dataclass(frozen=True)
class FieldExtractionResult:
    fields: ContractFields
    issues: list[ProcessingIssue] = field(default_factory=list)


@dataclass(frozen=True)
class ReviewItem:
    review_id: str
    level: ReviewLevel
    reason_codes: list[str]
    message: str
    page_refs: list[int]
    block_ids: list[str]
    field_names: list[str]
