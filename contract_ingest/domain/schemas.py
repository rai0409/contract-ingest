from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from contract_ingest.domain.enums import BlockType, ChunkType, DocumentKind, ExtractMethod, ReviewLevel, SectionType

_ALLOWED_FIELDS = {
    "contract_type",
    "counterparties",
    "effective_date",
    "expiration_date",
    "auto_renewal",
    "termination_notice_period",
    "governing_law",
    "jurisdiction",
}


class BBoxSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    x0: float
    y0: float
    x1: float
    y1: float

    @model_validator(mode="after")
    def validate_bbox(self) -> "BBoxSchema":
        if self.x0 >= self.x1:
            raise ValueError("x0 must be less than x1")
        if self.y0 >= self.y1:
            raise ValueError("y0 must be less than y1")
        return self


class EvidenceRefSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int = Field(ge=1)
    block_id: str
    bbox: BBoxSchema
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    engine: str


class ProcessingIssueSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: str
    reason_code: str
    message: str
    page: int | None = Field(default=None, ge=1)
    block_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class PageSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int = Field(ge=1)
    page_kind: DocumentKind
    native_text_char_count: int = Field(ge=0)
    ocr_ratio: float = Field(ge=0.0, le=1.0)
    classification_reason: str


class DocumentBlockSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int = Field(ge=1)
    block_id: str
    block_type: BlockType
    bbox: BBoxSchema
    text: str
    engine: str
    extract_method: ExtractMethod
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    searchable: bool
    reading_order: int = Field(ge=1)
    source_hash: str
    pipeline_version: str
    section_type: SectionType = SectionType.MAIN_CONTRACT


class ClauseSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    clause_id: str
    clause_no: str | None = None
    clause_title: str | None = None
    text: str
    page_start: int = Field(ge=1)
    page_end: int = Field(ge=1)
    block_ids: list[str] = Field(default_factory=list)
    evidence_refs: list[EvidenceRefSchema] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
    section_type: SectionType = SectionType.MAIN_CONTRACT

    @model_validator(mode="after")
    def validate_page_span(self) -> "ClauseSchema":
        if self.page_end < self.page_start:
            raise ValueError("page_end must be greater than or equal to page_start")
        return self


class ContractFieldSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    value: str | bool | list[str] | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    reason: str
    evidence_refs: list[EvidenceRefSchema] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
    quality: dict[str, Any] | None = None

    @field_validator("field_name")
    @classmethod
    def validate_field_name(cls, value: str) -> str:
        if value not in _ALLOWED_FIELDS:
            raise ValueError(f"unsupported field_name: {value}")
        return value


class FieldsSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_type: ContractFieldSchema
    counterparties: ContractFieldSchema
    effective_date: ContractFieldSchema
    expiration_date: ContractFieldSchema
    auto_renewal: ContractFieldSchema
    termination_notice_period: ContractFieldSchema
    governing_law: ContractFieldSchema
    jurisdiction: ContractFieldSchema


class DocumentSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str
    title: str | None = None
    source_file: str
    document_kind: DocumentKind
    pipeline_version: str
    source_hash: str
    pages: list[PageSchema]
    blocks: list[DocumentBlockSchema]
    clauses: list[ClauseSchema]
    fields: FieldsSchema
    warnings: list[ProcessingIssueSchema]
    errors: list[ProcessingIssueSchema]


class ChunkMetadataSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str
    chunk_index: int = Field(ge=0)
    type: ChunkType
    quality: ExtractMethod
    searchable: int
    clause_no: str | None = None
    clause_title: str | None = None
    source_pages: list[int]
    block_ids: list[str]
    evidence_refs: list[EvidenceRefSchema]
    contract_type: str | None = None
    section_type: SectionType

    @field_validator("searchable")
    @classmethod
    def validate_searchable(cls, value: int) -> int:
        if value not in {0, 1}:
            raise ValueError("searchable must be 0 or 1")
        return value

    @field_validator("source_pages")
    @classmethod
    def validate_source_pages(cls, value: list[int]) -> list[int]:
        if any(page < 1 for page in value):
            raise ValueError("source_pages must contain positive integers")
        deduped = sorted(set(value))
        if deduped != value:
            raise ValueError("source_pages must be sorted and unique")
        return value

    @field_validator("block_ids")
    @classmethod
    def validate_block_ids(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("block_ids must be unique")
        return value


class ChunkSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    text: str
    metadata: ChunkMetadataSchema


class ReviewItemSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    review_id: str
    level: ReviewLevel
    reason_codes: list[str]
    message: str
    page_refs: list[int]
    block_ids: list[str]
    field_names: list[str]
    field: str | None = None
    candidate_value: str | bool | list[str] | None = None
    why_rejected: str | None = None
    page: int | None = Field(default=None, ge=1)
    bbox: BBoxSchema | None = None
    snippet: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    suggested_action: str | None = None
    type: str | None = None
    severity: str | None = None
    evidence: dict[str, Any] | None = None
    suggested_fix: str | None = None


class ReviewSummarySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    warning_count: int = Field(ge=0)
    critical_count: int = Field(ge=0)
    high_count: int = Field(default=0, ge=0)
    medium_count: int = Field(default=0, ge=0)
    low_count: int = Field(default=0, ge=0)
    compare_ready: bool = False


class ReviewSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str
    review_required: bool
    items: list[ReviewItemSchema]
    summary: ReviewSummarySchema
