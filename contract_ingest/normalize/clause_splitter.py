from __future__ import annotations

import re
from dataclasses import dataclass, field

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import ErrorSeverity, ReasonCode
from contract_ingest.domain.models import ClauseSplitResult, ClauseUnit, EvidenceBlock, EvidenceRef, ProcessingIssue
from contract_ingest.utils.text import normalize_text, parse_article_number, unique_preserve_order

_ARTICLE_RE = re.compile(
    r"^\s*(?P<no>第[0-9０-９一二三四五六七八九十百千〇零]+条)(?:\s*[（(](?P<title>[^）)]+)[）)])?"
)
_APPENDIX_RE = re.compile(r"^\s*(?P<no>附則|別紙|別表)(?:\s*[（(](?P<title>[^）)]+)[）)])?")


@dataclass
class _ClauseDraft:
    clause_no: str | None
    clause_title: str | None
    text_parts: list[str] = field(default_factory=list)
    blocks: list[EvidenceBlock] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)


class ClauseSplitter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def split(self, blocks: list[EvidenceBlock]) -> ClauseSplitResult:
        ordered = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
        if not ordered:
            return ClauseSplitResult(
                clauses=[],
                issues=[
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                        message="no evidence blocks available for clause splitting",
                    )
                ],
            )

        drafts: list[_ClauseDraft] = []
        current: _ClauseDraft | None = None

        for block in ordered:
            text = normalize_text(block.text)
            heading = self._detect_heading(text)
            if heading is not None:
                if current is not None and current.blocks:
                    drafts.append(current)
                current = _ClauseDraft(clause_no=heading[0], clause_title=heading[1])
                current.blocks.append(block)
                current.text_parts.append(text)
                continue

            if current is None:
                current = _ClauseDraft(clause_no=None, clause_title="前文")

            current.blocks.append(block)
            current.text_parts.append(text)

        if current is not None and current.blocks:
            drafts.append(current)

        clauses: list[ClauseUnit] = []
        for idx, draft in enumerate(drafts, start=1):
            clause_id = f"clause_{idx:03d}"
            text = "\n".join(part for part in draft.text_parts if part).strip()
            pages = [block.page for block in draft.blocks]
            block_ids = unique_preserve_order([block.block_id for block in draft.blocks])
            evidence_refs = [self._to_ref(block) for block in draft.blocks]
            clauses.append(
                ClauseUnit(
                    clause_id=clause_id,
                    clause_no=draft.clause_no,
                    clause_title=draft.clause_title,
                    text=text,
                    page_start=min(pages),
                    page_end=max(pages),
                    block_ids=block_ids,
                    evidence_refs=evidence_refs,
                    flags=list(draft.flags),
                )
            )

        issues = self._evaluate_stability(clauses)
        return ClauseSplitResult(clauses=clauses, issues=issues)

    @staticmethod
    def _detect_heading(text: str) -> tuple[str, str | None] | None:
        if not text:
            return None
        article = _ARTICLE_RE.match(text)
        if article:
            return article.group("no"), article.group("title")
        appendix = _APPENDIX_RE.match(text)
        if appendix:
            return appendix.group("no"), appendix.group("title")
        return None

    @staticmethod
    def _to_ref(block: EvidenceBlock) -> EvidenceRef:
        return EvidenceRef(
            page=block.page,
            block_id=block.block_id,
            bbox=block.bbox,
            confidence=block.confidence,
            engine=block.engine,
        )

    def _evaluate_stability(self, clauses: list[ClauseUnit]) -> list[ProcessingIssue]:
        issues: list[ProcessingIssue] = []
        if not clauses:
            return issues

        short_count = 0
        heading_only_runs = 0
        previous_was_heading_only = False
        previous_number: int | None = None

        for clause in clauses:
            normalized_text = normalize_text(clause.text)
            if len(normalized_text) < self.settings.min_clause_text_chars:
                short_count += 1
                clause.flags.append(ReasonCode.SHORT_CLAUSE_TEXT.value)

            line_count = len([line for line in clause.text.splitlines() if line.strip()])
            heading_only = line_count <= 1 and len(normalized_text) < self.settings.min_clause_text_chars
            if heading_only and previous_was_heading_only:
                heading_only_runs += 1
            previous_was_heading_only = heading_only

            article_number = parse_article_number(clause.clause_no)
            if previous_number is not None and article_number is not None and article_number < previous_number:
                clause.flags.append(ReasonCode.REVERSED_CLAUSE_NUMBER.value)
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.REVERSED_CLAUSE_NUMBER,
                        message="clause numbering appears to go backwards",
                        page=clause.page_start,
                        details={"clause_no": clause.clause_no},
                    )
                )
            if article_number is not None:
                previous_number = article_number

            if clause.clause_no in {"別紙", "別表"} and len(normalized_text) < self.settings.min_clause_text_chars:
                clause.flags.append(ReasonCode.APPENDIX_BOUNDARY_AMBIGUOUS.value)

        short_ratio = short_count / max(len(clauses), 1)
        if short_ratio >= 0.40:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="high ratio of short clauses detected",
                    details={"short_clause_ratio": short_ratio},
                )
            )

        if heading_only_runs > 0:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.CONSECUTIVE_CLAUSE_HEADINGS,
                    message="consecutive heading-like clauses detected",
                    details={"run_count": heading_only_runs},
                )
            )

        unstable_reasons = {
            ReasonCode.REVERSED_CLAUSE_NUMBER.value,
            ReasonCode.CONSECUTIVE_CLAUSE_HEADINGS.value,
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value,
        }
        if any(issue.reason_code.value in unstable_reasons for issue in issues if isinstance(issue.reason_code, ReasonCode)):
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="clause split produced unstable signals",
                )
            )

        return issues
