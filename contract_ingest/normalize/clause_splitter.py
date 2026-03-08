from __future__ import annotations

import re
from dataclasses import dataclass, field

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ReasonCode
from contract_ingest.domain.models import ClauseSplitResult, ClauseUnit, EvidenceBlock, EvidenceRef, ProcessingIssue
from contract_ingest.utils.text import (
    is_annotation_like_text,
    is_fragment_like_text,
    is_page_number_text,
    normalize_text,
    parse_article_number,
    unique_preserve_order,
)

_STRONG_ARTICLE_RE = re.compile(
    r"^\s*[【\[(（]?\s*(?P<no>第[0-9０-９一二三四五六七八九十百千〇零]+条)\s*[】\])）]?\s*(?P<tail>.*)$"
)
_STRONG_ARTICLE_TOKEN_RE = re.compile(
    r"(?:(?<=^)|(?<=[\s。．、，:：;；\]】)）]))第[0-9０-９一二三四五六七八九十百千〇零]+条"
)
_APPENDIX_RE = re.compile(r"^\s*(?P<no>附則|別紙|別表)\s*(?:[（(](?P<title>[^）)]+)[）)])?\s*$")
_PAREN_TITLE_ONLY_RE = re.compile(r"^\s*[（(](?P<title>[^）)]{1,60})[）)]\s*$")
_ITEM_ONLY_RE = re.compile(r"^\s*(?:\(?[0-9０-９]{1,2}\)?|[①-⑳])\s*$")


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
        expecting_title_line = False
        previous_article_number: int | None = None
        pending_next_clause_title: tuple[str, EvidenceBlock, str] | None = None

        for block in ordered:
            raw_text = str(block.text)
            for segment in self._split_embedded_headings(raw_text):
                text = normalize_text(segment)
                if not text:
                    continue

                heading = self._detect_heading(text)
                if heading is not None:
                    if not self._should_start_new_clause_heading(
                        current=current,
                        heading_text=text,
                        heading=heading,
                        previous_article_number=previous_article_number,
                        block=block,
                    ):
                        if current is None:
                            current = _ClauseDraft(clause_no=None, clause_title="前文")
                        if not self._is_non_clause_material(block=block, text=text):
                            current.blocks.append(block)
                            current.text_parts.append(text)
                        continue
                    if current is not None and current.blocks:
                        drafts.append(current)
                    current = _ClauseDraft(clause_no=heading[0], clause_title=heading[1])
                    if pending_next_clause_title is not None and current.clause_title is None:
                        pending_title, pending_block, pending_text = pending_next_clause_title
                        current.clause_title = pending_title
                        current.blocks.append(pending_block)
                        current.text_parts.append(pending_text)
                    pending_next_clause_title = None
                    current.blocks.append(block)
                    current.text_parts.append(text)
                    expecting_title_line = current.clause_title is None
                    article_no = parse_article_number(heading[0])
                    if article_no is not None:
                        previous_article_number = article_no
                    continue

                if (
                    pending_next_clause_title is not None
                    and not self._is_next_clause_subtitle_candidate(current=current, block=block, text=text)
                ):
                    if current is not None and not self._is_non_clause_material(
                        block=pending_next_clause_title[1],
                        text=pending_next_clause_title[2],
                    ):
                        current.blocks.append(pending_next_clause_title[1])
                        current.text_parts.append(pending_next_clause_title[2])
                    pending_next_clause_title = None

                if current is not None and expecting_title_line:
                    paren_title = self._extract_parenthesized_title(text)
                    if paren_title is not None:
                        current.clause_title = paren_title
                        current.blocks.append(block)
                        current.text_parts.append(text)
                        expecting_title_line = False
                        continue
                expecting_title_line = False

                if current is None:
                    current = _ClauseDraft(clause_no=None, clause_title="前文")

                if self._is_next_clause_subtitle_candidate(current=current, block=block, text=text):
                    title = self._extract_parenthesized_title(text)
                    if title is not None:
                        pending_next_clause_title = (title, block, text)
                        continue

                if self._is_non_clause_material(block=block, text=text):
                    continue

                current.blocks.append(block)
                current.text_parts.append(text)

        if pending_next_clause_title is not None and current is not None:
            if not self._is_non_clause_material(block=pending_next_clause_title[1], text=pending_next_clause_title[2]):
                current.blocks.append(pending_next_clause_title[1])
                current.text_parts.append(pending_next_clause_title[2])
        if current is not None and current.blocks:
            drafts.append(current)

        clauses: list[ClauseUnit] = []
        for idx, draft in enumerate(drafts, start=1):
            text = "\n".join(part for part in draft.text_parts if part).strip()
            if not text:
                continue
            pages = [block.page for block in draft.blocks]
            block_ids = unique_preserve_order([block.block_id for block in draft.blocks])
            evidence_refs = [self._to_ref(block) for block in draft.blocks]
            clauses.append(
                ClauseUnit(
                    clause_id=f"clause_{idx:03d}",
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
    def _split_embedded_headings(text: str) -> list[str]:
        normalized = normalize_text(text)
        if not normalized:
            return []
        matches = list(_STRONG_ARTICLE_TOKEN_RE.finditer(normalized))
        if not matches:
            return [normalized]
        if len(matches) == 1 and matches[0].start() == 0:
            return [normalized]

        segments: list[str] = []
        first_start = matches[0].start()
        if first_start > 0:
            prefix = normalized[:first_start].strip()
            if prefix:
                segments.append(prefix)

        for idx, match in enumerate(matches):
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(normalized)
            chunk = normalized[match.start() : end].strip()
            if chunk:
                segments.append(chunk)
        return segments

    @staticmethod
    def _should_start_new_clause_heading(
        current: _ClauseDraft | None,
        heading_text: str,
        heading: tuple[str, str | None],
        previous_article_number: int | None,
        block: EvidenceBlock,
    ) -> bool:
        if is_annotation_like_text(heading_text):
            return False
        if heading[1] is None and len(heading_text) <= 6 and not block.searchable and not heading[0].startswith("第"):
            return False
        current_number = parse_article_number(heading[0])
        if current_number is None:
            return True
        if previous_article_number is None:
            return True
        # Ignore likely OCR-induced reverse headings when the candidate looks too short/noisy.
        if current_number < previous_article_number:
            if len(heading_text) < 20 or (not block.searchable and is_fragment_like_text(heading_text)):
                return False
        if current is not None and current.clause_no == heading[0] and len(heading_text) < 20:
            return False
        return True

    @staticmethod
    def _is_next_clause_subtitle_candidate(
        current: _ClauseDraft | None,
        block: EvidenceBlock,
        text: str,
    ) -> bool:
        if current is None or current.clause_no is None:
            return False
        if not current.clause_no.startswith("第"):
            return False
        if is_annotation_like_text(text):
            return False
        title = ClauseSplitter._extract_parenthesized_title(text)
        if title is None:
            return False
        compact = normalize_text(title)
        if len(compact) > 22:
            return False
        if any(token in compact for token in ["。", "、", "：", ":", "。", ";"]):
            return False
        if not block.searchable and len(compact) <= 4:
            return False
        return True

    @staticmethod
    def _detect_heading(text: str) -> tuple[str, str | None] | None:
        if not text or _ITEM_ONLY_RE.fullmatch(text):
            return None

        article = _STRONG_ARTICLE_RE.match(text)
        if article:
            clause_no = normalize_text(article.group("no"))
            tail = normalize_text(article.group("tail"))
            title = None
            if tail:
                title = ClauseSplitter._extract_parenthesized_title(tail)
            return clause_no, title

        appendix = _APPENDIX_RE.match(text)
        if appendix:
            return appendix.group("no"), appendix.group("title")

        return None

    @staticmethod
    def _extract_parenthesized_title(text: str) -> str | None:
        match = _PAREN_TITLE_ONLY_RE.match(text)
        if not match:
            return None
        title = normalize_text(match.group("title"))
        if not title or _ITEM_ONLY_RE.fullmatch(title):
            return None
        if is_fragment_like_text(title):
            return None
        return title

    @staticmethod
    def _is_non_clause_material(block: EvidenceBlock, text: str) -> bool:
        if not text:
            return True
        if block.block_type in {BlockType.HEADER, BlockType.FOOTER}:
            return True
        if is_page_number_text(text):
            return True
        if is_annotation_like_text(text):
            return True
        if is_fragment_like_text(text) and not any(token in text for token in ["甲", "乙", "条", "項"]):
            return True
        if not block.searchable and is_fragment_like_text(text):
            return True
        return False

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
        if any(
            issue.reason_code.value in unstable_reasons
            for issue in issues
            if isinstance(issue.reason_code, ReasonCode)
        ):
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="clause split produced unstable signals",
                )
            )

        return issues
