from __future__ import annotations

import re
from dataclasses import dataclass, field, replace

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import BlockType, ErrorSeverity, ReasonCode, SectionType
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
_ORPHAN_FRAGMENT_HEAD_RE = re.compile(r"^\s*[のてにをがでと、，]")
_FORM_SIGNAL_RE = re.compile(r"(?:様式第?[0-9０-９一二三四五六七八九十]*|通知書|届出書|請求書|実績報告書)")
_INSTRUCTION_SIGNAL_RE = re.compile(r"(?:記載要領|記入要領|作成要領|取扱要領|記載例)")
_APPENDIX_SIGNAL_RE = re.compile(r"(?:^|\s)(?:別紙|別表|別添|付属資料|仕様書)")
_SIGNATURE_SIGNAL_RE = re.compile(r"(?:記名押印|署名欄|押印欄|甲\s*[:：]|乙\s*[:：]|住所|氏名|代表者)")
_EXECUTION_SIGNATURE_SIGNAL_RE = re.compile(
    r"(?:上記契約の成立を証するため|本契約(?:締結)?の証として|この契約書は[0-9０-９一二三四五六七八九十]+通作成し|電磁的記録|電子署名)"
)
_TAIL_FORM_SIGNAL_RE = re.compile(r"(?:必要に応じて追加|任意記載)")
_LAW_NAME_PREFIX_RE = re.compile(
    r"(?:民法|商法|会社法|刑法|憲法|個人情報保護法|不正競争防止法|独占禁止法|下請法|著作権法|労働基準法)$"
)
_CITATION_TAIL_RE = re.compile(
    r"^(?:第[0-9０-９一二三四五六七八九十百千〇零]+(?:項|号|節|款)|"
    r"[のノ][0-9０-９一二三四五六七八九十百千〇零]+|"
    r"に|を|が|へ|で|と|の|では|において|により|による|に基づ|に従|に定め|で定め|または|若しくは)"
)


@dataclass
class _ClauseDraft:
    clause_no: str | None
    clause_title: str | None
    text_parts: list[str] = field(default_factory=list)
    blocks: list[EvidenceBlock] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    section_type: SectionType = SectionType.MAIN_CONTRACT


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
        seen_article_heading = False
        section_boundary_uncertain_count = 0

        for block in ordered:
            raw_text = str(block.text)
            for segment in self._split_embedded_headings(raw_text):
                text = normalize_text(segment)
                if not text:
                    continue

                heading = self._detect_heading(text)
                section_type = self._infer_section_type(
                    text=text,
                    heading=heading,
                    block=block,
                    seen_article_heading=seen_article_heading,
                )
                if heading is not None and heading[0].startswith("第"):
                    seen_article_heading = True

                if current is not None and current.section_type != section_type:
                    if (
                        pending_next_clause_title is not None
                        and not self._is_non_clause_material(
                            block=pending_next_clause_title[1],
                            text=pending_next_clause_title[2],
                            section_type=current.section_type,
                        )
                    ):
                        current.blocks.append(pending_next_clause_title[1])
                        current.text_parts.append(pending_next_clause_title[2])
                    pending_next_clause_title = None
                    if current.blocks:
                        drafts.append(current)
                    strong_boundary = self._is_strong_section_boundary_signal(text=text, section_type=section_type) or (
                        section_type == SectionType.SIGNATURE
                        and block.block_type in {BlockType.SIGNATURE_AREA, BlockType.STAMP_AREA}
                    )
                    if not strong_boundary:
                        section_boundary_uncertain_count += 1
                    current = None
                    expecting_title_line = False

                if heading is not None:
                    if not self._should_start_new_clause_heading(
                        current=current,
                        heading_text=text,
                        heading=heading,
                        previous_article_number=previous_article_number,
                        block=block,
                    ):
                        if current is None:
                            current = _ClauseDraft(
                                clause_no=None,
                                clause_title="前文" if section_type == SectionType.PREAMBLE else None,
                                section_type=section_type,
                            )
                        if not self._is_non_clause_material(block=block, text=text, section_type=section_type):
                            current.blocks.append(block)
                            current.text_parts.append(text)
                        continue
                    if current is not None and current.blocks:
                        drafts.append(current)
                    current = _ClauseDraft(clause_no=heading[0], clause_title=heading[1], section_type=section_type)
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
                        section_type=current.section_type,
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
                    current = _ClauseDraft(
                        clause_no=None,
                        clause_title="前文" if section_type == SectionType.PREAMBLE else None,
                        section_type=section_type,
                    )

                if self._is_next_clause_subtitle_candidate(current=current, block=block, text=text):
                    title = self._extract_parenthesized_title(text)
                    if title is not None:
                        pending_next_clause_title = (title, block, text)
                        continue

                if self._is_non_clause_material(block=block, text=text, section_type=current.section_type):
                    continue

                current.blocks.append(block)
                current.text_parts.append(text)

        if pending_next_clause_title is not None and current is not None:
            if not self._is_non_clause_material(
                block=pending_next_clause_title[1],
                text=pending_next_clause_title[2],
                section_type=current.section_type,
            ):
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
                    section_type=draft.section_type,
                )
            )

        clauses = self._postprocess_clauses(clauses)
        issues = self._evaluate_stability(clauses)
        if section_boundary_uncertain_count > 0:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.SECTION_BOUNDARY_UNCERTAIN,
                    message="section boundary required heuristic fallback",
                    details={"boundary_count": section_boundary_uncertain_count},
                )
            )
        return ClauseSplitResult(clauses=clauses, issues=issues)

    @staticmethod
    def _split_embedded_headings(text: str) -> list[str]:
        normalized = normalize_text(text)
        if not normalized:
            return []
        matches = [
            match
            for match in _STRONG_ARTICLE_TOKEN_RE.finditer(normalized)
            if ClauseSplitter._is_embedded_heading_split_point(normalized, match.start(), match.end())
        ]
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
    def _is_embedded_heading_split_point(text: str, token_start: int, token_end: int) -> bool:
        prefix = normalize_text(text[:token_start])
        suffix = normalize_text(text[token_end:])
        if ClauseSplitter._looks_like_citation_tail(suffix):
            return False
        if token_start <= 0:
            return True
        prefix_tail = normalize_text(text[max(0, token_start - 20) : token_start])
        if ClauseSplitter._looks_like_legal_reference_prefix(prefix_tail):
            return False
        compact_prefix = re.sub(r"\s+", "", prefix)
        if not compact_prefix:
            return True
        sentence_boundary = ("。", "．", ":", "：", ";", "；", ")", "）", "]", "】")
        return compact_prefix.endswith(sentence_boundary)

    @staticmethod
    def _looks_like_legal_reference_prefix(prefix: str) -> bool:
        compact = re.sub(r"\s+", "", normalize_text(prefix))
        if not compact:
            return False
        if _LAW_NAME_PREFIX_RE.search(compact):
            return True
        return bool(re.search(r"(?<!方)[一-龥]{1,8}法$", compact))

    @staticmethod
    def _looks_like_citation_tail(tail_text: str) -> bool:
        compact = re.sub(r"\s+", "", normalize_text(tail_text))
        if not compact:
            return False
        return _CITATION_TAIL_RE.match(compact) is not None

    @staticmethod
    def _is_probable_in_body_article_reference(heading_text: str) -> bool:
        compact = re.sub(r"\s+", "", normalize_text(heading_text))
        if not compact.startswith("第") or "条" not in compact:
            return False
        tail = compact.split("条", 1)[1]
        if not tail:
            return False
        if _CITATION_TAIL_RE.match(tail):
            return True
        return False

    @staticmethod
    def _should_start_new_clause_heading(
        current: _ClauseDraft | None,
        heading_text: str,
        heading: tuple[str, str | None],
        previous_article_number: int | None,
        block: EvidenceBlock,
    ) -> bool:
        if not ClauseSplitter._is_valid_clause_heading(heading_text=heading_text, heading=heading, block=block):
            return False
        if is_annotation_like_text(heading_text):
            return False
        if heading[0].startswith("第") and ClauseSplitter._is_probable_in_body_article_reference(heading_text):
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
    def _is_valid_clause_heading(
        heading_text: str,
        heading: tuple[str, str | None],
        block: EvidenceBlock,
    ) -> bool:
        normalized = normalize_text(heading_text)
        if not normalized:
            return False
        if is_annotation_like_text(normalized):
            return False
        if len(normalized) > 100:
            return False
        if any(token in normalized for token in ["コメント", "解説", "ひな形", "オプション条項"]):
            return False
        if heading[0].startswith("第"):
            if parse_article_number(heading[0]) is None:
                return False
            if _ITEM_ONLY_RE.fullmatch(normalized):
                return False
            if not block.searchable and len(normalized) <= 8 and heading[1] is None:
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
    def _infer_section_type(
        text: str,
        heading: tuple[str, str | None] | None,
        block: EvidenceBlock,
        seen_article_heading: bool,
    ) -> SectionType:
        normalized = normalize_text(text)
        if not normalized:
            return SectionType.MAIN_CONTRACT
        if heading is not None and heading[0].startswith("第"):
            return SectionType.MAIN_CONTRACT
        if "特記事項" in normalized:
            return SectionType.SPECIAL_PROVISIONS
        if _APPENDIX_SIGNAL_RE.search(normalized):
            return SectionType.APPENDIX
        if _INSTRUCTION_SIGNAL_RE.search(normalized):
            return SectionType.INSTRUCTION
        if _FORM_SIGNAL_RE.search(normalized):
            return SectionType.FORM
        if _TAIL_FORM_SIGNAL_RE.search(normalized):
            return SectionType.FORM
        if block.block_type in {BlockType.SIGNATURE_AREA, BlockType.STAMP_AREA}:
            return SectionType.SIGNATURE
        if _EXECUTION_SIGNATURE_SIGNAL_RE.search(normalized):
            return SectionType.SIGNATURE
        if _SIGNATURE_SIGNAL_RE.search(normalized):
            if any(token in normalized for token in ["記名押印", "署名", "押印欄", "住所", "氏名", "代表者"]):
                return SectionType.SIGNATURE
        if not seen_article_heading:
            return SectionType.PREAMBLE
        return SectionType.MAIN_CONTRACT

    @staticmethod
    def _is_strong_section_boundary_signal(text: str, section_type: SectionType) -> bool:
        normalized = normalize_text(text)
        if not normalized:
            return False
        if section_type == SectionType.SPECIAL_PROVISIONS:
            return "特記事項" in normalized
        if section_type == SectionType.APPENDIX:
            return bool(_APPENDIX_SIGNAL_RE.search(normalized))
        if section_type == SectionType.FORM:
            return bool(_FORM_SIGNAL_RE.search(normalized) or _TAIL_FORM_SIGNAL_RE.search(normalized))
        if section_type == SectionType.INSTRUCTION:
            return bool(_INSTRUCTION_SIGNAL_RE.search(normalized))
        if section_type == SectionType.SIGNATURE:
            return bool(_SIGNATURE_SIGNAL_RE.search(normalized) or _EXECUTION_SIGNATURE_SIGNAL_RE.search(normalized))
        if section_type == SectionType.MAIN_CONTRACT:
            return bool(re.match(r"^\s*第[0-9０-９一二三四五六七八九十百千〇零]+\s*条", normalized))
        return False

    @staticmethod
    def _is_non_clause_material(
        block: EvidenceBlock,
        text: str,
        section_type: SectionType = SectionType.MAIN_CONTRACT,
    ) -> bool:
        if not text:
            return True
        if block.block_type in {BlockType.HEADER, BlockType.FOOTER}:
            return True
        if is_page_number_text(text):
            return True
        if section_type in {SectionType.APPENDIX, SectionType.FORM, SectionType.INSTRUCTION, SectionType.SIGNATURE}:
            if block.block_type in {BlockType.IMAGE, BlockType.TABLE}:
                return False
            return bool(is_annotation_like_text(text) and len(normalize_text(text)) <= 6)
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
        main_clause_count = 0
        heading_only_runs = 0
        low_boundary_count = 0
        previous_was_heading_only = False
        previous_number: int | None = None

        for clause in clauses:
            if clause.section_type != SectionType.MAIN_CONTRACT:
                continue
            main_clause_count += 1
            normalized_text = normalize_text(clause.text)
            if len(normalized_text) < self.settings.min_clause_text_chars:
                short_count += 1
                clause.flags.append(ReasonCode.SHORT_CLAUSE_TEXT.value)

            line_count = len([line for line in clause.text.splitlines() if line.strip()])
            heading_only = line_count <= 1 and len(normalized_text) < self.settings.min_clause_text_chars
            if heading_only and previous_was_heading_only:
                heading_only_runs += 1
            previous_was_heading_only = heading_only
            if "LOW_BOUNDARY_CONFIDENCE" in clause.flags:
                low_boundary_count += 1

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

        short_ratio = short_count / max(main_clause_count, 1)
        if main_clause_count >= 4 and short_ratio >= 0.55:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="high ratio of short clauses detected",
                    details={"short_clause_ratio": short_ratio},
                )
            )

        if heading_only_runs >= 2:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.CONSECUTIVE_CLAUSE_HEADINGS,
                    message="consecutive heading-like clauses detected",
                    details={"run_count": heading_only_runs},
                )
            )

        if low_boundary_count >= 3:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="multiple low-confidence clause boundaries detected",
                    details={"low_boundary_count": low_boundary_count},
                )
            )

        unstable_reasons = {
            ReasonCode.REVERSED_CLAUSE_NUMBER.value,
            ReasonCode.CONSECUTIVE_CLAUSE_HEADINGS.value,
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value,
        }
        if main_clause_count >= 3 and any(
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

    def _postprocess_clauses(self, clauses: list[ClauseUnit]) -> list[ClauseUnit]:
        if not clauses:
            return clauses
        result = self._attach_orphan_paragraphs(clauses)
        result = self._merge_trailing_clause_fragments(result)
        result = self._repair_reversed_clause_numbers(result)
        result = [self._dedupe_clause_heading_prefix(clause) for clause in result]
        result = self._merge_spurious_citation_clauses(result)

        for idx in range(1, len(result)):
            if result[idx - 1].section_type != result[idx].section_type:
                continue
            if result[idx].section_type != SectionType.MAIN_CONTRACT:
                continue
            score = self._score_clause_boundary_confidence(result[idx - 1], result[idx])
            if score < 0.45 and "LOW_BOUNDARY_CONFIDENCE" not in result[idx].flags:
                result[idx].flags.append("LOW_BOUNDARY_CONFIDENCE")
        return result

    @staticmethod
    def _dedupe_clause_heading_prefix(clause: ClauseUnit) -> ClauseUnit:
        if not clause.clause_no:
            return clause
        text = normalize_text(clause.text)
        if not text:
            return clause
        no = re.escape(clause.clause_no)
        duplicate_head = re.compile(rf"^\s*({no})(?:\s*{no})+")
        match = duplicate_head.match(text)
        if match:
            remainder = text[match.end() :].lstrip()
            text = f"{clause.clause_no} {remainder}".strip()
        return replace(clause, text=text)

    def _merge_spurious_citation_clauses(self, clauses: list[ClauseUnit]) -> list[ClauseUnit]:
        if not clauses:
            return clauses
        merged: list[ClauseUnit] = [clauses[0]]
        for clause in clauses[1:]:
            prev = merged[-1]
            if self._is_spurious_citation_clause(previous=prev, current=clause):
                merged[-1] = self._merge_clause_units(prev, clause, reason_flag="merged_citation_like_clause")
                continue
            merged.append(clause)
        return merged

    @staticmethod
    def _is_spurious_citation_clause(previous: ClauseUnit, current: ClauseUnit) -> bool:
        if previous.section_type != SectionType.MAIN_CONTRACT or current.section_type != SectionType.MAIN_CONTRACT:
            return False
        if current.clause_no is None or current.clause_title is not None:
            return False
        article_no = parse_article_number(current.clause_no)
        if article_no is None:
            return False
        compact_text = re.sub(r"\s+", "", normalize_text(current.text))
        compact_clause_no = re.sub(r"\s+", "", normalize_text(current.clause_no))
        if not compact_text.startswith(compact_clause_no):
            return False
        citation_tail = compact_text[len(compact_clause_no) :]
        if not citation_tail:
            return article_no >= 150 and len(compact_text) <= len(compact_clause_no) + 2
        if re.match(r"^第[0-9０-９一二三四五六七八九十百千〇零]+(?:項|号|節|款)", citation_tail):
            return True
        if article_no >= 150 and _CITATION_TAIL_RE.match(citation_tail):
            return True
        return False

    def _repair_reversed_clause_numbers(self, clauses: list[ClauseUnit]) -> list[ClauseUnit]:
        if not clauses:
            return clauses
        repaired: list[ClauseUnit] = [clauses[0]]
        for clause in clauses[1:]:
            prev = repaired[-1]
            if clause.section_type != prev.section_type:
                repaired.append(clause)
                continue
            prev_no = parse_article_number(prev.clause_no)
            curr_no = parse_article_number(clause.clause_no)
            reversed_number = prev_no is not None and curr_no is not None and curr_no < prev_no
            if not reversed_number:
                repaired.append(clause)
                continue

            text_len = len(normalize_text(clause.text))
            line_count = len([line for line in clause.text.splitlines() if line.strip()])
            boundary_score = self._score_clause_boundary_confidence(prev, clause)
            if text_len <= 80 and line_count <= 3 and boundary_score < 0.45:
                repaired[-1] = self._merge_clause_units(prev, clause, reason_flag="repaired_reversed_clause_number")
                continue
            if ReasonCode.REVERSED_CLAUSE_NUMBER.value not in clause.flags:
                clause.flags.append(ReasonCode.REVERSED_CLAUSE_NUMBER.value)
            repaired.append(clause)
        return repaired

    def _merge_trailing_clause_fragments(self, clauses: list[ClauseUnit]) -> list[ClauseUnit]:
        if not clauses:
            return clauses
        merged: list[ClauseUnit] = [clauses[0]]
        for clause in clauses[1:]:
            prev = merged[-1]
            if clause.section_type != prev.section_type:
                merged.append(clause)
                continue
            if clause.clause_no is not None:
                merged.append(clause)
                continue
            compact = normalize_text(clause.text)
            line_count = len([line for line in clause.text.splitlines() if line.strip()])
            fragment_like = (
                len(compact) <= max(20, self.settings.min_clause_text_chars // 2)
                or _ORPHAN_FRAGMENT_HEAD_RE.match(compact) is not None
                or (line_count <= 2 and is_fragment_like_text(compact))
            )
            if fragment_like and prev.clause_no is not None:
                merged[-1] = self._merge_clause_units(prev, clause, reason_flag="merged_trailing_fragment")
                continue
            merged.append(clause)
        return merged

    def _attach_orphan_paragraphs(self, clauses: list[ClauseUnit]) -> list[ClauseUnit]:
        if not clauses:
            return clauses
        attached: list[ClauseUnit] = [clauses[0]]
        for clause in clauses[1:]:
            prev = attached[-1]
            if clause.section_type != prev.section_type:
                attached.append(clause)
                continue
            if clause.clause_no is not None:
                attached.append(clause)
                continue
            compact = normalize_text(clause.text)
            if not compact or is_annotation_like_text(compact):
                attached.append(clause)
                continue
            orphan_like = (
                len(compact) <= 140
                and not compact.startswith("第")
                and (
                    _ORPHAN_FRAGMENT_HEAD_RE.match(compact) is not None
                    or self._score_clause_boundary_confidence(prev, clause) < 0.55
                )
            )
            if orphan_like and prev.clause_no is not None:
                attached[-1] = self._merge_clause_units(prev, clause, reason_flag="attached_orphan_paragraph")
                continue
            attached.append(clause)
        return attached

    @staticmethod
    def _score_clause_boundary_confidence(previous: ClauseUnit, current: ClauseUnit) -> float:
        score = 0.55
        if previous.section_type != current.section_type:
            score -= 0.35
        prev_no = parse_article_number(previous.clause_no)
        curr_no = parse_article_number(current.clause_no)
        if prev_no is not None and curr_no is not None:
            if curr_no == prev_no + 1:
                score += 0.30
            elif curr_no <= prev_no:
                score -= 0.40
        elif current.clause_no is None:
            score -= 0.18

        page_gap = current.page_start - previous.page_end
        if page_gap <= 1:
            score += 0.08
        else:
            score -= 0.10

        curr_text = normalize_text(current.text)
        if len(curr_text) <= 16:
            score -= 0.10
        if _ORPHAN_FRAGMENT_HEAD_RE.match(curr_text):
            score -= 0.15
        if curr_text.endswith(("の", "て", "に", "を", "が", "で", "と", "、")):
            score -= 0.08
        return max(0.0, min(1.0, score))

    @staticmethod
    def _merge_clause_units(previous: ClauseUnit, current: ClauseUnit, reason_flag: str) -> ClauseUnit:
        merged_block_ids = unique_preserve_order(previous.block_ids + current.block_ids)
        merged_refs = list(previous.evidence_refs) + [ref for ref in current.evidence_refs if ref not in previous.evidence_refs]
        merged_flags = list(previous.flags)
        if reason_flag not in merged_flags:
            merged_flags.append(reason_flag)
        return replace(
            previous,
            text=f"{previous.text.rstrip()}\n{current.text.lstrip()}",
            page_end=max(previous.page_end, current.page_end),
            block_ids=merged_block_ids,
            evidence_refs=merged_refs,
            flags=merged_flags,
        )
