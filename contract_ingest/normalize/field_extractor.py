from __future__ import annotations

from datetime import date
import re
from typing import Callable

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import ErrorSeverity, ReasonCode
from contract_ingest.domain.models import (
    ClauseUnit,
    ContractFields,
    EvidenceBlock,
    EvidenceRef,
    ExtractedField,
    FieldExtractionResult,
    ProcessingIssue,
)
from contract_ingest.normalize.contract_type_router import ROUTE_UNKNOWN, infer_contract_type, route_field_bias
from contract_ingest.normalize.counterparty_finder import (
    CounterpartyCandidate,
    find_preamble_counterparties,
    find_signature_counterparties,
    merge_counterparty_candidates,
)
from contract_ingest.normalize.field_validators import (
    FieldValidationResult,
    validate_counterparties,
    validate_effective_date,
    validate_expiration_date,
    validate_governing_law,
    validate_jurisdiction,
)
from contract_ingest.normalize.tail_clause_finder import (
    TailFieldCandidate,
    find_tail_effective_date_candidates,
    find_tail_expiration_candidates,
    find_tail_governing_law_candidates,
    find_tail_jurisdiction_candidates,
)
from contract_ingest.utils.text import normalize_digits, normalize_text, unique_preserve_order
from contract_ingest.utils.time import to_iso_date

_ABSOLUTE_DATE_TOKEN_PATTERN = (
    r"(?:令和\s*[0-9０-９元]{1,2}年\s*[0-9０-９]{1,2}月\s*[0-9０-９]{1,2}日"
    r"|[0-9０-９]{4}\s*年\s*[0-9０-９]{1,2}\s*月\s*[0-9０-９]{1,2}\s*日"
    r"|[0-9０-９]{4}\s*[/.]\s*[0-9０-９]{1,2}\s*[/.]\s*[0-9０-９]{1,2}"
    r"|[0-9０-９]{4}-[0-9０-９]{2}-[0-9０-９]{2})"
)
_ABSOLUTE_DATE_TOKEN_RE = re.compile(_ABSOLUTE_DATE_TOKEN_PATTERN)
_ENGLISH_MONTH_DATE_TOKEN_PATTERN = (
    r"(?:(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|"
    r"Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,2},\s*\d{4}"
    r"|\d{1,2}\s+(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|"
    r"Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4})"
)


class ContractFieldExtractor:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def extract(
        self,
        blocks: list[EvidenceBlock],
        clauses: list[ClauseUnit] | None = None,
    ) -> FieldExtractionResult:
        ordered_blocks = sorted(blocks, key=lambda b: (b.reading_order, b.page, b.bbox.y0, b.bbox.x0))
        issues: list[ProcessingIssue] = []

        contract_type = self._extract_contract_type(ordered_blocks)
        counterparties = self._extract_counterparties(ordered_blocks)
        effective_date = self._extract_effective_date(ordered_blocks)
        expiration_date = self._extract_expiration_date(ordered_blocks)
        auto_renewal = self._extract_auto_renewal(ordered_blocks)
        termination_notice_period = self._extract_termination_notice_period(ordered_blocks)
        governing_law = self._extract_governing_law(ordered_blocks, clauses)
        jurisdiction = self._extract_jurisdiction(ordered_blocks, clauses)

        route = infer_contract_type(
            ordered_blocks,
            clauses=clauses,
            hinted_contract_type=contract_type.value if isinstance(contract_type.value, str) else None,
        )
        preamble_counterparty_candidates = find_preamble_counterparties(ordered_blocks)
        signature_counterparty_candidates = find_signature_counterparties(ordered_blocks)
        merged_counterparty_candidates = merge_counterparty_candidates(
            preamble_counterparty_candidates,
            signature_counterparty_candidates,
        )
        counterparties = self._supplement_counterparties_with_finders(
            current=counterparties,
            candidates=merged_counterparty_candidates,
            route=route,
        )

        governing_law = self._supplement_text_field_with_tail_finder(
            current=governing_law,
            candidates=find_tail_governing_law_candidates(ordered_blocks, clauses=clauses, route=route),
            validator=validate_governing_law,
            finder_flag="tail_governing_law_finder",
            replace_margin=self._replacement_margin(route, "governing_law"),
        )
        jurisdiction = self._supplement_text_field_with_tail_finder(
            current=jurisdiction,
            candidates=find_tail_jurisdiction_candidates(ordered_blocks, clauses=clauses, route=route),
            validator=validate_jurisdiction,
            finder_flag="tail_jurisdiction_finder",
            replace_margin=self._replacement_margin(route, "jurisdiction"),
        )
        expiration_date = self._supplement_text_field_with_tail_finder(
            current=expiration_date,
            candidates=find_tail_expiration_candidates(ordered_blocks, clauses=clauses, route=route),
            validator=validate_expiration_date,
            finder_flag="tail_expiration_finder",
            replace_margin=self._replacement_margin(route, "expiration_date"),
        )
        effective_date = self._supplement_text_field_with_tail_finder(
            current=effective_date,
            candidates=find_tail_effective_date_candidates(ordered_blocks, clauses=clauses, route=route),
            validator=validate_effective_date,
            finder_flag="tail_effective_finder",
            replace_margin=self._replacement_margin(route, "effective_date"),
        )
        relative_jurisdiction = self._detect_relative_jurisdiction_expression(ordered_blocks, clauses=clauses)

        counterparties, counterparties_issues = self._apply_field_validation_gate(
            field=counterparties,
            validator=validate_counterparties,
            rejection_reason=ReasonCode.LOW_QUALITY_COUNTERPARTY,
            accepted_warning_reason=ReasonCode.PARTIAL_COUNTERPARTY,
            rejection_message="counterparty candidate was rejected by quality gate",
            accepted_warning_message="counterparty candidate contains partial fragments",
        )
        effective_date, effective_date_issues = self._apply_field_validation_gate(
            field=effective_date,
            validator=validate_effective_date,
            rejection_reason=ReasonCode.MISSING_EFFECTIVE_DATE,
            accepted_warning_reason=ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE,
            rejection_message="effective_date candidate was rejected by quality gate",
            accepted_warning_message="effective_date candidate is anchor-only and requires review",
        )
        expiration_date, expiration_date_issues = self._apply_field_validation_gate(
            field=expiration_date,
            validator=validate_expiration_date,
            rejection_reason=ReasonCode.LOW_QUALITY_EXPIRATION_DATE,
            accepted_warning_reason=ReasonCode.LOW_QUALITY_EXPIRATION_DATE,
            rejection_message="expiration_date candidate was rejected by quality gate",
            accepted_warning_message="expiration_date candidate is low quality",
        )
        governing_law, governing_law_issues = self._apply_field_validation_gate(
            field=governing_law,
            validator=validate_governing_law,
            rejection_reason=ReasonCode.LOW_QUALITY_GOVERNING_LAW,
            accepted_warning_reason=ReasonCode.LOW_QUALITY_GOVERNING_LAW,
            rejection_message="governing_law candidate was rejected by quality gate",
            accepted_warning_message="governing_law candidate has low quality signal",
        )
        jurisdiction, jurisdiction_issues = self._apply_field_validation_gate(
            field=jurisdiction,
            validator=validate_jurisdiction,
            rejection_reason=ReasonCode.LOW_QUALITY_JURISDICTION,
            accepted_warning_reason=ReasonCode.LOW_QUALITY_JURISDICTION,
            rejection_message="jurisdiction candidate was rejected by quality gate",
            accepted_warning_message="jurisdiction candidate has low quality signal",
        )
        jurisdiction, jurisdiction_issues = self._attach_relative_jurisdiction_expression(
            field=jurisdiction,
            issues=jurisdiction_issues,
            relative_expression=relative_jurisdiction,
        )
        issues.extend(counterparties_issues)
        issues.extend(effective_date_issues)
        issues.extend(expiration_date_issues)
        issues.extend(governing_law_issues)
        issues.extend(jurisdiction_issues)

        fields = ContractFields(
            contract_type=contract_type,
            counterparties=counterparties,
            effective_date=effective_date,
            expiration_date=expiration_date,
            auto_renewal=auto_renewal,
            termination_notice_period=termination_notice_period,
            governing_law=governing_law,
            jurisdiction=jurisdiction,
        )

        required_missing = [
            (contract_type, ReasonCode.MISSING_CONTRACT_TYPE),
            (effective_date, ReasonCode.MISSING_EFFECTIVE_DATE),
            (expiration_date, ReasonCode.MISSING_EXPIRATION_DATE),
            (governing_law, ReasonCode.MISSING_GOVERNING_LAW),
            (jurisdiction, ReasonCode.MISSING_JURISDICTION),
        ]

        for field, reason in required_missing:
            if field.value is None:
                if reason == ReasonCode.MISSING_EFFECTIVE_DATE and self._has_effective_date_anchor(field):
                    continue
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=reason,
                        message=f"required field is missing: {field.field_name}",
                        details={"field_name": field.field_name},
                    )
                )

        for field in [
            contract_type,
            counterparties,
            effective_date,
            expiration_date,
            auto_renewal,
            termination_notice_period,
            governing_law,
            jurisdiction,
        ]:
            if field.confidence is not None and field.confidence < self.settings.low_confidence_threshold:
                issues.append(
                    ProcessingIssue(
                        severity=ErrorSeverity.REVIEW,
                        reason_code=ReasonCode.LOW_CONFIDENCE,
                        message=f"low confidence field: {field.field_name}",
                        details={"confidence": field.confidence, "field_name": field.field_name},
                    )
                )

        if clauses is not None and len(clauses) <= 1 and len(ordered_blocks) > 10:
            issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.UNSTABLE_CLAUSE_SPLIT,
                    message="clause count appears low relative to block count",
                    details={"clause_count": len(clauses), "block_count": len(ordered_blocks)},
                )
            )

        return FieldExtractionResult(fields=fields, issues=issues)

    @staticmethod
    def _has_effective_date_anchor(field: ExtractedField) -> bool:
        return field.reason == "matched_effective_date_anchor_rule" or "anchor_only_effective_date" in field.flags

    def _apply_field_validation_gate(
        self,
        field: ExtractedField,
        validator: Callable[..., FieldValidationResult],
        rejection_reason: ReasonCode,
        accepted_warning_reason: ReasonCode | None,
        rejection_message: str,
        accepted_warning_message: str,
    ) -> tuple[ExtractedField, list[ProcessingIssue]]:
        if field.value is None:
            return field, []

        result = validator(
            field.value,
            reason=field.reason,
            confidence=field.confidence,
        )
        merged_flags = unique_preserve_order(list(field.flags) + list(result.quality_flags))
        issues: list[ProcessingIssue] = []

        if not result.accepted:
            merged_flags = unique_preserve_order(merged_flags + ["rejected_by_validator"])
            issues.append(
                self._build_validation_issue(
                    field=field,
                    reason_code=rejection_reason,
                    message=rejection_message,
                    result=result,
                )
            )
            return (
                ExtractedField(
                    field_name=field.field_name,
                    value=None,
                    confidence=None,
                    reason=f"{field.reason};rejected:{result.reason}",
                    evidence_refs=field.evidence_refs,
                    flags=merged_flags,
                ),
                issues,
            )

        normalized_value = result.normalized_value
        normalized_confidence = result.confidence if result.confidence is not None else field.confidence
        normalized_reason = field.reason
        accepted_field = ExtractedField(
            field_name=field.field_name,
            value=normalized_value,
            confidence=normalized_confidence,
            reason=normalized_reason,
            evidence_refs=field.evidence_refs,
            flags=merged_flags,
        )

        if result.anchor_only and accepted_warning_reason is not None:
            issues.append(
                self._build_validation_issue(
                    field=accepted_field,
                    reason_code=accepted_warning_reason,
                    message=accepted_warning_message,
                    result=result,
                )
            )
        elif accepted_warning_reason is not None:
            should_emit_warning = any(flag.endswith("partial_accept") for flag in merged_flags)
            if accepted_field.field_name == "expiration_date":
                should_emit_warning = should_emit_warning or any(
                    flag in merged_flags for flag in ["relative_period_only", "placeholder_date", "renewable_term"]
                )
            if should_emit_warning:
                issues.append(
                    self._build_validation_issue(
                        field=accepted_field,
                        reason_code=accepted_warning_reason,
                        message=accepted_warning_message,
                        result=result,
                    )
                )

        return accepted_field, issues

    @staticmethod
    def _looks_like_signature_execution_context(text: str) -> bool:
        compact = normalize_text(text)
        return any(
            marker in compact
            for marker in [
                "契約の成立を証するため",
                "契約締結を証するため",
                "契約締結の証として",
                "本契約の成立を証するため",
            ]
        )

    @staticmethod
    def _extract_placeholder_date_token(text: str) -> str | None:
        match = re.search(r"(令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)", normalize_text(text))
        if not match:
            return None
        return normalize_text(match.group(1))

    @staticmethod
    def _signature_execution_date_candidate(blocks: list[EvidenceBlock]) -> tuple[str, list[EvidenceRef]] | None:
        ordered = sorted(blocks, key=lambda b: (b.page, b.reading_order, b.bbox.y0, b.bbox.x0))
        for idx, block in enumerate(ordered):
            text = normalize_text(block.text)
            if not ContractFieldExtractor._looks_like_signature_execution_context(text):
                continue
            current_date = ContractFieldExtractor._extract_placeholder_date_token(text)
            refs = [ContractFieldExtractor._to_ref(block)]
            if current_date is not None:
                return current_date, refs
            for offset in range(1, 9):
                candidate_idx = idx + offset
                if candidate_idx >= len(ordered):
                    break
                neighbor = ordered[candidate_idx]
                if neighbor.page != block.page:
                    break
                candidate_date = ContractFieldExtractor._extract_placeholder_date_token(neighbor.text)
                if candidate_date is None:
                    continue
                refs.append(ContractFieldExtractor._to_ref(neighbor))
                return candidate_date, refs
        return None

    @staticmethod
    def _make_effective_signature_date_field(candidate: str, refs: list[EvidenceRef]) -> ExtractedField:
        return ExtractedField(
            field_name="effective_date",
            value=candidate,
            confidence=0.60,
            reason="matched_effective_date_signature_execution_rule",
            evidence_refs=unique_preserve_order_refs(refs),
            flags=["signature_execution_date"],
        )

    def _extract_effective_date(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(rf"(?:効力発生日|発効日|契約締結日|契約日)\s*[:：]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})"),
            re.compile(rf"(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*(?:より|から)\s*(?:効力|発効|開始)"),
            re.compile(rf"本契約[^。\n]{0,30}?(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})[^。\n]{0,10}?(?:締結|効力)"),
        ]
        absolute = self._extract_date_field(
            field_name="effective_date",
            blocks=blocks,
            patterns=patterns,
            reason="matched_effective_date_rule",
            missing_reason="rule_no_match_effective_date",
            allow_fallback_absolute=False,
        )
        if absolute.value is not None:
            return absolute

        english_absolute_patterns = [
            re.compile(
                rf"(?:effective\s+as\s+of|effective\s+on\s+and\s+after|from\s+and\s+after|"
                rf"entered\s+into\s+as\s+of|made\s+and\s+entered\s+into\s+as\s+of|dated\s+as\s+of|"
                rf"commencing\s+on|date\s+of\s+execution|execution\s+date)\s*(?P<date>{_ENGLISH_MONTH_DATE_TOKEN_PATTERN}|[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})",
                re.IGNORECASE,
            ),
            re.compile(
                rf"as\s+of\s+(?P<date>{_ENGLISH_MONTH_DATE_TOKEN_PATTERN}|[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})",
                re.IGNORECASE,
            ),
        ]
        for block in blocks:
            text = normalize_text(block.text)
            for pattern in english_absolute_patterns:
                match = pattern.search(text)
                if not match:
                    continue
                raw_date = normalize_text(match.group("date"))
                iso = self._normalize_date_token(raw_date)
                if iso is None:
                    continue
                return ExtractedField(
                    field_name="effective_date",
                    value=iso,
                    confidence=0.90,
                    reason="matched_effective_date_rule",
                    evidence_refs=[self._to_ref(block)],
                )

        relative_pattern = re.compile(
            r"(?P<expr>(?:本契約|効力|発効)[^。\n]{0,40}(?:契約締結日|契約締結の日|締結日)[^。\n]{0,24}(?:から|より)[^。\n]{0,24}(?:日|週|か月|ヶ月|ヵ月|年)間)"
        )

        for block in blocks:
            text = normalize_text(block.text)
            match = relative_pattern.search(text)
            if not match:
                continue
            return ExtractedField(
                field_name="effective_date",
                value=normalize_text(match.group("expr")).replace("契約締結の日", "契約締結日"),
                confidence=0.66,
                reason="matched_relative_effective_period_rule",
                evidence_refs=[self._to_ref(block)],
                flags=["relative_period_only"],
            )

        anchor_pattern = re.compile(
            r"(?P<anchor>(?:本契約締結日|契約締結日|契約締結の日|締結日)\s*(?:から|より))"
        )
        english_anchor_pattern = re.compile(
            r"(?P<anchor>(?:as\s+of\s+the\s+date\s+first\s+written\s+above|on\s+the\s+date\s+of\s+last\s+signature))",
            re.IGNORECASE,
        )
        anchor_refs: list[EvidenceRef] = []
        anchor_value: str | None = None
        for block in blocks:
            text = normalize_text(block.text)
            match = anchor_pattern.search(text)
            if not match:
                english_match = english_anchor_pattern.search(text)
                if not english_match:
                    continue
                if anchor_value is None:
                    anchor_value = normalize_text(english_match.group("anchor")).lower()
                anchor_refs.append(self._to_ref(block))
                continue
            if anchor_value is None:
                anchor_value = normalize_text(match.group("anchor")).replace("契約締結の日", "契約締結日")
            anchor_refs.append(self._to_ref(block))

        if anchor_refs and anchor_value is not None:
            return ExtractedField(
                field_name="effective_date",
                value=anchor_value,
                confidence=0.72,
                reason="matched_effective_date_anchor_rule",
                evidence_refs=unique_preserve_order_refs(anchor_refs),
                flags=["execution_date_anchor_detected"],
            )

        signature_candidate = self._signature_execution_date_candidate(blocks)
        if signature_candidate is not None:
            candidate_value, refs = signature_candidate
            return self._make_effective_signature_date_field(candidate_value, refs)

        period_clause_patterns = [
            re.compile(rf"本契約の有効期間は[、,]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*から"),
            re.compile(rf"契約期間は[、,]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*から"),
            re.compile(rf"委託期間は[、,]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*から"),
            re.compile(rf"有効期間は[、,]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*から"),
            re.compile(
                r"本契約の有効期間は[、,]?\s*(?P<date>令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)\s*から"
            ),
            re.compile(
                r"契約期間は[、,]?\s*(?P<date>令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)\s*から"
            ),
            re.compile(
                r"委託期間は[、,]?\s*(?P<date>令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)\s*から"
            ),
            re.compile(
                r"有効期間は[、,]?\s*(?P<date>令和\s*[0-9０-９元○◯]{1,4}年\s*[0-9０-９○◯]{1,3}月\s*[0-9０-９○◯]{1,3}日)\s*から"
            ),
        ]
        for block in blocks:
            text = normalize_text(block.text)
            for pattern in period_clause_patterns:
                match = pattern.search(text)
                if not match:
                    continue
                raw_date = normalize_text(match.group("date"))
                normalized_date = self._normalize_date_token(raw_date) or self._extract_placeholder_date_token(raw_date)
                if normalized_date is None:
                    continue
                return ExtractedField(
                    field_name="effective_date",
                    value=normalized_date,
                    confidence=0.64,
                    reason="matched_effective_date_period_clause_fallback",
                    evidence_refs=[self._to_ref(block)],
                    flags=["period_clause_fallback"],
                )

        return absolute

    @staticmethod
    def _build_validation_issue(
        field: ExtractedField,
        reason_code: ReasonCode,
        message: str,
        result: FieldValidationResult,
    ) -> ProcessingIssue:
        first_ref = field.evidence_refs[0] if field.evidence_refs else None
        return ProcessingIssue(
            severity=ErrorSeverity.REVIEW,
            reason_code=reason_code,
            message=message,
            page=first_ref.page if first_ref is not None else None,
            block_id=first_ref.block_id if first_ref is not None else None,
            details={
                "field_name": field.field_name,
                "candidate_value": result.raw_value if result.raw_value is not None else field.value,
                "why_rejected": result.reason,
                "confidence": field.confidence,
                "quality_flags": list(result.quality_flags),
                "anchor_only": result.anchor_only,
                "semantic_type": ContractFieldExtractor._extract_semantic_type(result.quality_flags),
                "relative_jurisdiction_expression": ContractFieldExtractor._extract_relative_jurisdiction_expression(
                    result.raw_value,
                    result.quality_flags,
                ),
                "bbox": first_ref.bbox.to_dict() if first_ref is not None else None,
                "snippet": normalize_text(str(result.raw_value))[:120] if result.raw_value is not None else None,
            },
        )

    def _supplement_counterparties_with_finders(
        self,
        current: ExtractedField,
        candidates: list[CounterpartyCandidate],
        route: str,
    ) -> ExtractedField:
        if not candidates:
            return current

        finder_names = [candidate.name for candidate in candidates if candidate.name]
        finder_value = unique_preserve_order(finder_names)
        if not finder_value:
            return current
        finder_refs = unique_preserve_order_refs([candidate.evidence_ref for candidate in candidates])
        finder_confidence = min(0.90, max(candidate.confidence for candidate in candidates))
        finder_field = ExtractedField(
            field_name="counterparties",
            value=finder_value,
            confidence=finder_confidence,
            reason=f"finder_counterparty_{route.lower()}",
            evidence_refs=finder_refs,
            flags=["finder_counterparty_candidate"],
        )

        current_eval = validate_counterparties(current.value, reason=current.reason, confidence=current.confidence)
        finder_eval = validate_counterparties(finder_field.value, reason=finder_field.reason, confidence=finder_field.confidence)

        if not finder_eval.accepted:
            return current
        normalized_finder = finder_eval.normalized_value
        if not isinstance(normalized_finder, list) or not normalized_finder:
            return current

        if not current_eval.accepted:
            return ExtractedField(
                field_name=current.field_name,
                value=normalized_finder,
                confidence=finder_eval.confidence if finder_eval.confidence is not None else finder_field.confidence,
                reason=f"{current.reason};finder_supplemented_counterparty",
                evidence_refs=unique_preserve_order_refs(finder_refs + current.evidence_refs),
                flags=unique_preserve_order(list(current.flags) + ["finder_counterparty_supplemented"]),
            )

        current_names = current_eval.normalized_value if isinstance(current_eval.normalized_value, list) else []
        if len(current_names) >= 2 and not current_eval.quality_flags:
            return current
        should_replace = False
        if len(normalized_finder) > len(current_names):
            should_replace = True
        elif current_eval.quality_flags and len(normalized_finder) >= 2 and len(normalized_finder) >= len(current_names):
            should_replace = True

        if should_replace:
            return ExtractedField(
                field_name=current.field_name,
                value=normalized_finder,
                confidence=finder_eval.confidence if finder_eval.confidence is not None else finder_field.confidence,
                reason=f"{current.reason};finder_supplemented_counterparty",
                evidence_refs=unique_preserve_order_refs(finder_refs + current.evidence_refs),
                flags=unique_preserve_order(list(current.flags) + ["finder_counterparty_supplemented"]),
            )
        return current

    def _supplement_text_field_with_tail_finder(
        self,
        current: ExtractedField,
        candidates: list[TailFieldCandidate],
        validator: Callable[..., FieldValidationResult],
        finder_flag: str,
        replace_margin: float = 0.08,
    ) -> ExtractedField:
        if not candidates:
            return current

        current_eval = validator(current.value, reason=current.reason, confidence=current.confidence)
        validated_candidates: list[tuple[TailFieldCandidate, FieldValidationResult, float]] = []
        for candidate in candidates:
            validated = validator(candidate.value, reason=candidate.reason, confidence=candidate.confidence)
            if not validated.accepted or validated.normalized_value is None:
                continue
            score = candidate.confidence + (validated.confidence if validated.confidence is not None else 0.0)
            if validated.anchor_only:
                score -= 0.10
            validated_candidates.append((candidate, validated, score))

        if not validated_candidates:
            return current
        best_candidate, best_validated, _ = sorted(validated_candidates, key=lambda item: (-item[2], item[0].reason))[0]

        should_replace = False
        if not current_eval.accepted or current.value is None:
            should_replace = True
        elif current_eval.anchor_only and not best_validated.anchor_only:
            should_replace = True
        elif current_eval.quality_flags and not best_validated.quality_flags:
            should_replace = True
        elif (current_eval.confidence or 0.0) + replace_margin < (best_validated.confidence or 0.0):
            should_replace = True

        if not should_replace:
            return current

        return ExtractedField(
            field_name=current.field_name,
            value=best_validated.normalized_value,
            confidence=best_validated.confidence if best_validated.confidence is not None else best_candidate.confidence,
            reason=f"{current.reason};finder:{best_candidate.reason}",
            evidence_refs=unique_preserve_order_refs([best_candidate.evidence_ref] + current.evidence_refs),
            flags=unique_preserve_order(list(current.flags) + [finder_flag, "finder_supplemented"]),
        )

    @staticmethod
    def _replacement_margin(route: str, field_name: str) -> float:
        bonus = route_field_bias(route, field_name)
        return max(0.04, 0.08 - bonus)

    @staticmethod
    def _extract_semantic_type(flags: list[str]) -> str | None:
        for flag in flags:
            if flag.startswith("semantic_type:"):
                semantic = normalize_text(flag.split(":", 1)[1])
                return semantic or None
        return None

    @staticmethod
    def _extract_relative_jurisdiction_expression(
        raw_value: str | bool | list[str] | None,
        flags: list[str],
    ) -> str | None:
        if "relative_jurisdiction_expression" not in flags:
            return None
        if isinstance(raw_value, str):
            candidate = normalize_text(raw_value)
            if "管轄する裁判所" in candidate:
                return candidate
        return None

    def _extract_contract_type(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        pattern_rules: list[tuple[str, re.Pattern[str], float]] = [
            ("業務委託基本契約書", re.compile(r"業務委託基本契約(?:書)?", re.IGNORECASE), 1.00),
            (
                "業務委託契約書",
                re.compile(r"(?:業務委託契約(?:書)?|準委任契約(?:書)?|業務を委託)", re.IGNORECASE),
                0.92,
            ),
            (
                "秘密保持契約書",
                re.compile(
                    r"(?:秘密保持契約(?:書)?|機密保持契約(?:書)?|\bNDA\b|NON[-\s]?DISCLOSURE)",
                    re.IGNORECASE,
                ),
                0.96,
            ),
            ("取引基本契約書", re.compile(r"取引基本契約(?:書)?", re.IGNORECASE), 0.95),
            ("売買基本契約書", re.compile(r"売買基本契約(?:書)?", re.IGNORECASE), 0.95),
            ("覚書", re.compile(r"覚書", re.IGNORECASE), 0.85),
            ("基本契約書", re.compile(r"基本契約(?:書)?", re.IGNORECASE), 0.55),
        ]

        inferred_rules: list[tuple[str, re.Pattern[str], float]] = [
            (
                "秘密保持契約書",
                re.compile(r"(?:目的|第1条)[^。\n]{0,40}(?:秘密情報|秘密保持|漏えい|開示)", re.IGNORECASE),
                0.60,
            ),
            (
                "業務委託契約書",
                re.compile(r"(?:目的|第1条)[^。\n]{0,40}(?:業務委託|委託業務|受託)", re.IGNORECASE),
                0.58,
            ),
            (
                "取引基本契約書",
                re.compile(r"(?:目的|第1条)[^。\n]{0,40}(?:継続的取引|取引基本)", re.IGNORECASE),
                0.58,
            ),
            (
                "売買基本契約書",
                re.compile(r"(?:目的|第1条)[^。\n]{0,40}(?:売買|商品供給)", re.IGNORECASE),
                0.58,
            ),
        ]

        scores: dict[str, float] = {}
        refs: dict[str, list[EvidenceRef]] = {}

        for idx, block in enumerate(blocks[:40]):
            text = normalize_text(block.text)
            if not text:
                continue
            context_bonus = self._contract_context_bonus(idx=idx, text=text)

            for contract_type, pattern, base_score in pattern_rules:
                if not pattern.search(text):
                    continue
                score = base_score + context_bonus
                scores[contract_type] = scores.get(contract_type, 0.0) + score
                refs.setdefault(contract_type, []).append(self._to_ref(block))

            for contract_type, pattern, base_score in inferred_rules:
                if not pattern.search(text):
                    continue
                score = base_score + context_bonus * 0.8
                scores[contract_type] = scores.get(contract_type, 0.0) + score
                refs.setdefault(contract_type, []).append(self._to_ref(block))

        if scores:
            priority = {
                "業務委託基本契約書": 0,
                "業務委託契約書": 1,
                "秘密保持契約書": 2,
                "取引基本契約書": 3,
                "売買基本契約書": 4,
                "覚書": 5,
                "基本契約書": 6,
            }
            best = sorted(
                scores.items(),
                key=lambda item: (-item[1], priority.get(item[0], 999), item[0]),
            )[0]
            best_type = best[0]
            best_score = best[1]
            confidence = min(0.96, 0.68 + min(best_score, 2.0) * 0.14)
            return ExtractedField(
                field_name="contract_type",
                value=best_type,
                confidence=round(confidence, 2),
                reason="matched_contract_type_scored_rule",
                evidence_refs=unique_preserve_order_refs(refs.get(best_type, []))[:3],
            )

        return ExtractedField(
            field_name="contract_type",
            value=None,
            confidence=None,
            reason="rule_no_match_contract_type",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_CONTRACT_TYPE.value],
        )

    @staticmethod
    def _contract_context_bonus(idx: int, text: str) -> float:
        bonus = 0.0
        if idx <= 2:
            bonus += 0.35
        elif idx <= 8:
            bonus += 0.20
        elif idx <= 20:
            bonus += 0.10
        if "第1条" in text or "目的" in text:
            bonus += 0.18
        if "契約書" in text:
            bonus += 0.08
        return bonus

    def _extract_counterparties(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        role_hits: dict[str, tuple[float, str, EvidenceRef]] = {}
        partial_fragments_detected = False
        role_patterns: list[tuple[re.Pattern[str], float]] = [
            (
                re.compile(
                    r"(?P<name>[^\n]{2,140}?)\s*[（(]?以下\s*[「『]?(?P<role>甲|乙)[」』]?",
                    re.IGNORECASE,
                ),
                1.00,
            ),
            (
                re.compile(r"(?P<role>甲|乙)\s*[:：]\s*(?P<name>[^\n]{2,140})", re.IGNORECASE),
                0.90,
            ),
            (
                re.compile(r"(?P<name>[^\n]{2,140}?)\s*を\s*(?P<role>甲|乙)\s*とし", re.IGNORECASE),
                0.80,
            ),
        ]

        max_page = max((block.page for block in blocks), default=1)

        scanned_blocks = blocks[:80]
        for idx, block in enumerate(scanned_blocks):
            text = normalize_text(block.text)
            if not text:
                continue
            is_signature_zone = (
                block.block_type.value in {"signature_area", "stamp_area"}
                or block.page == max_page
                or any(marker in text for marker in ["記名押印", "署名", "住所", "代表者"])
            )
            zone_bonus = 0.25 if is_signature_zone else (0.18 if block.reading_order <= 30 else 0.0)
            candidate_texts = [text]
            if idx + 1 < len(scanned_blocks):
                next_block = scanned_blocks[idx + 1]
                if next_block.page == block.page and (next_block.reading_order - block.reading_order) <= 2:
                    next_text = normalize_text(next_block.text)
                    if next_text:
                        candidate_texts.append(f"{text} {next_text}")

            for role_text in candidate_texts:
                for pattern, base_score in role_patterns:
                    for match in pattern.finditer(role_text):
                        role = normalize_text(match.group("role"))
                        name = self._normalize_party_name(match.group("name"))
                        if role not in {"甲", "乙"}:
                            continue
                        if not self._is_valid_party_name(name):
                            partial_fragments_detected = True
                            continue
                        score = base_score + zone_bonus
                        if role_text != text:
                            score -= 0.04
                        previous = role_hits.get(role)
                        if previous is None or score > previous[0]:
                            role_hits[role] = (score, name, self._to_ref(block))

        if role_hits:
            ordered_roles = [role for role in ["甲", "乙"] if role in role_hits]
            parties = [role_hits[role][1] for role in ordered_roles]
            refs = [role_hits[role][2] for role in ordered_roles]
            confidence = 0.92 if len(parties) == 2 else 0.76
            flags: list[str] = []
            if len(parties) == 1:
                flags.append("single_party_detected")
            if partial_fragments_detected and len(parties) < 2:
                flags.append("counterparty_partial_accept")
            return ExtractedField(
                field_name="counterparties",
                value=parties,
                confidence=confidence,
                reason="matched_party_role_japanese_rule",
                evidence_refs=unique_preserve_order_refs(refs),
                flags=flags,
            )

        fallback_names: list[str] = []
        fallback_refs: list[EvidenceRef] = []
        name_pattern = re.compile(
            r"([\w\u3040-\u30FF\u4E00-\u9FFF・\-\s]{1,80}(?:株式会社|合同会社|有限会社|Inc\.?|LLC))"
        )
        for block in blocks[:60]:
            text = normalize_text(block.text)
            for match in name_pattern.finditer(text):
                normalized_name = self._normalize_party_name(match.group(1))
                if not self._is_valid_party_name(normalized_name):
                    continue
                fallback_names.append(normalized_name)
                fallback_refs.append(self._to_ref(block))

        unique_names = unique_preserve_order(fallback_names)
        if unique_names:
            return ExtractedField(
                field_name="counterparties",
                value=unique_names[:2],
                confidence=0.65,
                reason="matched_party_name_fallback_rule",
                evidence_refs=unique_preserve_order_refs(fallback_refs),
                flags=["party_role_not_found"]
                + (["counterparty_partial_accept"] if partial_fragments_detected and len(unique_names) < 2 else []),
            )

        return ExtractedField(
            field_name="counterparties",
            value=None,
            confidence=None,
            reason="rule_no_match_counterparties",
            evidence_refs=[],
            flags=["counterparty_not_found"],
        )

    @staticmethod
    def _normalize_party_name(raw_name: str) -> str:
        text = normalize_text(raw_name)
        text = re.sub(r"(?:以下\s*[「『]?[甲乙].*)$", "", text)
        text = re.sub(r"(?:という。?\)?\s*(?:と|と、)?\s*)$", "", text)
        text = text.replace("㈱", "株式会社")
        text = re.sub(r"^[（(]\s*株\s*[）)]", "株式会社", text)
        text = re.sub(r"[（(]\s*株\s*[）)]", "株式会社", text)
        text = re.sub(r"^[（(]\s*同\s*[）)]", "合同会社", text)
        text = re.sub(r"[（(]\s*同\s*[）)]", "合同会社", text)
        text = re.sub(r"^[、,。\s]+", "", text)
        text = re.sub(r"\s+", " ", text).strip(" 　:：、，。;；")
        text = re.sub(r"(御中|様)$", "", text).strip(" 　")
        text = text.rstrip("と、のはにを")
        if text.count("(") != text.count(")") or text.count("（") != text.count("）"):
            text = text.replace("(", "").replace(")", "").replace("（", "").replace("）", "").strip()
        return text

    @staticmethod
    def _is_valid_party_name(name: str) -> bool:
        if len(name) < 2:
            return False
        if name in {"甲", "乙", "当事者", "本契約"}:
            return False
        if name.endswith(("と", "と、", "の", "は", "に", "を", "で")):
            return False
        if any(fragment in name for fragment in ["という。", "という", "とする", "以下"]):
            return False
        if name in {"株式会社", "合同会社", "有限会社", "公立大学法人", "国立大学法人", "独立行政法人"}:
            return False
        if re.fullmatch(r"[0-9]+", normalize_digits(name)):
            return False
        reject_markers = ("締結", "契約", "以下", "第", "条")
        return not any(marker in name for marker in reject_markers)

    def _extract_expiration_date(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(rf"(?:満了日|契約終了日|終了日)\s*[:：]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})"),
            re.compile(rf"(?:有効期間|契約期間)[^。\n]{0,80}?(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*まで"),
            re.compile(rf"(?:契約締結日|契約締結の日|締結日)\s*から\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*まで"),
            re.compile(rf"(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*をもって\s*(?:終了|満了)"),
        ]
        relative_patterns = [
            re.compile(r"(?:契約締結日|契約締結の日|締結日)\s*から\s*[0-9０-９一二三四五六七八九十百]+(?:日|か月|ヶ月|ヵ月|年)間"),
            re.compile(r"有効期間[^。\n]{0,40}(?:1|１|一)年間"),
            re.compile(r"(?:有効期間|契約期間)[^。\n]{0,80}(?:自動更新|同一条件で更新|更新するものとする|更新される)"),
        ]
        return self._extract_date_field(
            field_name="expiration_date",
            blocks=blocks,
            patterns=patterns,
            reason="matched_expiration_date_rule",
            missing_reason="rule_no_match_expiration_date",
            relative_patterns=relative_patterns,
            relative_reason="matched_relative_expiration_period_rule",
            allow_fallback_absolute=True,
        )

    def _extract_auto_renewal(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        negative_pattern = re.compile(r"(自動更新しない|更新しないものとする)")
        positive_pattern = re.compile(r"(自動更新|同一条件で更新|更新するものとする)")

        for block in blocks:
            text = normalize_text(block.text)
            if negative_pattern.search(text):
                return ExtractedField(
                    field_name="auto_renewal",
                    value=False,
                    confidence=0.90,
                    reason="matched_auto_renewal_negative_rule",
                    evidence_refs=[self._to_ref(block)],
                )
            if positive_pattern.search(text):
                return ExtractedField(
                    field_name="auto_renewal",
                    value=True,
                    confidence=0.88,
                    reason="matched_auto_renewal_positive_rule",
                    evidence_refs=[self._to_ref(block)],
                )

        return ExtractedField(
            field_name="auto_renewal",
            value=None,
            confidence=None,
            reason="rule_no_match_auto_renewal",
            evidence_refs=[],
            flags=["auto_renewal_undetermined"],
        )

    def _extract_termination_notice_period(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        pattern = re.compile(
            r"(?:解約|更新拒絶|通知)[^。\n]{0,40}?(?P<num>[0-9０-９一二三四五六七八九十百]+)(?P<unit>日|か月|ヶ月|ヵ月)\s*(前|まで|以内)"
        )

        for block in blocks:
            text = normalize_text(block.text)
            match = pattern.search(text)
            if not match:
                continue

            num = normalize_digits(match.group("num"))
            unit = match.group("unit")
            value = f"{num}{unit}"
            return ExtractedField(
                field_name="termination_notice_period",
                value=value,
                confidence=0.80,
                reason="matched_termination_notice_period_rule",
                evidence_refs=[self._to_ref(block)],
            )

        return ExtractedField(
            field_name="termination_notice_period",
            value=None,
            confidence=None,
            reason="rule_no_match_termination_notice_period",
            evidence_refs=[],
        )

    def _extract_governing_law(
        self,
        blocks: list[EvidenceBlock],
        clauses: list[ClauseUnit] | None = None,
    ) -> ExtractedField:
        patterns = [
            re.compile(r"本契約[^。\n]{0,96}?(?P<law>日本法|日本国法)[^。\n]{0,40}?(?:準拠|よる|従う|適用)", re.IGNORECASE),
            re.compile(
                r"本契約[^。\n]{0,120}?(?:成立|効力|履行|解釈)[^。\n]{0,120}?(?P<law>日本法|日本国法)[^。\n]{0,30}?(?:による|に準拠|を適用)",
                re.IGNORECASE,
            ),
            re.compile(
                r"本契約[^。\n]{0,120}?(?:成立及び効力|成立および効力)[^。\n]{0,120}?(?P<law>日本法|日本国法)[^。\n]{0,30}?(?:による|に準拠|を適用)",
                re.IGNORECASE,
            ),
            re.compile(r"(?P<law>日本法|日本国法)[^。\n]{0,20}?(?:に準拠|を適用|による)", re.IGNORECASE),
            re.compile(r"本契約に関する紛争には[^。\n]{0,30}?(?P<law>日本法|日本国法)[^。\n]{0,20}?を適用", re.IGNORECASE),
            re.compile(
                r"本契約[^。\n]{0,80}?(?:適用される法|関する一切の事項)[^。\n]{0,60}?(?P<law>日本法|日本国法)[^。\n]{0,20}?(?:とする|に従う|による)",
                re.IGNORECASE,
            ),
            re.compile(r"準拠法[^。\n]{0,40}?(?P<law>日本法|日本国法|[^\s、。\n]{2,20}法)", re.IGNORECASE),
            re.compile(r"適用法[^。\n]{0,40}?(?P<law>日本法|日本国法|[^\s、。\n]{2,20}法)", re.IGNORECASE),
        ]
        clause_keywords = (
            "準拠法",
            "適用法",
            "管轄及び準拠法",
            "裁判管轄及び準拠法",
            "準拠法等",
            "管轄",
            "合意管轄",
            "管轄裁判所",
            "一般条項",
            "雑則",
            "その他",
            "附則",
        )
        for scope_text, scope_ref, clause_related in self._iter_field_scopes(
            blocks=blocks,
            clauses=clauses,
            clause_keywords=clause_keywords,
        ):
            for sentence in self._split_clause_sentences(scope_text):
                sentence_clean = re.sub(r"[\[\]【】()（）]", "", sentence)
                sentence_compact = re.sub(r"\s+", "", sentence_clean)
                for target_sentence in [sentence_clean, sentence_compact]:
                    for pattern in patterns:
                        match = pattern.search(target_sentence)
                        if not match:
                            continue
                        law = normalize_text(match.group("law"))
                        if law in {"準拠法", "法", "本契約", "契約"}:
                            continue
                        if "準拠法" in law and "日本法" not in law:
                            continue
                        if "協議" in law:
                            continue
                        if "日本" in law:
                            law = "日本法"
                        confidence = 0.94 if clause_related else 0.89
                        return ExtractedField(
                            field_name="governing_law",
                            value=law,
                            confidence=confidence,
                            reason="matched_governing_law_clause_rule",
                            evidence_refs=[scope_ref],
                        )

                if re.search(r"[A-Za-z]", sentence_clean):
                    english_candidate = validate_governing_law(
                        sentence_clean,
                        reason="matched_governing_law_clause_rule",
                        confidence=0.90 if clause_related else 0.86,
                    )
                    if english_candidate.accepted:
                        confidence = 0.90 if clause_related else 0.86
                        return ExtractedField(
                            field_name="governing_law",
                            value=sentence_clean,
                            confidence=confidence,
                            reason="matched_governing_law_clause_rule",
                            evidence_refs=[scope_ref],
                        )

                if (
                    (
                        "準拠法" in sentence_compact
                        or "適用法" in sentence_compact
                        or "管轄及び準拠法" in sentence_compact
                        or "裁判管轄及び準拠法" in sentence_compact
                        or "成立効力履行および解釈" in sentence_compact
                        or "成立及び効力" in sentence_compact
                    )
                    and ("日本法" in sentence_compact or "日本国法" in sentence_compact)
                ):
                    confidence = 0.92 if clause_related else 0.87
                    return ExtractedField(
                        field_name="governing_law",
                        value="日本法",
                        confidence=confidence,
                        reason="matched_governing_law_clause_rule",
                        evidence_refs=[scope_ref],
                    )

        return ExtractedField(
            field_name="governing_law",
            value=None,
            confidence=None,
            reason="rule_no_match_governing_law",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_GOVERNING_LAW.value],
        )

    def _extract_jurisdiction(
        self,
        blocks: list[EvidenceBlock],
        clauses: list[ClauseUnit] | None = None,
    ) -> ExtractedField:
        patterns = [
            re.compile(
                r"(?P<court>[^\s、。\n]{2,40}裁判所)[^。\n]{0,50}(?:第一審の)?専属的合意管轄(?:裁判所)?",
                re.IGNORECASE,
            ),
            re.compile(
                r"(?:専属的)?合意管轄裁判所[^。\n]{0,20}(?P<court>[^\s、。\n]{2,40}裁判所)",
                re.IGNORECASE,
            ),
            re.compile(r"第一審[^。\n]{0,20}(?P<court>[^\s、。\n]{2,40}裁判所)", re.IGNORECASE),
        ]

        clause_keywords = ("管轄", "合意管轄", "管轄裁判所", "裁判所", "紛争", "一般条項", "雑則", "附則")
        for scope_text, scope_ref, clause_related in self._iter_field_scopes(
            blocks=blocks,
            clauses=clauses,
            clause_keywords=clause_keywords,
        ):
            for sentence in self._split_clause_sentences(scope_text):
                sentence_clean = re.sub(r"[\[\]【】()（）]", "", sentence)
                sentence_compact = re.sub(r"\s+", "", sentence_clean)
                for target_sentence in [sentence_clean, sentence_compact]:
                    for pattern in patterns:
                        match = pattern.search(target_sentence)
                        if not match:
                            continue
                        court = self._normalize_court_name(match.group("court"))
                        confidence = 0.94 if clause_related else 0.88
                        return ExtractedField(
                            field_name="jurisdiction",
                            value=court,
                            confidence=confidence,
                            reason="matched_jurisdiction_clause_rule",
                            evidence_refs=[scope_ref],
                        )

                if "合意管轄" in sentence_compact and "裁判所" in sentence_compact:
                    fallback_court = re.search(r"([^\s、。\n]{2,40}裁判所)", sentence_compact)
                    if fallback_court:
                        candidate = self._normalize_court_name(fallback_court.group(1))
                        if candidate in {"裁判所", "地方裁判所", "簡易裁判所", "家庭裁判所", "高等裁判所"}:
                            continue
                        confidence = 0.90 if clause_related else 0.84
                        return ExtractedField(
                            field_name="jurisdiction",
                            value=candidate,
                            confidence=confidence,
                            reason="matched_jurisdiction_clause_rule",
                            evidence_refs=[scope_ref],
                        )

        return ExtractedField(
            field_name="jurisdiction",
            value=None,
            confidence=None,
            reason="rule_no_match_jurisdiction",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_JURISDICTION.value],
        )

    def _iter_field_scopes(
        self,
        blocks: list[EvidenceBlock],
        clauses: list[ClauseUnit] | None,
        clause_keywords: tuple[str, ...],
    ) -> list[tuple[str, EvidenceRef, bool]]:
        scopes: list[tuple[str, EvidenceRef, bool]] = []
        if clauses:
            tail_start = max(0, len(clauses) - 5)
            prioritized_clauses = sorted(
                enumerate(clauses),
                key=lambda item: (
                    -self._clause_priority(item[1], clause_keywords),
                    -(1 if item[0] >= tail_start else 0),
                    -item[0],
                ),
            )
            for _, clause in prioritized_clauses:
                if not clause.evidence_refs:
                    continue
                clause_related = self._clause_priority(clause, clause_keywords) > 0.0
                scopes.append((clause.text, clause.evidence_refs[0], clause_related))

        for block in blocks:
            scopes.append((block.text, self._to_ref(block), False))
        return scopes

    def _detect_relative_jurisdiction_expression(
        self,
        blocks: list[EvidenceBlock],
        clauses: list[ClauseUnit] | None,
    ) -> tuple[str, EvidenceRef] | None:
        pattern = re.compile(
            r"(?P<expr>(?:(?:甲|乙|被告|原告|相手方|当事者)\s*の\s*)?(?:所在地|住所|本店所在地|本店)\s*を\s*管轄する裁判所)"
        )
        clause_keywords = ("管轄", "合意管轄", "管轄裁判所", "裁判所", "紛争", "雑則", "附則")
        for scope_text, scope_ref, _ in self._iter_field_scopes(blocks=blocks, clauses=clauses, clause_keywords=clause_keywords):
            compact = normalize_text(scope_text)
            match = pattern.search(compact)
            if not match:
                continue
            expr = normalize_text(match.group("expr"))
            if expr and "管轄する裁判所" in expr:
                return expr, scope_ref
        return None

    def _attach_relative_jurisdiction_expression(
        self,
        field: ExtractedField,
        issues: list[ProcessingIssue],
        relative_expression: tuple[str, EvidenceRef] | None,
    ) -> tuple[ExtractedField, list[ProcessingIssue]]:
        if relative_expression is None:
            return field, issues

        expression, ref = relative_expression
        expression_flag = f"relative_jurisdiction_expression:{expression}"
        updated_field = ExtractedField(
            field_name=field.field_name,
            value=field.value,
            confidence=field.confidence,
            reason=field.reason,
            evidence_refs=field.evidence_refs,
            flags=unique_preserve_order(list(field.flags) + ["relative_jurisdiction_expression", expression_flag]),
        )

        updated_issues = list(issues)
        replaced = False
        for idx, issue in enumerate(updated_issues):
            issue_reason = issue.reason_code.value if isinstance(issue.reason_code, ReasonCode) else str(issue.reason_code)
            if issue_reason != ReasonCode.LOW_QUALITY_JURISDICTION.value:
                continue
            details = dict(issue.details)
            details["candidate_value"] = expression
            details["why_rejected"] = "relative_jurisdiction_expression"
            details["snippet"] = expression
            details["relative_jurisdiction_expression"] = expression
            details["bbox"] = ref.bbox.to_dict()
            updated_issues[idx] = ProcessingIssue(
                severity=issue.severity,
                reason_code=issue.reason_code,
                message=issue.message,
                page=ref.page,
                block_id=ref.block_id,
                details=details,
            )
            replaced = True
            break

        if not replaced and updated_field.value is None:
            updated_issues.append(
                ProcessingIssue(
                    severity=ErrorSeverity.REVIEW,
                    reason_code=ReasonCode.LOW_QUALITY_JURISDICTION,
                    message="relative jurisdiction expression detected but normalized court name is unresolved",
                    page=ref.page,
                    block_id=ref.block_id,
                    details={
                        "field_name": "jurisdiction",
                        "candidate_value": expression,
                        "why_rejected": "relative_jurisdiction_expression",
                        "snippet": expression,
                        "bbox": ref.bbox.to_dict(),
                        "relative_jurisdiction_expression": expression,
                    },
                )
            )
        return updated_field, updated_issues

    @staticmethod
    def _clause_priority(clause: ClauseUnit, keywords: tuple[str, ...]) -> float:
        title_text = normalize_text(f"{clause.clause_no or ''} {clause.clause_title or ''}")
        body_text = normalize_text(clause.text)
        title_compact = re.sub(r"\s+", "", title_text)
        body_compact = re.sub(r"\s+", "", body_text)
        score = 0.0
        if any(keyword in title_text or re.sub(r"\s+", "", keyword) in title_compact for keyword in keywords):
            score += 1.0
        if any(keyword in body_text or re.sub(r"\s+", "", keyword) in body_compact for keyword in keywords):
            score += 0.3
        if clause.clause_no in {"附則", "別紙", "別表"}:
            score += 0.2
        return score

    @staticmethod
    def _normalize_court_name(raw_text: str) -> str:
        normalized = normalize_text(raw_text)
        normalized = re.sub(r"^第[0-9０-９一二三四五六七八九十百千〇零]+条", "", normalized)
        normalized = re.sub(r"^(準拠法|管轄)", "", normalized)
        normalized = re.sub(r"^(専属的)?合意管轄", "", normalized)
        normalized = re.sub(r"^第一審(?:の)?", "", normalized)
        tail_match = re.search(r"([^\s、。\n]{2,20}裁判所)$", normalized)
        if tail_match:
            return normalize_text(tail_match.group(1))
        return normalized

    def _extract_date_field(
        self,
        field_name: str,
        blocks: list[EvidenceBlock],
        patterns: list[re.Pattern[str]],
        reason: str,
        missing_reason: str,
        relative_patterns: list[re.Pattern[str]] | None = None,
        relative_reason: str | None = None,
        allow_fallback_absolute: bool = False,
    ) -> ExtractedField:
        relative_refs: list[EvidenceRef] = []

        for block in blocks:
            text = normalize_text(block.text)
            sentences = self._split_clause_sentences(text)
            for sentence in sentences:
                for pattern in patterns:
                    match = pattern.search(sentence)
                    if not match:
                        continue
                    raw_date = normalize_text(match.group("date"))
                    iso = self._normalize_date_token(raw_date)
                    if iso is None:
                        continue
                    return ExtractedField(
                        field_name=field_name,
                        value=iso,
                        confidence=0.93,
                        reason=reason,
                        evidence_refs=[self._to_ref(block)],
                    )

                if allow_fallback_absolute:
                    fallback = self._first_absolute_date(sentence)
                    if fallback is not None:
                        return ExtractedField(
                            field_name=field_name,
                            value=fallback,
                            confidence=0.82,
                            reason=f"{reason}_fallback",
                            evidence_refs=[self._to_ref(block)],
                        )

            if relative_patterns is not None and relative_reason is not None:
                for relative_pattern in relative_patterns:
                    if relative_pattern.search(text):
                        relative_refs.append(self._to_ref(block))
                        break

        if relative_refs and relative_reason is not None:
            return ExtractedField(
                field_name=field_name,
                value=None,
                confidence=0.60,
                reason=relative_reason,
                evidence_refs=unique_preserve_order_refs(relative_refs),
                flags=["relative_period_only"],
            )

        return ExtractedField(
            field_name=field_name,
            value=None,
            confidence=None,
            reason=missing_reason,
            evidence_refs=[],
        )

    @staticmethod
    def _split_clause_sentences(text: str) -> list[str]:
        if not text:
            return []
        parts = re.split(r"[。\n]", text)
        sentences: list[str] = []
        for part in parts:
            normalized = normalize_text(part)
            if normalized:
                sentences.append(normalized)
        return sentences if sentences else [normalize_text(text)]

    @staticmethod
    def _first_absolute_date(text: str) -> str | None:
        for match in _ABSOLUTE_DATE_TOKEN_RE.finditer(text):
            raw_date = normalize_text(match.group(0))
            iso = ContractFieldExtractor._normalize_date_token(raw_date)
            if iso is not None:
                return iso
        return None

    @staticmethod
    def _normalize_date_token(raw_date: str) -> str | None:
        normalized = normalize_digits(normalize_text(raw_date))

        iso_match = re.search(r"(?P<y>[0-9]{4})-(?P<m>[0-9]{1,2})-(?P<d>[0-9]{1,2})", normalized)
        if iso_match:
            year = int(iso_match.group("y"))
            month = int(iso_match.group("m"))
            day = int(iso_match.group("d"))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None

        reiwa_match = re.search(r"令和\s*(?P<y>元|[0-9]{1,2})年\s*(?P<m>[0-9]{1,2})月\s*(?P<d>[0-9]{1,2})日", normalized)
        if reiwa_match:
            year_token = reiwa_match.group("y")
            year = 2019 if year_token == "元" else 2018 + int(year_token)
            month = int(reiwa_match.group("m"))
            day = int(reiwa_match.group("d"))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None

        ymd_slash_match = re.search(r"(?P<y>[0-9]{4})\s*[/.]\s*(?P<m>[0-9]{1,2})\s*[/.]\s*(?P<d>[0-9]{1,2})", normalized)
        if ymd_slash_match:
            year = int(ymd_slash_match.group("y"))
            month = int(ymd_slash_match.group("m"))
            day = int(ymd_slash_match.group("d"))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None

        month_map = {
            "jan": 1,
            "january": 1,
            "feb": 2,
            "february": 2,
            "mar": 3,
            "march": 3,
            "apr": 4,
            "april": 4,
            "may": 5,
            "jun": 6,
            "june": 6,
            "jul": 7,
            "july": 7,
            "aug": 8,
            "august": 8,
            "sep": 9,
            "sept": 9,
            "september": 9,
            "oct": 10,
            "october": 10,
            "nov": 11,
            "november": 11,
            "dec": 12,
            "december": 12,
        }
        lower = normalized.lower()
        month_first = re.search(r"([a-z]+)\s+([0-9]{1,2}),\s*([0-9]{4})", lower)
        if month_first and month_first.group(1) in month_map:
            month = month_map[month_first.group(1)]
            day = int(month_first.group(2))
            year = int(month_first.group(3))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None

        day_first = re.search(r"([0-9]{1,2})\s+([a-z]+)\s+([0-9]{4})", lower)
        if day_first and day_first.group(2) in month_map:
            day = int(day_first.group(1))
            month = month_map[day_first.group(2)]
            year = int(day_first.group(3))
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None

        return to_iso_date(normalized)

    @staticmethod
    def _to_ref(block: EvidenceBlock) -> EvidenceRef:
        return EvidenceRef(
            page=block.page,
            block_id=block.block_id,
            bbox=block.bbox,
            confidence=block.confidence,
            engine=block.engine,
        )



def unique_preserve_order_refs(refs: list[EvidenceRef]) -> list[EvidenceRef]:
    seen: set[tuple[int, str]] = set()
    result: list[EvidenceRef] = []
    for ref in refs:
        key = (ref.page, ref.block_id)
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


FieldExtractor = ContractFieldExtractor
