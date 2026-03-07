from __future__ import annotations

import re

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
from contract_ingest.utils.text import normalize_digits, normalize_text, unique_preserve_order
from contract_ingest.utils.time import to_iso_date

_DATE_PATTERN = r"[0-9０-９]{4}年\s*[0-9０-９]{1,2}月\s*[0-9０-９]{1,2}日"


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
        governing_law = self._extract_governing_law(ordered_blocks)
        jurisdiction = self._extract_jurisdiction(ordered_blocks)

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

    def _extract_contract_type(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        candidates = [
            ("NDA", "秘密保持契約", re.compile(r"(秘密保持契約|機密保持契約|\bNDA\b)", re.IGNORECASE)),
            (
                "SERVICE",
                "業務委託契約",
                re.compile(r"(業務委託契約|委託契約書|準委任契約)", re.IGNORECASE),
            ),
            ("MASTER", "基本契約書", re.compile(r"(基本契約書|取引基本契約)", re.IGNORECASE)),
        ]
        for idx, block in enumerate(blocks[:12]):
            text = normalize_text(block.text)
            for _, contract_label, pattern in candidates:
                if pattern.search(text):
                    confidence = 0.95 if idx < 3 else 0.85
                    return ExtractedField(
                        field_name="contract_type",
                        value=contract_label,
                        confidence=confidence,
                        reason="matched_contract_type_title_rule",
                        evidence_refs=[self._to_ref(block)],
                    )

        return ExtractedField(
            field_name="contract_type",
            value=None,
            confidence=None,
            reason="rule_no_match_contract_type",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_CONTRACT_TYPE.value],
        )

    def _extract_counterparties(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        role_map: dict[str, str] = {}
        refs: list[EvidenceRef] = []

        role_pattern_1 = re.compile(
            r"(?P<name>[^\s、，。\n]{2,80})\s*（?以下[「『]?(?P<role>甲|乙)", re.IGNORECASE
        )
        role_pattern_2 = re.compile(r"(?P<role>甲|乙)\s*[:：]\s*(?P<name>[^\s、，。\n]{2,80})")

        for block in blocks[:40]:
            text = normalize_text(block.text)
            for pattern in [role_pattern_1, role_pattern_2]:
                for match in pattern.finditer(text):
                    role = match.group("role")
                    name = normalize_text(match.group("name"))
                    name = re.sub(r"[（(].*$", "", name).strip()
                    if len(name) < 2:
                        continue
                    if role not in role_map:
                        role_map[role] = name
                        refs.append(self._to_ref(block))

        if role_map:
            ordered = [role_map[k] for k in sorted(role_map.keys())]
            return ExtractedField(
                field_name="counterparties",
                value=ordered,
                confidence=0.90 if len(role_map) >= 2 else 0.75,
                reason="matched_party_role_rule",
                evidence_refs=unique_preserve_order_refs(refs),
                flags=[] if len(role_map) >= 2 else ["single_party_detected"],
            )

        fallback_names: list[str] = []
        fallback_refs: list[EvidenceRef] = []
        name_pattern = re.compile(r"([^\s、，。\n]{2,80}(?:株式会社|合同会社|有限会社|Inc\.?|LLC))")
        for block in blocks[:50]:
            text = normalize_text(block.text)
            for match in name_pattern.finditer(text):
                fallback_names.append(normalize_text(match.group(1)))
                fallback_refs.append(self._to_ref(block))

        unique_names = unique_preserve_order(fallback_names)
        if unique_names:
            return ExtractedField(
                field_name="counterparties",
                value=unique_names[:2],
                confidence=0.65,
                reason="matched_party_name_fallback_rule",
                evidence_refs=unique_preserve_order_refs(fallback_refs),
                flags=["party_role_not_found"],
            )

        return ExtractedField(
            field_name="counterparties",
            value=None,
            confidence=None,
            reason="rule_no_match_counterparties",
            evidence_refs=[],
            flags=["counterparty_not_found"],
        )

    def _extract_effective_date(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(rf"(?:効力発生日|契約締結日|発効日)[:：]?\s*(?P<date>{_DATE_PATTERN})"),
            re.compile(rf"(?P<date>{_DATE_PATTERN})\s*より\s*効力"),
        ]
        return self._extract_date_field(
            field_name="effective_date",
            blocks=blocks,
            patterns=patterns,
            reason="matched_effective_date_rule",
            missing_reason="rule_no_match_effective_date",
        )

    def _extract_expiration_date(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(rf"(?:満了日|契約終了日)[:：]?\s*(?P<date>{_DATE_PATTERN})"),
            re.compile(rf"(?:有効期間|契約期間)[^。\n]*?(?P<date>{_DATE_PATTERN})\s*まで"),
        ]
        return self._extract_date_field(
            field_name="expiration_date",
            blocks=blocks,
            patterns=patterns,
            reason="matched_expiration_date_rule",
            missing_reason="rule_no_match_expiration_date",
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

    def _extract_governing_law(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(r"準拠法[^。\n]{0,20}?(?P<law>日本法|[A-Za-z]+ law|[^\s、。\n]{2,20}法)", re.IGNORECASE),
            re.compile(r"本契約[^。\n]{0,20}?(?P<law>日本法)", re.IGNORECASE),
        ]

        for block in blocks:
            text = normalize_text(block.text)
            for pattern in patterns:
                match = pattern.search(text)
                if not match:
                    continue
                law = normalize_text(match.group("law"))
                return ExtractedField(
                    field_name="governing_law",
                    value=law,
                    confidence=0.90,
                    reason="matched_governing_law_rule",
                    evidence_refs=[self._to_ref(block)],
                )

        return ExtractedField(
            field_name="governing_law",
            value=None,
            confidence=None,
            reason="rule_no_match_governing_law",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_GOVERNING_LAW.value],
        )

    def _extract_jurisdiction(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(
                r"(?P<court>[^\s、。\n]{2,30}裁判所)[^。\n]{0,30}(専属的合意管轄|第一審の専属的管轄)",
                re.IGNORECASE,
            ),
            re.compile(r"専属的合意管轄裁判所は(?P<court>[^\s、。\n]{2,30}裁判所)", re.IGNORECASE),
        ]

        for block in blocks:
            text = normalize_text(block.text)
            for pattern in patterns:
                match = pattern.search(text)
                if not match:
                    continue
                court = normalize_text(match.group("court"))
                return ExtractedField(
                    field_name="jurisdiction",
                    value=court,
                    confidence=0.90,
                    reason="matched_jurisdiction_rule",
                    evidence_refs=[self._to_ref(block)],
                )

        return ExtractedField(
            field_name="jurisdiction",
            value=None,
            confidence=None,
            reason="rule_no_match_jurisdiction",
            evidence_refs=[],
            flags=[ReasonCode.MISSING_JURISDICTION.value],
        )

    def _extract_date_field(
        self,
        field_name: str,
        blocks: list[EvidenceBlock],
        patterns: list[re.Pattern[str]],
        reason: str,
        missing_reason: str,
    ) -> ExtractedField:
        for block in blocks:
            text = normalize_text(block.text)
            for pattern in patterns:
                match = pattern.search(text)
                if not match:
                    continue
                raw_date = normalize_text(match.group("date"))
                iso = to_iso_date(raw_date)
                value = iso if iso is not None else raw_date
                confidence = 0.92 if iso is not None else 0.78
                return ExtractedField(
                    field_name=field_name,
                    value=value,
                    confidence=confidence,
                    reason=reason,
                    evidence_refs=[self._to_ref(block)],
                )

        return ExtractedField(
            field_name=field_name,
            value=None,
            confidence=None,
            reason=missing_reason,
            evidence_refs=[],
        )

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