from __future__ import annotations

from datetime import date
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

_ABSOLUTE_DATE_TOKEN_PATTERN = (
    r"(?:令和\s*[0-9０-９元]{1,2}年\s*[0-9０-９]{1,2}月\s*[0-9０-９]{1,2}日"
    r"|[0-9０-９]{4}\s*年\s*[0-9０-９]{1,2}\s*月\s*[0-9０-９]{1,2}\s*日"
    r"|[0-9０-９]{4}\s*[/.]\s*[0-9０-９]{1,2}\s*[/.]\s*[0-9０-９]{1,2})"
)
_ABSOLUTE_DATE_TOKEN_RE = re.compile(_ABSOLUTE_DATE_TOKEN_PATTERN)


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
        return field.reason == "matched_effective_date_anchor_rule"

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

        for block in blocks[:80]:
            text = normalize_text(block.text)
            if not text:
                continue
            is_signature_zone = (
                block.block_type.value in {"signature_area", "stamp_area"}
                or block.page == max_page
                or any(marker in text for marker in ["記名押印", "署名", "住所", "代表者"])
            )
            zone_bonus = 0.25 if is_signature_zone else (0.18 if block.reading_order <= 30 else 0.0)

            for pattern, base_score in role_patterns:
                for match in pattern.finditer(text):
                    role = normalize_text(match.group("role"))
                    name = self._normalize_party_name(match.group("name"))
                    if role not in {"甲", "乙"} or not self._is_valid_party_name(name):
                        continue
                    score = base_score + zone_bonus
                    previous = role_hits.get(role)
                    if previous is None or score > previous[0]:
                        role_hits[role] = (score, name, self._to_ref(block))

        if role_hits:
            ordered_roles = [role for role in ["甲", "乙"] if role in role_hits]
            parties = [role_hits[role][1] for role in ordered_roles]
            refs = [role_hits[role][2] for role in ordered_roles]
            confidence = 0.92 if len(parties) == 2 else 0.76
            return ExtractedField(
                field_name="counterparties",
                value=parties,
                confidence=confidence,
                reason="matched_party_role_japanese_rule",
                evidence_refs=unique_preserve_order_refs(refs),
                flags=[] if len(parties) == 2 else ["single_party_detected"],
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

    @staticmethod
    def _normalize_party_name(raw_name: str) -> str:
        text = normalize_text(raw_name)
        text = re.sub(r"(?:以下\s*[「『]?[甲乙].*)$", "", text)
        text = text.replace("㈱", "株式会社")
        text = re.sub(r"^[（(]\s*株\s*[）)]", "株式会社", text)
        text = re.sub(r"[（(]\s*株\s*[）)]", "株式会社", text)
        text = re.sub(r"^[（(]\s*同\s*[）)]", "合同会社", text)
        text = re.sub(r"[（(]\s*同\s*[）)]", "合同会社", text)
        text = re.sub(r"\s+", " ", text).strip(" 　:：、，。;；")
        text = re.sub(r"(御中|様)$", "", text).strip(" 　")
        return text

    @staticmethod
    def _is_valid_party_name(name: str) -> bool:
        if len(name) < 2:
            return False
        if name in {"甲", "乙", "当事者", "本契約"}:
            return False
        if re.fullmatch(r"[0-9]+", normalize_digits(name)):
            return False
        reject_markers = ("締結", "契約", "以下", "第", "条")
        return not any(marker in name for marker in reject_markers)

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

        anchor_pattern = re.compile(
            r"(?P<anchor>(?:本契約締結日|契約締結日|締結日)\s*(?:から|より))"
        )
        anchor_refs: list[EvidenceRef] = []
        anchor_value: str | None = None
        for block in blocks:
            text = normalize_text(block.text)
            match = anchor_pattern.search(text)
            if not match:
                continue
            if anchor_value is None:
                anchor_value = normalize_text(match.group("anchor"))
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

        return absolute

    def _extract_expiration_date(self, blocks: list[EvidenceBlock]) -> ExtractedField:
        patterns = [
            re.compile(rf"(?:満了日|契約終了日|終了日)\s*[:：]?\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})"),
            re.compile(rf"(?:有効期間|契約期間)[^。\n]{0,80}?(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*まで"),
            re.compile(rf"(?:契約締結日|締結日)\s*から\s*(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*まで"),
            re.compile(rf"(?P<date>{_ABSOLUTE_DATE_TOKEN_PATTERN})\s*をもって\s*(?:終了|満了)"),
        ]
        relative_patterns = [
            re.compile(r"(?:契約締結日|締結日)\s*から\s*[0-9０-９一二三四五六七八九十百]+(?:日|か月|ヶ月|ヵ月|年)間"),
            re.compile(r"有効期間[^。\n]{0,40}(?:1|１|一)年間"),
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
            re.compile(r"本契約[^。\n]{0,40}?(?P<law>日本法)[^。\n]{0,20}?(?:準拠|よる|従う)", re.IGNORECASE),
            re.compile(r"(?P<law>日本法)[^。\n]{0,15}?に準拠", re.IGNORECASE),
            re.compile(r"準拠法[^。\n]{0,30}?(?P<law>日本法|[^\s、。\n]{2,20}法)", re.IGNORECASE),
        ]

        clause_keywords = ("準拠法", "管轄", "合意管轄", "管轄裁判所", "一般条項", "雑則", "その他", "附則")
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

                if "準拠法" in sentence_compact and "日本法" in sentence_compact:
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
                        confidence = 0.90 if clause_related else 0.84
                        return ExtractedField(
                            field_name="jurisdiction",
                            value=self._normalize_court_name(fallback_court.group(1)),
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
            prioritized_clauses = sorted(
                enumerate(clauses),
                key=lambda item: (
                    -self._clause_priority(item[1], clause_keywords),
                    item[0],
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
