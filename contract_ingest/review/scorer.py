from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from contract_ingest.config import Settings, get_settings
from contract_ingest.domain.enums import ErrorSeverity, ReasonCode, ReviewLevel
from contract_ingest.domain.models import ContractFields, MergedPage, ProcessingIssue


@dataclass(frozen=True)
class ReviewSignal:
    reason_code: str
    level: ReviewLevel
    score: float
    message: str
    action_hint: str
    page_refs: list[int] = field(default_factory=list)
    block_ids: list[str] = field(default_factory=list)
    field_names: list[str] = field(default_factory=list)
    field: str | None = None
    candidate_value: str | bool | list[str] | None = None
    why_rejected: str | None = None
    page: int | None = None
    bbox: dict[str, float] | None = None
    snippet: str | None = None
    confidence: float | None = None


@dataclass(frozen=True)
class ReviewAssessment:
    review_required: bool
    overall_score: float
    signals: list[ReviewSignal]


class ReviewScorer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def score(
        self,
        issues: list[ProcessingIssue],
        merged_pages: list[MergedPage],
        fields: ContractFields,
    ) -> ReviewAssessment:
        buckets: dict[str, dict] = {}

        for issue in issues:
            reason_code = issue.reason_code.value if isinstance(issue.reason_code, ReasonCode) else str(issue.reason_code)
            self._add_to_bucket(
                buckets=buckets,
                reason_code=reason_code,
                level=self._level_from_issue(issue, reason_code),
                score=self._score_from_reason(reason_code),
                message=issue.message,
                page=issue.page,
                block_id=issue.block_id,
                field_names=self._extract_field_names(issue),
                issue=issue,
            )

        for page in merged_pages:
            if page.ocr_ratio >= self.settings.high_ocr_ratio_threshold:
                self._add_to_bucket(
                    buckets=buckets,
                    reason_code=ReasonCode.HIGH_OCR_RATIO.value,
                    level=ReviewLevel.WARNING,
                    score=self._score_from_reason(ReasonCode.HIGH_OCR_RATIO.value),
                    message="OCR比率が閾値を超えています。",
                    page=page.page,
                    block_id=None,
                    field_names=[],
                    issue=None,
                )

        required_fields = {
            "contract_type": fields.contract_type.value,
            "effective_date": fields.effective_date.value,
            "expiration_date": fields.expiration_date.value,
            "governing_law": fields.governing_law.value,
            "jurisdiction": fields.jurisdiction.value,
        }
        missing_reason_map = {
            "contract_type": ReasonCode.MISSING_CONTRACT_TYPE.value,
            "effective_date": ReasonCode.MISSING_EFFECTIVE_DATE.value,
            "expiration_date": ReasonCode.MISSING_EXPIRATION_DATE.value,
            "governing_law": ReasonCode.MISSING_GOVERNING_LAW.value,
            "jurisdiction": ReasonCode.MISSING_JURISDICTION.value,
        }

        for field_name, value in required_fields.items():
            if value is None:
                self._add_to_bucket(
                    buckets=buckets,
                    reason_code=missing_reason_map[field_name],
                    level=ReviewLevel.WARNING,
                    score=self._score_from_reason(missing_reason_map[field_name]),
                    message=f"必須項目 `{field_name}` を抽出できませんでした。",
                    page=None,
                    block_id=None,
                    field_names=[field_name],
                    issue=None,
                )

        signals = [self._bucket_to_signal(reason_code, bucket) for reason_code, bucket in buckets.items()]
        signals = sorted(signals, key=lambda s: (s.level != ReviewLevel.CRITICAL, -s.score, s.reason_code))

        overall_score = min(1.0, sum(signal.score for signal in signals) / 3.0) if signals else 0.0
        review_required = any(signal.level in {ReviewLevel.WARNING, ReviewLevel.CRITICAL} for signal in signals)

        return ReviewAssessment(
            review_required=review_required,
            overall_score=overall_score,
            signals=signals,
        )

    def _add_to_bucket(
        self,
        buckets: dict[str, dict],
        reason_code: str,
        level: ReviewLevel,
        score: float,
        message: str,
        page: int | None,
        block_id: str | None,
        field_names: list[str],
        issue: ProcessingIssue | None,
    ) -> None:
        bucket = buckets.setdefault(
            reason_code,
            {
                "level": level,
                "score": score,
                "messages": [],
                "pages": set(),
                "block_ids": set(),
                "field_names": set(),
                "field": None,
                "candidate_value": None,
                "why_rejected": None,
                "page": None,
                "bbox": None,
                "snippet": None,
                "confidence": None,
            },
        )

        if level == ReviewLevel.CRITICAL:
            bucket["level"] = ReviewLevel.CRITICAL
        elif level == ReviewLevel.WARNING and bucket["level"] == ReviewLevel.INFO:
            bucket["level"] = ReviewLevel.WARNING

        bucket["score"] = max(float(bucket["score"]), score)
        bucket["messages"].append(message)

        if page is not None and page > 0:
            bucket["pages"].add(page)
        if block_id:
            bucket["block_ids"].add(block_id)
        for field_name in field_names:
            bucket["field_names"].add(field_name)

        context = self._extract_issue_context(issue=issue)
        if page is not None and bucket["page"] is None:
            bucket["page"] = page
        if field_names and bucket["field"] is None:
            bucket["field"] = field_names[0]
        if context["candidate_value"] is not None and bucket["candidate_value"] is None:
            bucket["candidate_value"] = context["candidate_value"]
        if context["why_rejected"] and bucket["why_rejected"] is None:
            bucket["why_rejected"] = context["why_rejected"]
        if context["bbox"] is not None and bucket["bbox"] is None:
            bucket["bbox"] = context["bbox"]
        if context["snippet"] and bucket["snippet"] is None:
            bucket["snippet"] = context["snippet"]
        if context["confidence"] is not None and bucket["confidence"] is None:
            bucket["confidence"] = context["confidence"]

    def _bucket_to_signal(self, reason_code: str, bucket: dict) -> ReviewSignal:
        message = self._message_for_reason(reason_code)
        action_hint = self._action_hint_for_reason(reason_code)

        if bucket["messages"]:
            message = f"{message} ({bucket['messages'][0]})"

        return ReviewSignal(
            reason_code=reason_code,
            level=bucket["level"],
            score=float(bucket["score"]),
            message=message,
            action_hint=action_hint,
            page_refs=sorted(int(page) for page in bucket["pages"]),
            block_ids=sorted(str(block_id) for block_id in bucket["block_ids"]),
            field_names=sorted(str(field_name) for field_name in bucket["field_names"]),
            field=bucket["field"],
            candidate_value=bucket["candidate_value"],
            why_rejected=bucket["why_rejected"],
            page=bucket["page"],
            bbox=bucket["bbox"],
            snippet=bucket["snippet"],
            confidence=bucket["confidence"],
        )

    @staticmethod
    def _extract_field_names(issue: ProcessingIssue) -> list[str]:
        field_names: list[str] = []
        raw_single = issue.details.get("field_name")
        if isinstance(raw_single, str) and raw_single:
            field_names.append(raw_single)
        raw_many = issue.details.get("field_names")
        if isinstance(raw_many, list):
            field_names.extend(str(item) for item in raw_many if item)
        return field_names

    @staticmethod
    def _extract_issue_context(issue: ProcessingIssue | None) -> dict[str, Any]:
        if issue is None:
            return {
                "candidate_value": None,
                "why_rejected": None,
                "bbox": None,
                "snippet": None,
                "confidence": None,
            }
        details = issue.details if isinstance(issue.details, dict) else {}
        bbox = details.get("bbox")
        if not isinstance(bbox, dict):
            bbox = None
        confidence = details.get("confidence")
        if isinstance(confidence, (int, float)):
            normalized_confidence: float | None = float(confidence)
        else:
            normalized_confidence = None
        return {
            "candidate_value": details.get("candidate_value"),
            "why_rejected": details.get("why_rejected"),
            "bbox": bbox,
            "snippet": details.get("snippet"),
            "confidence": normalized_confidence,
        }

    @staticmethod
    def _level_from_issue(issue: ProcessingIssue, reason_code: str) -> ReviewLevel:
        if issue.severity == ErrorSeverity.FATAL:
            return ReviewLevel.CRITICAL
        if issue.severity == ErrorSeverity.RECOVERABLE:
            if reason_code == ReasonCode.OCR_FAILURE.value:
                return ReviewLevel.CRITICAL
            return ReviewLevel.WARNING
        if reason_code in {
            ReasonCode.MISSING_CONTRACT_TYPE.value,
            ReasonCode.MISSING_EFFECTIVE_DATE.value,
            ReasonCode.MISSING_EXPIRATION_DATE.value,
            ReasonCode.MISSING_GOVERNING_LAW.value,
            ReasonCode.MISSING_JURISDICTION.value,
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value,
            ReasonCode.HIGH_OCR_RATIO.value,
            ReasonCode.LOW_CONFIDENCE.value,
            ReasonCode.LOW_QUALITY_COUNTERPARTY.value,
            ReasonCode.LOW_QUALITY_GOVERNING_LAW.value,
            ReasonCode.LOW_QUALITY_JURISDICTION.value,
            ReasonCode.LOW_QUALITY_EXPIRATION_DATE.value,
            ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value,
            ReasonCode.TABLE_CONTENT_UNPARSED.value,
            ReasonCode.APPENDIX_DETECTED_NOT_PARSED.value,
        }:
            return ReviewLevel.WARNING
        return ReviewLevel.INFO

    @staticmethod
    def _score_from_reason(reason_code: str) -> float:
        score_map = {
            ReasonCode.OCR_FAILURE.value: 0.95,
            ReasonCode.PARTIAL_EXTRACTION_FAILURE.value: 0.85,
            ReasonCode.HIGH_OCR_RATIO.value: 0.70,
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value: 0.65,
            ReasonCode.LOW_CONFIDENCE.value: 0.55,
            ReasonCode.MISSING_CONTRACT_TYPE.value: 0.75,
            ReasonCode.MISSING_EFFECTIVE_DATE.value: 0.65,
            ReasonCode.MISSING_EXPIRATION_DATE.value: 0.60,
            ReasonCode.MISSING_GOVERNING_LAW.value: 0.65,
            ReasonCode.MISSING_JURISDICTION.value: 0.65,
            ReasonCode.LOW_QUALITY_COUNTERPARTY.value: 0.70,
            ReasonCode.LOW_QUALITY_GOVERNING_LAW.value: 0.68,
            ReasonCode.LOW_QUALITY_JURISDICTION.value: 0.72,
            ReasonCode.LOW_QUALITY_EXPIRATION_DATE.value: 0.62,
            ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value: 0.50,
            ReasonCode.TABLE_CONTENT_UNPARSED.value: 0.45,
            ReasonCode.APPENDIX_DETECTED_NOT_PARSED.value: 0.42,
        }
        return score_map.get(reason_code, 0.30)

    @staticmethod
    def _message_for_reason(reason_code: str) -> str:
        message_map = {
            ReasonCode.LOW_CONFIDENCE.value: "低信頼度の抽出結果が含まれます。",
            ReasonCode.HIGH_OCR_RATIO.value: "OCR依存率が高い文書です。",
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value: "条文分解が不安定です。",
            ReasonCode.MISSING_CONTRACT_TYPE.value: "契約類型を抽出できませんでした。",
            ReasonCode.MISSING_EFFECTIVE_DATE.value: "効力発生日を抽出できませんでした。",
            ReasonCode.MISSING_EXPIRATION_DATE.value: "終了日を抽出できませんでした。",
            ReasonCode.MISSING_GOVERNING_LAW.value: "準拠法を抽出できませんでした。",
            ReasonCode.MISSING_JURISDICTION.value: "管轄裁判所を抽出できませんでした。",
            ReasonCode.OCR_FAILURE.value: "OCR処理に失敗した領域があります。",
            ReasonCode.PARTIAL_EXTRACTION_FAILURE.value: "一部領域で抽出欠損があります。",
            ReasonCode.LOW_QUALITY_COUNTERPARTY.value: "当事者名候補に低品質値が含まれます。",
            ReasonCode.LOW_QUALITY_GOVERNING_LAW.value: "準拠法候補が低品質です。",
            ReasonCode.LOW_QUALITY_JURISDICTION.value: "管轄裁判所候補が低品質です。",
            ReasonCode.LOW_QUALITY_EXPIRATION_DATE.value: "終了日候補が低品質です。",
            ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value: "効力発生日が締結日アンカー表現のみです。",
            ReasonCode.TABLE_CONTENT_UNPARSED.value: "表形式の内容が未構造化です。",
            ReasonCode.APPENDIX_DETECTED_NOT_PARSED.value: "別紙・付属資料が検出されましたが未解析です。",
        }
        return message_map.get(reason_code, "確認が必要な抽出シグナルがあります。")

    @staticmethod
    def _action_hint_for_reason(reason_code: str) -> str:
        hint_map = {
            ReasonCode.LOW_CONFIDENCE.value: "該当blockの原文と抽出値を目視確認してください。",
            ReasonCode.HIGH_OCR_RATIO.value: "OCR対象ページを優先レビューしてください。",
            ReasonCode.UNSTABLE_CLAUSE_SPLIT.value: "条見出し境界と分割結果を確認してください。",
            ReasonCode.MISSING_CONTRACT_TYPE.value: "表題と前文から契約類型を補完してください。",
            ReasonCode.MISSING_EFFECTIVE_DATE.value: "発効日条項または前文から日付を補完してください。",
            ReasonCode.MISSING_EXPIRATION_DATE.value: "契約期間条項から終了日を補完してください。",
            ReasonCode.MISSING_GOVERNING_LAW.value: "準拠法条項を確認し補完してください。",
            ReasonCode.MISSING_JURISDICTION.value: "管轄条項を確認し補完してください。",
            ReasonCode.OCR_FAILURE.value: "当該領域を再OCRまたは手入力補正してください。",
            ReasonCode.PARTIAL_EXTRACTION_FAILURE.value: "欠損領域を原本照合して補完してください。",
            ReasonCode.LOW_QUALITY_COUNTERPARTY.value: "当事者名の崩れ断片を除外し法人名を補正してください。",
            ReasonCode.LOW_QUALITY_GOVERNING_LAW.value: "準拠法条項の文を確認して値を補正してください。",
            ReasonCode.LOW_QUALITY_JURISDICTION.value: "裁判所名の完全表記を確認して補正してください。",
            ReasonCode.LOW_QUALITY_EXPIRATION_DATE.value: "終了日条項を確認し絶対日付か相対期間かを補完してください。",
            ReasonCode.ANCHOR_ONLY_EFFECTIVE_DATE.value: "締結日アンカーのため実日付が必要か確認してください。",
            ReasonCode.TABLE_CONTENT_UNPARSED.value: "表の主要セルを手入力で補完してください。",
            ReasonCode.APPENDIX_DETECTED_NOT_PARSED.value: "別紙・付属資料の必要項目を確認して補完してください。",
        }
        return hint_map.get(reason_code, "該当箇所を確認してください。")
