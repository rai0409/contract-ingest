from __future__ import annotations

from dataclasses import dataclass, field

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
        }
        return hint_map.get(reason_code, "該当箇所を確認してください。")
