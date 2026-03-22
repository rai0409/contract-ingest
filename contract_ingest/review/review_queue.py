from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from contract_ingest.review.scorer import ReviewAssessment


@dataclass(frozen=True)
class ReviewQueueResult:
    payload: dict[str, Any]


class ReviewQueueBuilder:
    def build(self, doc_id: str, assessment: ReviewAssessment) -> ReviewQueueResult:
        items: list[dict[str, Any]] = []
        warning_count = 0
        critical_count = 0
        high_count = 0
        medium_count = 0
        low_count = 0

        for idx, signal in enumerate(assessment.signals, start=1):
            if signal.level.value == "warning":
                warning_count += 1
            if signal.level.value == "critical":
                critical_count += 1
            severity = self._to_compare_severity(signal.reason_code, signal.level.value)
            if severity == "high":
                high_count += 1
            elif severity == "medium":
                medium_count += 1
            else:
                low_count += 1

            message = f"{signal.message} 対応方針: {signal.action_hint}"
            evidence = {
                "candidate_value": signal.candidate_value,
                "why_rejected": signal.why_rejected,
                "snippet": signal.snippet,
                "bbox": signal.bbox,
                "page_refs": signal.page_refs,
                "block_ids": signal.block_ids,
            }
            items.append(
                {
                    "review_id": f"rev_{idx:04d}",
                    "level": signal.level.value,
                    "reason_codes": [signal.reason_code],
                    "message": message,
                    "page_refs": signal.page_refs,
                    "block_ids": signal.block_ids,
                    "field_names": signal.field_names,
                    "field": signal.field,
                    "candidate_value": signal.candidate_value,
                    "why_rejected": signal.why_rejected,
                    "page": signal.page,
                    "bbox": signal.bbox,
                    "snippet": signal.snippet,
                    "confidence": signal.confidence,
                    "suggested_action": signal.action_hint,
                    "type": signal.reason_code,
                    "severity": severity,
                    "evidence": evidence,
                    "suggested_fix": signal.action_hint,
                }
            )

        payload = {
            "doc_id": doc_id,
            "review_required": assessment.review_required,
            "items": items,
            "summary": {
                "warning_count": warning_count,
                "critical_count": critical_count,
                "high_count": high_count,
                "medium_count": medium_count,
                "low_count": low_count,
                "compare_ready": high_count == 0,
            },
        }

        return ReviewQueueResult(payload=payload)

    @staticmethod
    def _to_compare_severity(reason_code: str, level: str) -> str:
        high_reasons = {
            "UNSTABLE_CLAUSE_SPLIT",
            "OCR_FAILURE",
            "SECTION_BOUNDARY_UNCERTAIN",
        }
        medium_reasons = {
            "MISSING_TITLE",
            "PARTIAL_COUNTERPARTY",
            "MISSING_GOVERNING_LAW",
            "MISSING_JURISDICTION",
            "ANCHOR_ONLY_EFFECTIVE_DATE",
            "LOW_QUALITY_COUNTERPARTY",
            "LOW_QUALITY_GOVERNING_LAW",
            "LOW_QUALITY_JURISDICTION",
        }
        if reason_code in high_reasons:
            return "high"
        if reason_code in medium_reasons:
            return "medium"
        if level == "critical":
            return "high"
        if level == "warning":
            return "medium"
        return "low"
