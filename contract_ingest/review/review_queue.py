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

        for idx, signal in enumerate(assessment.signals, start=1):
            if signal.level.value == "warning":
                warning_count += 1
            if signal.level.value == "critical":
                critical_count += 1

            message = f"{signal.message} 対応方針: {signal.action_hint}"
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
                }
            )

        payload = {
            "doc_id": doc_id,
            "review_required": assessment.review_required,
            "items": items,
            "summary": {
                "warning_count": warning_count,
                "critical_count": critical_count,
            },
        }

        return ReviewQueueResult(payload=payload)
