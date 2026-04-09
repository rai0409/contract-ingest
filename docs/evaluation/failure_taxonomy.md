# Failure Taxonomy

## Philosophy

This project is review-oriented.  
A review flag is often a safety feature, not a system failure.

The primary risk to control is **false clean**: cases where the pipeline emits no review despite unresolved ambiguity.

## Core Review/Failure Types

| Category | Signal / Source | What it means | Why it matters |
|---|---|---|---|
| Missing required field | `MISSING_CONTRACT_TYPE`, `MISSING_EFFECTIVE_DATE`, `MISSING_EXPIRATION_DATE`, `MISSING_GOVERNING_LAW`, `MISSING_JURISDICTION` | Required field unresolved | Downstream automation should not assume completeness |
| Low-quality field | `LOW_QUALITY_GOVERNING_LAW`, `LOW_QUALITY_JURISDICTION`, `LOW_QUALITY_EXPIRATION_DATE`, `LOW_QUALITY_COUNTERPARTY` | Candidate found but failed quality gate | Prevents weak normalization from being treated as trusted output |
| Partial field acceptance | `PARTIAL_COUNTERPARTY`, `ANCHOR_ONLY_EFFECTIVE_DATE` | Field has constrained semantic value | Explicitly preserves reviewability for edge cases |
| Low confidence | `LOW_CONFIDENCE` | Confidence below configured threshold | Indicates weak extraction evidence |
| Clause split instability | `UNSTABLE_CLAUSE_SPLIT`, `CONSECUTIVE_CLAUSE_HEADINGS`, `SHORT_CLAUSE_TEXT` | Clause segmentation likely degraded | Field extraction quality and evidence localization become less reliable |
| Clause numbering drift | `REVERSED_CLAUSE_NUMBER` | Article numbers appear to go backward | Strong indicator of segmentation/ordering error |
| Section boundary ambiguity | `SECTION_BOUNDARY_UNCERTAIN` (with `category_counts`, `transition_counts`, `samples`) | Section transition required heuristic fallback | Useful for diagnosing appendix/form/signature/tail-restart boundary risk |
| Appendix boundary ambiguity | `APPENDIX_BOUNDARY_AMBIGUOUS` | Appendix/material boundary remains unclear | Can contaminate main-contract extraction |

## Leakage and Contamination Buckets for Baseline Reporting

These are baseline metric buckets built from emitted review data and details:

- `header_footer_leakage_count`
- `signature_leakage_count`
- `appendix_form_contamination_count`

These should be interpreted as structural quality indicators, not standalone field errors.

## Placeholder-Only Date Cases

Date outputs can contain placeholders (for example `令和○○年○○月○○日`, `年 月 日`).  
These are intentionally kept reviewable, and should be counted as `placeholder`, not silently converted to exact dates.

## Review Needed vs False Clean

For baseline operations:

- `review needed`: case where ambiguity or low quality is present and review is expected.
- `false clean`: no review emitted even though review was needed.

`false clean` is the most important safety metric for legal/business ingestion workflows.

## Practical Use

When investigating regressions:

1. Check structural signals first (`UNSTABLE_CLAUSE_SPLIT`, `SECTION_BOUNDARY_UNCERTAIN`, reversed numbering).
2. Then inspect field quality/missing signals.
3. Confirm that reviewer-facing evidence (`evidence_refs`, review details) remains intact.

This keeps improvements aligned with reliable, auditable extraction behavior.
