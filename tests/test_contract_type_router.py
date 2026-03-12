from __future__ import annotations

from contract_ingest.domain.enums import BlockType, ExtractMethod
from contract_ingest.domain.models import BBox, EvidenceBlock
from contract_ingest.normalize.contract_type_router import (
    ROUTE_LICENSE_OR_ITAKU,
    ROUTE_NDA,
    ROUTE_SERVICE,
    ROUTE_UNKNOWN,
    infer_contract_type,
    route_field_bias,
)


def _make_block(order: int, text: str) -> EvidenceBlock:
    y0 = 10.0 + order * 18.0
    return EvidenceBlock(
        page=1,
        block_id=f"p1_b{order:03d}",
        block_type=BlockType.TEXT,
        bbox=BBox(x0=20.0, y0=y0, x1=580.0, y1=y0 + 16.0),
        text=text,
        engine="native_text",
        extract_method=ExtractMethod.NATIVE_TEXT,
        confidence=0.95,
        searchable=True,
        reading_order=order,
        source_hash="sha256:test",
        pipeline_version="0.1.0",
    )


def test_contract_type_router_classifies_nda_service_license_and_unknown() -> None:
    nda_blocks = [_make_block(1, "秘密保持契約書"), _make_block(2, "秘密情報の開示および目的外利用の禁止")]
    service_blocks = [_make_block(1, "業務委託契約書"), _make_block(2, "成果物の検収および再委託の条件")]
    license_blocks = [_make_block(1, "利用許諾契約書"), _make_block(2, "ライセンスの使用許諾条件")]
    unknown_blocks = [_make_block(1, "基本合意"), _make_block(2, "当事者は協議する")]

    assert infer_contract_type(nda_blocks) == ROUTE_NDA
    assert infer_contract_type(service_blocks) == ROUTE_SERVICE
    assert infer_contract_type(license_blocks) == ROUTE_LICENSE_OR_ITAKU
    assert infer_contract_type(unknown_blocks) == ROUTE_UNKNOWN


def test_route_field_bias_changes_tail_priority_lightly_and_unknown_is_safe() -> None:
    assert route_field_bias(ROUTE_NDA, "governing_law") > 0.0
    assert route_field_bias(ROUTE_SERVICE, "effective_date") > 0.0
    assert route_field_bias(ROUTE_LICENSE_OR_ITAKU, "jurisdiction") > 0.0
    assert route_field_bias(ROUTE_LICENSE_OR_ITAKU, "governing_law") > 0.0
    assert route_field_bias(ROUTE_UNKNOWN, "jurisdiction") == 0.0
