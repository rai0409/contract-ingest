from __future__ import annotations

from pathlib import Path

import fitz

from contract_ingest.extract.layout_ppstructure import PPStructureLayoutAdapter


def test_ppstructure_bbox_normalization_from_polygon() -> None:
    bbox = PPStructureLayoutAdapter._to_bbox([[10, 20], [60, 20], [60, 80], [10, 80]])

    assert bbox is not None
    assert bbox.x0 == 10.0
    assert bbox.y0 == 20.0
    assert bbox.x1 == 60.0
    assert bbox.y1 == 80.0


def test_ppstructure_parse_page_output_normalizes_fields() -> None:
    adapter = PPStructureLayoutAdapter()
    raw = [
        {
            "type": "text",
            "bbox": [10, 20, 110, 60],
            "score": 0.9,
            "res": [{"text": "第1条"}, {"text": "目的"}],
        }
    ]

    blocks = adapter._parse_page_output(page_no=1, raw_result=raw)

    assert len(blocks) == 1
    assert blocks[0].label == "text"
    assert blocks[0].text == "第1条\n目的"


def test_ppstructure_analyze_pdf_fails_gracefully_on_init_error(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "sample.pdf"
    doc = fitz.open()
    doc.new_page(width=200, height=200)
    doc.save(pdf_path)
    doc.close()

    adapter = PPStructureLayoutAdapter()

    def _raise() -> object:
        raise RuntimeError("init failed")

    monkeypatch.setattr(adapter, "_get_engine", _raise)
    result = adapter.analyze_pdf(pdf_path)

    assert result.blocks == []
    assert any("ppstructure init failed" in issue for issue in result.issues)
