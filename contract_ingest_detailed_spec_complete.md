# contract-ingest Detailed Specification Complete

## 1. 文書の目的
本書は `contract-ingest` の MVP 中核実装を、そのまま開発に着手できる粒度で定義する詳細仕様書である。

対象は、契約書 PDF を native text first / OCR fallback で解析し、条文単位・証跡付きで構造化し、以下を出力するパイプラインである。

- `document.json`
- `chunks.jsonl`
- `review.json`

本書は以下を固定する。

- モジュール責務
- ディレクトリ構成
- ドメインモデル
- 入出力 schema
- エラー方針
- review 方針
- CLI 振る舞い
- MVP タスク分解
- 受け入れ条件

---

## 2. 前提
- repo パス: `/home/rai/contract-ingest`
- 言語: Python
- OCR 主軸: PaddleOCR
- existing brain とは `chunks.jsonl` 契約で接続
- Linux / WSL2 を主対象環境とする

---

## 3. ディレクトリ構成
```text
/home/rai/contract-ingest/
  pyproject.toml
  .gitignore

  contract_ingest/
    __init__.py
    config.py

    domain/
      enums.py
      models.py
      schemas.py

    extract/
      pdf_classifier.py
      native_text.py
      ocr_base.py
      ocr_paddle.py
      layout.py
      block_merger.py

    normalize/
      clause_splitter.py
      field_extractor.py
      evidence_builder.py
      chunk_builder.py

    review/
      scorer.py
      review_queue.py

    export/
      write_document_json.py
      write_chunks_jsonl.py
      write_review_json.py

    utils/
      hash.py
      time.py
      text.py
      image.py
      logging.py

    cli/
      ingest_contract.py
```

---

## 4. 各モジュール責務
### 4.1 `config.py`
責務:

- 環境変数 / デフォルト設定読込
- pipeline version 管理
- OCR / review 閾値管理
- 入出力パス設定

最低限の設定項目:

- `PIPELINE_VERSION`
- `OCR_ENGINE_NAME`
- `LOW_CONFIDENCE_THRESHOLD`
- `HIGH_OCR_RATIO_THRESHOLD`
- `MIN_NATIVE_TEXT_CHARS`
- `MAX_GARBLED_RATIO`
- `OUTPUT_INDENT`

### 4.2 `domain/enums.py`
責務:

- Enum 群の定義

例:

- `DocumentKind`
- `BlockType`
- `ExtractMethod`
- `ReviewLevel`
- `ReasonCode`
- `ChunkType`

### 4.3 `domain/models.py`
責務:

- dataclass / pydantic model による内部ドメインモデル定義

### 4.4 `domain/schemas.py`
責務:

- writer 出力用 validation model
- `document.json`, `chunks.jsonl`, `review.json` のバリデーション定義

### 4.5 `extract/pdf_classifier.py`
責務:

- document / page を `text-native`, `scanned`, `hybrid` に分類
- 判定理由を返す

### 4.6 `extract/native_text.py`
責務:

- PyMuPDF 等を用いたページ / block 単位 native text 抽出
- bbox、文字数、抽出補助情報を保持

### 4.7 `extract/ocr_base.py`
責務:

- OCR adapter interface
- OCR request / response model

### 4.8 `extract/ocr_paddle.py`
責務:

- PaddleOCR 実呼び出し
- region 単位 OCR 実行
- text / confidence / bbox 返却

### 4.9 `extract/layout.py`
責務:

- ページ / block / region の分類
- OCR 対象領域決定
- native text 十分性判定

### 4.10 `extract/block_merger.py`
責務:

- native text block と OCR block を統合
- reading order 付与
- searchable 判定

### 4.11 `normalize/evidence_builder.py`
責務:

- block から evidence block へ変換
- engine / confidence / source_hash / pipeline_version を付与

### 4.12 `normalize/clause_splitter.py`
責務:

- evidence block 群を clause 単位へ束ねる
- clause no / title / span を付与

### 4.13 `normalize/field_extractor.py`
責務:

- rule-based で業務項目抽出
- confidence / reason を返す

### 4.14 `normalize/chunk_builder.py`
責務:

- clause / field 結果から retrieval chunk を構築
- brain 連携 metadata を付与

### 4.15 `review/scorer.py`
責務:

- review 必要性スコア計算
- reason code 算出

### 4.16 `review/review_queue.py`
責務:

- review item 作成
- level / reasons / summary を構築

### 4.17 `export/write_document_json.py`
責務:

- `document.json` 出力
- validation を経て保存

### 4.18 `export/write_chunks_jsonl.py`
責務:

- `chunks.jsonl` 出力
- 1 行 1 chunk で validation 後保存

### 4.19 `export/write_review_json.py`
責務:

- `review.json` 出力
- validation を経て保存

### 4.20 `cli/ingest_contract.py`
責務:

- CLI 引数受理
- extract -> normalize -> review -> export を接続
- doc_id 生成
- 例外ハンドリング
- structured logging

---

## 5. ドメインモデル
以下は内部的に保持すべき最小モデルである。

### 5.1 BBox
```json
{ "x0": 0.0, "y0": 0.0, "x1": 100.0, "y1": 20.0 }
```

### 5.2 NativeTextBlock
```json
{
  "page": 1,
  "block_id": "p1_b001",
  "bbox": {"x0": 10, "y0": 20, "x1": 500, "y1": 80},
  "text": "秘密保持契約書",
  "char_count": 7,
  "garbled_ratio": 0.0,
  "extract_method": "native_text",
  "searchable": true
}
```

### 5.3 OCRBlock
```json
{
  "page": 2,
  "block_id": "p2_o003",
  "bbox": {"x0": 12, "y0": 120, "x1": 520, "y1": 170},
  "text": "甲および乙は以下の通り合意する。",
  "confidence": 0.93,
  "engine": "paddleocr",
  "extract_method": "ocr"
}
```

### 5.4 EvidenceBlock
```json
{
  "page": 2,
  "block_id": "p2_b010",
  "block_type": "text",
  "bbox": {"x0": 12, "y0": 120, "x1": 520, "y1": 170},
  "text": "甲および乙は以下の通り合意する。",
  "engine": "paddleocr",
  "extract_method": "ocr",
  "confidence": 0.93,
  "searchable": true,
  "reading_order": 10,
  "source_hash": "sha256:...",
  "pipeline_version": "0.1.0"
}
```

### 5.5 ClauseUnit
```json
{
  "clause_id": "clause_005",
  "clause_no": "第5条",
  "clause_title": "再委託",
  "text": "乙は甲の事前承諾なく再委託してはならない。",
  "page_start": 3,
  "page_end": 4,
  "block_ids": ["p3_b012", "p3_b013"],
  "evidence_refs": [
    {"page": 3, "block_id": "p3_b012"}
  ]
}
```

### 5.6 ExtractedField
```json
{
  "field_name": "governing_law",
  "value": "日本法",
  "confidence": 0.88,
  "reason": "matched_rule_governing_law_01",
  "evidence_refs": [
    {"page": 7, "block_id": "p7_b008"}
  ]
}
```

### 5.7 ReviewItem
```json
{
  "review_id": "rev_0001",
  "level": "warning",
  "reason_codes": ["LOW_CONFIDENCE", "MISSING_EXPIRATION_DATE"],
  "message": "契約終了日が抽出できず、一部 block の confidence が低い。",
  "page_refs": [1, 4],
  "block_ids": ["p4_b009"],
  "field_names": ["expiration_date"]
}
```

---

## 6. 出力 schema

## 6.1 `document.json`
### 6.1.1 役割
監査 / 再処理 / 原本再現用。

### 6.1.2 最上位構造
```json
{
  "doc_id": "contract_xxx",
  "source_file": "nda_001.pdf",
  "document_kind": "hybrid",
  "pipeline_version": "0.1.0",
  "source_hash": "sha256:...",
  "pages": [...],
  "blocks": [...],
  "clauses": [...],
  "fields": {...},
  "warnings": [...],
  "errors": [...]
}
```

### 6.1.3 必須項目
- `doc_id`: string
- `source_file`: string
- `document_kind`: enum[`text-native`, `scanned`, `hybrid`]
- `pipeline_version`: string
- `source_hash`: string
- `pages`: array
- `blocks`: array
- `clauses`: array
- `fields`: object
- `warnings`: array
- `errors`: array

### 6.1.4 `pages[]`
```json
{
  "page": 1,
  "page_kind": "text-native",
  "native_text_char_count": 350,
  "ocr_ratio": 0.1,
  "classification_reason": "sufficient_native_text"
}
```

### 6.1.5 `blocks[]`
各 block は以下必須。

- `page`: int
- `block_id`: string
- `block_type`: enum[`text`, `table`, `header`, `footer`, `image`, `signature_area`, `stamp_area`, `other`]
- `bbox`: object
- `text`: string
- `engine`: string
- `extract_method`: enum[`native_text`, `ocr`, `hybrid`]
- `confidence`: float | null
- `searchable`: bool
- `reading_order`: int
- `source_hash`: string
- `pipeline_version`: string

## 6.2 `chunks.jsonl`
### 6.2.1 役割
brain / embedding / retrieval 用。

### 6.2.2 1 行構造
```json
{
  "id": "contract_xxx__chunk_0012",
  "text": "第5条（再委託）乙は甲の事前承諾なく再委託してはならない。",
  "metadata": {
    "doc_id": "contract_xxx",
    "chunk_index": 12,
    "type": "clause",
    "quality": "hybrid",
    "searchable": 1,
    "clause_no": "第5条",
    "clause_title": "再委託",
    "source_pages": [3, 4],
    "block_ids": ["p3_b012", "p3_b013"],
    "evidence_refs": [
      {"page": 3, "block_id": "p3_b012", "bbox": {"x0": 10, "y0": 20, "x1": 500, "y1": 80}, "confidence": 0.98, "engine": "native_text"}
    ],
    "contract_type": "業務委託契約"
  }
}
```

### 6.2.3 metadata 必須項目
- `doc_id`: string
- `chunk_index`: int
- `type`: enum[`clause`, `schedule`, `table`, `preamble`, `appendix`, `other`]
- `quality`: enum[`native_text`, `ocr`, `hybrid`]
- `searchable`: int (0 or 1)
- `clause_no`: string | null
- `clause_title`: string | null
- `source_pages`: array[int]
- `block_ids`: array[string]
- `evidence_refs`: array[object]
- `contract_type`: string | null

## 6.3 `review.json`
### 6.3.1 役割
review queue 用。

### 6.3.2 最上位構造
```json
{
  "doc_id": "contract_xxx",
  "review_required": true,
  "items": [...],
  "summary": {
    "warning_count": 2,
    "critical_count": 0
  }
}
```

### 6.3.3 `items[]`
各 item は以下必須。

- `review_id`: string
- `level`: enum[`info`, `warning`, `critical`]
- `reason_codes`: array[string]
- `message`: string
- `page_refs`: array[int]
- `block_ids`: array[string]
- `field_names`: array[string]

---

## 7. 型と厳格ルール
### 7.1 bbox
- `x0`, `y0`, `x1`, `y1` は float
- `x0 < x1`, `y0 < y1`

### 7.2 confidence
- 範囲: `0.0 <= confidence <= 1.0`
- native text で信頼度が定量化不可の場合は `null` 可

### 7.3 searchable
- 内部モデルは bool
- `chunks.jsonl.metadata.searchable` は brain 互換のため int(0/1) 可

### 7.4 source_pages
- 重複禁止
- 昇順ソート

### 7.5 block_ids
- 重複禁止
- 出現順維持

---

## 8. PDF分類仕様
### 8.1 document/page 分類
分類種別:

- `text-native`
- `scanned`
- `hybrid`

### 8.2 判定指標
最低限使う指標:

- native text 文字数
- 文字化け率
- 画像占有率
- text block 数
- OCR 必要領域割合

### 8.3 判定例
- 文字数十分、文字化け率低い → `text-native`
- text layer ほぼなし、画像主体 → `scanned`
- 一部 text layer あり + 一部 scan → `hybrid`

---

## 9. native text 抽出仕様
- ページ / block 単位で抽出
- bbox を保持
- 抽出 text は正規化前生文字列も保持可能にする
- 補助情報として `char_count`, `garbled_ratio` を持つ

最低限の文字化け指標例:

- 制御文字率
- replacement char 率
- 非期待文字連続率

---

## 10. OCR fallback 仕様
### 10.1 基本方針
- native text first
- page 丸ごと OCR は例外的対応
- block / region 単位 OCR を基本とする

### 10.2 OCR 対象条件
例:

- text が空
- char_count が閾値未満
- garbled_ratio が閾値超過
- image region と判定

### 10.3 OCR 出力必須項目
- text
- bbox
- confidence
- engine
- extract_method
- page
- parent region 情報（必要なら）

---

## 11. block 統合と reading order
### 11.1 統合方針
- native block を優先
- ただし品質不足 block は OCR 結果で補完
- native と OCR 重複時は採用理由を保持

### 11.2 reading order
最低限、`page -> y0 -> x0` ベースで順序付けする。
必要に応じて header/footer 分離を考慮する。

### 11.3 searchable
以下は検索対象外候補。

- 空文字 block
- ノイズ block
- 署名欄の空欄
- 印影のみ block
- 連続 page number のみ

---

## 12. evidence 構築仕様
すべての条文 / field / chunk は evidence に辿れる必要がある。

### 12.1 evidence 必須項目
- `page`
- `block_id`
- `bbox`
- `text`
- `engine`
- `confidence`
- `source_hash`
- `pipeline_version`

### 12.2 evidence_refs
`chunk` や `field` には `evidence_refs` を持たせる。

---

## 13. 条文分解仕様
### 13.1 対象パターン
最低限扱う:

- `第1条`
- `第2条（定義）`
- `第十条`
- `附則`
- `別紙`
- `別表`

### 13.2 分解ルール
- 見出し block を clause 起点にする
- 次の clause 起点までを束ねる
- 途中で page を跨いでも clause 継続を許可する

### 13.3 不安定判定
以下は review 候補:

- 見出しが連続しすぎる
- clause text が極端に短い
- 条番号が逆行する
- 別紙境界が曖昧

---

## 14. 契約項目抽出仕様
### 14.1 抽出対象
- `contract_type`
- `counterparties`
- `effective_date`
- `expiration_date`
- `auto_renewal`
- `termination_notice_period`
- `governing_law`
- `jurisdiction`

### 14.2 抽出方式
MVP は rule-based を主とする。

### 14.3 ルール例
- 契約類型: 表題 / 冒頭から推定
- 当事者: 前文 / 甲乙定義 / 署名欄近辺
- 準拠法: `本契約.*日本法`
- 管轄: `.*裁判所.*専属的合意管轄`

### 14.4 抽出結果
各フィールドは以下を持つ。

- `value`
- `confidence`
- `reason`
- `evidence_refs`

### 14.5 抽出失敗
未抽出でも `null` を許可するが、必要に応じ review reason を追加する。

---

## 15. review 仕様
### 15.1 reason code 例
- `LOW_CONFIDENCE`
- `HIGH_OCR_RATIO`
- `UNSTABLE_CLAUSE_SPLIT`
- `MISSING_CONTRACT_TYPE`
- `MISSING_EFFECTIVE_DATE`
- `MISSING_EXPIRATION_DATE`
- `MISSING_JURISDICTION`
- `MISSING_GOVERNING_LAW`
- `OCR_FAILURE`
- `PARTIAL_EXTRACTION_FAILURE`

### 15.2 level 判定例
- `critical`: fatal に近い欠損
- `warning`: 要確認
- `info`: 補助的通知

### 15.3 review 発火条件
最低限:

- confidence が閾値未満
- OCR 比率が閾値超過
- clause split 不安定
- 必須項目欠落

---

## 16. エラー方針
### 16.1 fatal error
exit code 非 0 を返す。

例:

- PDF open failure
- output write failure
- OCR engine initialization failure

### 16.2 recoverable error
処理継続し、`document.json.errors` や `review.json` に残す。

### 16.3 warning
review / warning として出す。

---

## 17. CLI 仕様
### 17.1 コマンド
```bash
python -m contract_ingest.cli.ingest_contract --input /path/to/input.pdf --output-dir /path/to/output
```

### 17.2 引数
- `--input`: PDF path, required
- `--output-dir`: output directory, required
- `--doc-id`: optional
- `--log-level`: optional

### 17.3 出力ディレクトリ
```text
<output-dir>/
  document.json
  chunks.jsonl
  review.json
```

### 17.4 exit code
- `0`: success / warning 含む成功
- `1`: fatal error

---

## 18. brain 連携仕様
### 18.1 `chunks.jsonl` 契約
brain 側に必須の metadata:

- `doc_id`
- `chunk_index`
- `type`
- `quality`
- `searchable`
- `clause_no`
- `clause_title`
- `source_pages`
- `block_ids`
- `evidence_refs`
- `contract_type`

### 18.2 互換性方針
brain 側の大規模変更は前提にしない。
`contract-ingest` が metadata を合わせる。

---

## 19. 非機能要件
### 19.1 再現性
- `pipeline_version` を必須保持
- `source_hash` を必須保持

### 19.2 ログ
最低限ログに含める:

- `doc_id`
- `page`
- `block_id`（必要時）
- `reason_code`

### 19.3 保守性
- pathlib 使用
- typed 実装
- 例外握りつぶし禁止
- validation 前提 writer

---

## 20. MVP タスク分解
### 20.1 前半
- `pyproject.toml`
- `config.py`
- `domain/*`
- `extract/*`
- `normalize/clause_splitter.py`
- `normalize/field_extractor.py`
- `normalize/evidence_builder.py`
- `utils/*`

### 20.2 後半
- `normalize/chunk_builder.py`
- `review/*`
- `export/*`
- `cli/ingest_contract.py`

### 20.3 次回
- README
- tests
- Makefile
- benchmark

---

## 21. 受け入れ基準
以下を満たしたら MVP 中核受け入れ可能。

- import 解決する
- CLI 起動できる
- PDF 1 件で 3 出力を生成できる
- `document.json` が validation 通過
- `chunks.jsonl` 各行が validation 通過
- `review.json` が validation 通過
- chunk metadata が brain 契約を満たす
- review reason が structured に出る

---

## 22. 今後の追加仕様候補
- JSON Schema 厳格版別紙
- error code 一覧
- fixture 契約書セット
- benchmark 指標定義
- OCR backend comparator

