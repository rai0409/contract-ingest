# contract-ingest MVP Plan Complete

## 1. 文書の目的
本書は、契約書 PDF を対象とした ingestion 基盤 `contract-ingest` の MVP 方針を確定するための計画書である。

目的は、PDF 契約書を **native text 優先 / OCR fallback** で解析し、条文単位・証跡付きで構造化し、既存 brain / retrieval 基盤へ疎結合で投入できるようにすることにある。

本書は次を固定する。

- 何を MVP に含めるか
- 何を MVP から除外するか
- なぜ repo / サービスを分離するか
- 何を差別化軸として戦うか
- どの OCR / PDF 処理戦略を採用するか
- どの成果物を出せば MVP 完了とみなすか
- どの順序で実装するか

詳細なスキーマ、モジュール責務、CLI、テスト観点は `contract_ingest_detailed_spec_complete.md` に委譲する。

---

## 2. プロダクト定義
### 2.1 何を作るか
作るものは「OCR SaaS」ではない。

作るものは、**契約書を条文単位・証跡付きでナレッジ化する contract ingestion platform** である。

### 2.2 中核価値
MVP の中核価値は以下の 3 点とする。

1. **証跡性**  
   どのページのどの領域から、どのエンジンで、どの信頼度で、どの pipeline version により抽出されたかを残す。

2. **法務運用に直結する構造化**  
   OCR text の単純返却ではなく、契約類型・当事者・期間・自動更新・解約通知・準拠法・管轄など、業務項目へ変換する。

3. **不確実性を扱える運用性**  
   低信頼箇所だけを review queue に送る。全件人手確認は前提にしない。

### 2.3 やらないこと
MVP は以下を目標にしない。

- 契約レビュー自動判定の完成
- 手書き文字の高精度最適化
- 契約ライフサイクル管理全体
- 締結・承認・ワークフロー UI
- 法務リスク評価の完全自動化
- すべての契約類型への即時対応

---

## 3. システム境界
### 3.1 repo / サービス境界
`contract-ingest` は新規 repo とする。

推奨配置:

```text
/home/rai/
  chatbot/
  contract-ingest/
```

### 3.2 既存 brain との境界
既存 brain / retrieval 基盤とは、**JSON / JSONL 契約** で接続する。

- `document.json`: 原本再現・監査用
- `chunks.jsonl`: retrieval / embedding 用
- `review.json`: review queue 用

brain 側へ渡す主成果物は `chunks.jsonl` とする。

### 3.3 なぜ分離するか
理由は以下。

- OCR / PDF 解析は依存が重く、CPU/GPU 負荷も高い
- retrieval / QA と責務が異なる
- 将来 OCR backend を差し替えやすい
- Docker イメージ・CI/CD を分離しやすい
- 既存 brain の安定運用を崩さない

---

## 4. 言語・技術方針
### 4.1 言語
中核実装言語は **Python** とする。

理由:

- OCR / document AI の主要 OSS が Python 中心
- PDF / 画像処理エコシステムが強い
- PaddleOCR との相性がよい
- 後続で Azure / Google adapter を足しやすい

### 4.2 OCR 方針
MVP の OCR 主軸は **PaddleOCR** とする。

処理戦略:

- native text first
- block / region 単位 OCR fallback
- page 丸ごと OCR は原則回避

将来の拡張:

- Azure OCR adapter
- Google OCR adapter
- Surya comparator

### 4.3 PDF 処理方針
最初に document / page の分類を行う。

- text-native
- scanned
- hybrid

契約書は 1 ファイル内で native text と scan が混在しうるため、**ページ単位、さらに block / region 単位で fallback 判定**を行う。

---

## 5. MVP 対象スコープ
### 5.1 入力
MVP で扱う入力は PDF のみ。

対象:

- テキスト PDF
- スキャン PDF
- 混在 PDF

非対象:

- 画像単体アップロード最適化
- Word 原本解析
- Excel / PowerPoint 契約書
- 手書き原本の本格最適化

### 5.2 契約類型
MVP で主対象とする契約類型:

- NDA（秘密保持契約）
- 業務委託契約
- 基本契約書

### 5.3 抽出対象フィールド
MVP で抽出する業務項目は以下。

- `contract_type`
- `counterparties`
- `effective_date`
- `expiration_date`
- `auto_renewal`
- `termination_notice_period`
- `governing_law`
- `jurisdiction`

### 5.4 出力成果物
MVP で必ず出す成果物:

- `document.json`
- `chunks.jsonl`
- `review.json`

---

## 6. 差別化戦略
### 6.1 1位: 証跡性
以下を保持する。

- page
- block_id
- bbox
- text
- engine
- confidence
- extract_method
- source_hash
- pipeline_version
- reason / warning

### 6.2 2位: 業務項目への構造化
全文検索用テキストだけでなく、法務・契約管理で直接使う項目へ正規化する。

### 6.3 3位: 低信頼のみ review
すべて人手確認しない。

以下で review 対象化する。

- low confidence
- OCR 比率が高い
- clause split 不安定
- 必須項目欠落
- レイアウト不安定

---

## 7. MVP 完了条件
MVP 完了は以下を満たしたときとする。

1. PDF 契約書を入力できる
2. native text 抽出が動作する
3. block / region 単位で PaddleOCR fallback が動作する
4. native text と OCR block を統合できる
5. reading order を付与できる
6. 条文分解が動作する
7. 抽出対象フィールドが rule-based で抽出できる
8. `document.json` を出力できる
9. `chunks.jsonl` を出力できる
10. `review.json` を出力できる
11. 低信頼のみ review として切り出せる
12. `chunks.jsonl` を既存 brain へ渡せる
13. 失敗時に structured reason を保持できる

---

## 8. MVP 非対象
以下は Phase 2 以降とする。

- Azure OCR adapter 本番利用
- Google OCR adapter 本番利用
- Surya comparator 実装
- 手書き本格対応
- table extraction 高度化
- review UI
- Web API 化
- benchmark CLI
- 契約レビュー自動判定
- 契約比較 / 差分判定
- 英文契約最適化

---

## 9. 高レベル処理フロー
```text
input.pdf
  -> classify document/page
  -> extract native text blocks
  -> detect weak/native-insufficient regions
  -> OCR fallback for selected regions
  -> merge blocks + reading order
  -> build evidence blocks
  -> split into clauses
  -> extract contract fields
  -> build retrieval chunks
  -> score review necessity
  -> write document.json / chunks.jsonl / review.json
```

---

## 10. 出力物の役割
### 10.1 document.json
原本再現・監査・再計算用。

### 10.2 chunks.jsonl
brain / embedding / retrieval 用。

### 10.3 review.json
低信頼箇所の review queue 用。

---

## 11. エラーと review の考え方
MVP では、すべての失敗を fatal にしない。

### 11.1 fatal error
処理を継続できない場合。

例:

- PDF を開けない
- ページレンダリング不可
- 出力先書き込み不可
- モデル初期化不可

### 11.2 recoverable error
文書全体処理は継続するが、一部領域に欠損が出る場合。

例:

- 一部 OCR 失敗
- 一部 block が文字化け
- 一部 clause split 不安定

### 11.3 review 対象
処理は成功だが、人手確認が望ましい場合。

例:

- confidence 低下
- 必須項目欠落
- OCR 比率高い
- 重要条文抽出不安定

---

## 12. 非機能要件
MVP で最低限守る。

- 再計算可能であること
- pipeline version を出力に保持すること
- source hash を保持すること
- ログに doc_id / page を含めること
- typed / structured に実装すること
- path は pathlib ベースにすること
- 失敗を黙殺しないこと

---

## 13. 実装優先順位
### Phase 0
- repo 作成
- 計画 / 詳細仕様固定
- サンプル契約書収集

### Phase 1-A
- domain / schema
- PDF分類
- native text 抽出
- PaddleOCR adapter
- layout / block merge
- evidence builder
- clause splitter
- field extractor

### Phase 1-B
- chunk builder
- review scorer
- review queue
- document/chunks/review writer
- ingest CLI

### Phase 2
- README / tests / Makefile / benchmark

### Phase 3
- Azure / Google adapter
- Surya comparator
- review 高度化
- table 強化
- API / UI

---

## 14. 受け入れ基準
最低限の受け入れ基準:

- 30〜50 件の契約書サンプルで最後まで走る
- 条文分割が大きく崩れない
- 主要 8 項目の抽出が実用域に達する
- `document.json / chunks.jsonl / review.json` が出る
- 低信頼だけが review に出る
- `chunks.jsonl` が brain 連携前提 metadata を持つ

---

## 15. 今後の判断原則
以後の設計判断は以下に従う。

1. OCR より ingestion platform を優先
2. retrieval と OCR を密結合しない
3. evidence を失わない
4. clause 単位を優先し、固定長 chunk は二次的手段とする
5. review を前提とした不確実性処理を行う
6. 手書き・クラウドOCRは Phase 2 以降で差し替え可能にしておく

