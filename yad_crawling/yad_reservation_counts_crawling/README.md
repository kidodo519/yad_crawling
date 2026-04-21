# yad_reservation_count_crawling

`yad_reservation_count_crawling.py` を PyInstaller で exe 化するためのメモです。

## 前提

- カレントディレクトリを本フォルダに移動して実行すること
  - `yad_crawling/yad_reservation_counts_crawling`
- 実行時に `config.yaml` は exe と同じディレクトリに配置すること

## Bash コマンド（onedir 推奨）

```bash
pyinstaller \
  --clean \
  --noconfirm \
  --onedir \
  --console \
  --name yad_reservation_count_crawling \
  --collect-all bs4 \
  --collect-all soupsieve \
  --collect-all charset_normalizer \
  --collect-all certifi \
  yad_reservation_count_crawling.py
```

生成物:

- `dist/yad_reservation_count_crawling/`

## Bash コマンド（onefile）

```bash
pyinstaller \
  --clean \
  --noconfirm \
  --onefile \
  --console \
  --name yad_reservation_count_crawling \
  --collect-all bs4 \
  --collect-all soupsieve \
  --collect-all charset_normalizer \
  --collect-all certifi \
  yad_reservation_count_crawling.py
```

生成物:

- `dist/yad_reservation_count_crawling`

## 実行例

onedir の場合:

```bash
cd dist/yad_reservation_count_crawling
./yad_reservation_count_crawling
```

onefile の場合:

```bash
cd dist
./yad_reservation_count_crawling
```

