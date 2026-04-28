# Amazon PPC Analyzer — Web Tool

Веб-версия PPC-анализатора. Drop xlsx → получи decisions.csv с готовым планом действий по каждому search term.

**Деплой:** https://ppc-analyzer-puce.vercel.app

## Стек

- Python 3.10+ / Flask 3 (Vercel serverless function)
- pandas + openpyxl (анализ xlsx)
- Vanilla JS frontend (без фреймворков)

## Структура

```
ppc-analyzer/
├── api/
│   └── analyze.py     # Flask endpoint: POST /api/analyze
├── index.html         # Frontend (drop zone + summary + download)
├── requirements.txt   # Python deps
├── vercel.json        # Vercel routing config
└── .gitignore
```

## Локальный запуск

```bash
# 1. Установить зависимости
python3 -m pip install -r requirements.txt

# 2. Запустить Flask dev server
python3 api/analyze.py

# 3. Открыть http://localhost:5000 в браузере
```

## Деплой

Push в `main` → Vercel автодеплой.

```bash
git add .
git commit -m "..."
git push
```

## API

### POST /api/analyze

**Request:** `multipart/form-data` с полем `file` (xlsx или csv).

**Response 200:** JSON
```json
{
  "summary": {
    "meta": { "date_range": [...], "num_campaigns": ..., ... },
    "overall": { "spend": ..., "sales": ..., "acos": ..., ... },
    "products": [...],
    "tier_counts": { "high_clicks": N, "gray_zone": N, "low_data": N },
    "decision_counts": { "wait_for_data": 951, ... },
    "top_winners": [...],
    "top_bleeders_high_clicks": [...]
  },
  "csv_content": "search_term,match,...",
  "filename": "<original_name>__decisions.csv"
}
```

**Errors:**
- `400` — файл не приложен / пустой / не Search Term Report
- `413` — файл больше 4 MB (Vercel Hobby limit)
- `500` — внутренняя ошибка анализа

## Логика анализа

Логика портирована из Claude Code skill `amazon-ppc-analyzer`. Кратко:

- Детект продуктов по ASIN из имён кампаний, группировка вариаций
- Тиры bleeders: HIGH_CLICKS (≥20 кликов), GRAY_ZONE (10-19), LOW_DATA (<10)
- ASIN-таргеты — отдельная логика (variation conflict detection)
- Per-row decision (15 категорий действий) + reason

Подробнее — в [SKILL repo](../amazon-ppc-analyzer-skill).

## Лимиты

- Файл: 4 MB (Vercel Hobby)
- Время: 30 секунд (`maxDuration` в vercel.json)
- Память: 1024 MB

## Версия

v1.0.0 — clean rewrite на основе skill methodology (2026-04-28)
