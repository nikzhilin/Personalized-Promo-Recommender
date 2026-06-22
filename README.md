# Personalized Promo Recommender

Рекомендательная система персональных промо-предложений с uplift-моделированием,
оптимизацией маржи, снижением риска фрода и MLOps-инфраструктурой.

Подробная архитектура и план реализации, включая HDFS с двумя DataNode и дисковый
бюджет 40 ГБ, описаны в `documentation.docx`.

## Требования

- Python 3.11+
- GNU Make

Исходные CSV не входят в репозиторий. Разместите их в `data/raw/`:

```text
clients.csv
products.csv
purchases.csv
uplift_train.csv
uplift_test.csv
```

## Подготовка окружения

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

Все команды поддерживают выбор интерпретатора через переменную `PYTHON`:

```bash
make lint PYTHON=.venv/bin/python
make test PYTHON=.venv/bin/python
```

## Валидация исходных данных

Полная потоковая проверка всех CSV, включая `purchases.csv`:

```bash
make validate-data PYTHON=.venv/bin/python \
  FEATURE_CUTOFF=2019-03-01T00:00:00
```

Команда проверяет обязательные столбцы и значения, типы, уникальность ключей и ссылки
на клиентов и товары. Весь `purchases.csv` читается последовательно и не загружается в
память целиком.

Для быстрой проверки первых строк покупок и сохранения JSON-отчёта используйте:

```bash
.venv/bin/python -m spark_jobs.validate_raw_data \
  --data-dir data/raw \
  --feature-cutoff 2019-03-01T00:00:00 \
  --max-purchase-rows 10000 \
  --report /tmp/promo-validation-report.json
```

Raw-файл может содержать события после `feature_cutoff`: отчёт разделяет строки на
`rows_before_cutoff` и `rows_on_or_after_cutoff`. При проверке уже подготовленного
feature-набора включайте строгий режим, который трактует такие события как leakage:

```bash
.venv/bin/python -m spark_jobs.validate_raw_data \
  --data-dir data/raw \
  --feature-cutoff 2019-03-01T00:00:00 \
  --enforce-feature-cutoff
```

Процесс завершается кодом `0`, если ошибок контракта нет, и кодом `1`, если найдена
хотя бы одна ошибка. Диагностика агрегируется по типу проблемы и содержит до пяти
примеров, чтобы отчёт оставался ограниченным по размеру.
