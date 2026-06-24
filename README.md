# Personalized Promo Recommender

Рекомендательная система персональных промо-предложений с uplift-моделированием,
оптимизацией маржи, снижением риска фрода и MLOps-инфраструктурой.

Подробная архитектура и план реализации, включая HDFS с двумя DataNode и дисковый
бюджет 40 ГБ, описаны в `documentation.docx`.

## Требования

- Python 3.11+
- GNU Make
- Docker с Compose plugin

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

## HDFS и Bronze-слой

Текущий инфраструктурный инкремент поднимает один NameNode, два DataNode и Spark
standalone. HDFS использует два отдельных persistent volume, `replication=2` и block
size 64 МБ. Web UI и RPC-порты не публикуются наружу Docker-сети.

Поднять сервисы и инициализировать `/promo`:

```bash
make data-up DATA_DIR=data/raw
make hdfs-bootstrap
```

Загрузить малые справочники в типизированный Bronze Parquet:

```bash
make ingest-bronze DATA_DIR=data/raw INGEST_DATE=2026-07-01
```

Команда обрабатывает `clients.csv`, `products.csv`, `uplift_train.csv` и
`uplift_test.csv`. Повтор с тем же `INGEST_DATE` безопасно заменяет соответствующие
партиции через staging и HDFS rename.

Покупки загружаются отдельной командой, так как файл содержит 45,8 млн строк и требует
помесячной публикации:

```bash
# Все найденные месяцы
make ingest-purchases DATA_DIR=data/raw

# Выборочный backfill без изменения остальных месяцев
make ingest-purchases DATA_DIR=data/raw PURCHASE_MONTHS="2019-01 2019-02"
```

Job строит партиции `bronze/purchases/purchase_month=YYYY-MM`, сохраняет исходные
бизнес-значения без Silver-фильтрации и отклоняет невалидные типы до публикации.
Полная FK-проверка остаётся явным отдельным шагом `make validate-data`, чтобы каждый
ingest не выполнял скрытый дополнительный scan файла размером 4,2 ГБ.

## Silver-слой

Silver dimensions строятся из конкретного Bronze ingest, после чего покупки очищаются
с использованием опубликованных client/product ключей:

```bash
make build-silver \
  BRONZE_INGEST_DATE=2026-07-01 \
  SNAPSHOT_DATE=2026-07-01
```

Jobs можно запускать независимо:

```bash
make build-silver-dimensions \
  BRONZE_INGEST_DATE=2026-07-01 SNAPSHOT_DATE=2026-07-01

make clean-silver-purchases \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  SNAPSHOT_DATE=2026-07-01 \
  PURCHASE_MONTHS="2019-01 2019-02"
```

Dimensions схлопывают идентичные primary-key дубли, а конфликтующие ключи сохраняют в
`silver/rejects/`. Uplift с отсутствующим Silver client завершает job без частичной
публикации. Экстремальные age и спорные client dates сохраняются и учитываются в DQ
metadata; их исправление относится к feature layer.

Purchases с неизвестным клиентом/товаром, неположительным количеством или датой после
конца `SNAPSHOT_DATE` отправляются в rejects. Пустой или отрицательный
`trn_sum_from_iss` не удаляет взаимодействие, но выставляет `is_valid_for_price=false`.
Exact duplicate lines сохраняются и отражаются только в метриках. Для каждого snapshot
публикуются schema-stable rejects и JSON metadata.

Остановить контейнеры без удаления данных:

```bash
make data-down
```

Интеграционная проверка использует отдельный Compose project и синтетические fixtures,
проверяет схемы Parquet, полный и выборочный purchases-ingest, rollback при невалидном
вводе, Silver data/reject/metadata, FK failure, две реплики и деградацию одного DataNode:

```bash
make test-hdfs
```

Обычный `make test` Docker не запускает. Полный набор проверок доступен через
`make test-all`.
