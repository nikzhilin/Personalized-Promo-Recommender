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

## Gold user features

Первый Gold-инкремент строит только пользовательские признаки. Дата HDFS-партиции
выводится из UTC-даты `FEATURE_CUTOFF`; timezone в аргументе запрещён:

```bash
make build-user-features \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  LOOKBACK_DAYS=180
```

Job читает только пересекающиеся месяцы Silver purchases и использует полуинтервал
`[FEATURE_CUTOFF - LOOKBACK_DAYS, FEATURE_CUTOFF)`. Snapshot dimensions в purchases
должен совпадать с запрошенным; смешивание snapshot завершает job до публикации.

Результат публикуется в `gold/user_features/snapshot_date=YYYY-MM-DD`, а JSON metadata —
в `gold/metadata/user_features/snapshot_date=YYYY-MM-DD`. В набор входят все Silver
clients: для cold-start пользователей счётчики равны нулю, а средние, recency,
частота и любимая категория остаются `null`. Возраст winsorize-ится по 1/99 процентилям.
Любимая `level_2` определяется по суммарному количеству товара с лексикографическим
tie-break. Сумма чека при расхождении строк берётся как максимум в рамках транзакции,
а цена единицы использует только строки `is_valid_for_price=true`. Метаданные фиксируют
границы winsorization, исходные месяцы и Silver run IDs, cold-start/leakage/DQ-счётчики.
Повторный запуск атомарно заменяет data и metadata одной cutoff-партиции.

## Gold feedback features

Экспортированные API-события агрегируются в отдельный cutoff-safe пользовательский
snapshot:

```bash
make build-feedback-features \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  LOOKBACK_DAYS=180
```

Job читает только `feedback/events/event_date=*`, пересекающие 180-дневное окно, и
применяет dual cutoff: и `created_at`, и `received_at` должны быть строго меньше
`FEATURE_CUTOFF`. В модельные признаки входят только события со статусом `VERIFIED`;
непроверенные и поздние события остаются в DQ-метриках.

Snapshot `gold/feedback_features/snapshot_date=YYYY-MM-DD` содержит counts click, cart и
purchase за 30/90 дней, а также 180-дневные доли покупок со скидкой и без скидки,
среднюю показанную скидку и средний purchase value. В набор всегда входят все текущие
Silver clients. При отсутствии feedback counts равны нулю, averages и shares — `null`.
Data и metadata публикуются атомарно.

`build_user_features` автоматически присоединяет snapshot с точно совпадающими cutoff,
lookback и dimensions snapshot. Если он ещё не опубликован, user features сохраняет ту
же схему с zero/null defaults. Составная команда `make build-gold-features` сначала
строит feedback features, затем user features и остальные Gold-наборы.

Feedback улучшает predictive-признаки, но без A/B-теста не позволяет интерпретировать
наблюдаемый эффект скидки как causal uplift.

## Gold item, candidate и user-item features

Полный Gold feature layer строится одной последовательной командой:

```bash
make build-gold-features \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  LOOKBACK_DAYS=180
```

Item features включают cutoff-safe продажи, category-filtered approximate median цены,
популярность, repeat rate и синтетическую маржу. Выбросы цены вне 1/99 процентилей
`level_2` исключаются; отсутствующая item median восстанавливается через `level_3`, затем
`level_2`. Ставки читаются из
`configs/margin_seed.csv`: строка `__DEFAULT__` обязательна, остальные строки задают
override для конкретного `level_2`.

Candidate job объединяет repeat purchase, category popular, receipt-cosine item-to-item
и global popular источники. Score каждого источника rank-нормируется, пары
дедуплицируются, итог ограничен top-50 на клиента. Cold-start клиенты получают
неалкогольный global fallback. `user_item_features` строится только для этих пар и не
создаёт декартово произведение клиентов и товаров. Все три набора проверяют общий
cutoff/lookback/snapshot contract и публикуют data с metadata атомарно.

## Propensity dataset

Следующий training-инкремент формирует cutoff-safe observation dataset из нескольких
уже опубликованных Gold-снапшотов. Последний cutoff используется как validation,
предыдущие — как train:

```bash
make build-propensity-dataset \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  PROPENSITY_CUTOFFS="2019-02-01T00:00:00 2019-03-01T00:00:00" \
  LABEL_WINDOW_DAYS=30 NEGATIVE_RATIO=3
```

Observation unit — candidate `client_id × product_id × feature_cutoff`. Label равен 1,
если пара куплена в полуинтервале `[cutoff, cutoff + LABEL_WINDOW_DAYS)`. Все positives
среди кандидатов сохраняются; negatives выбираются воспроизводимым hash-sampling до
заданного отношения. Датасет и lineage metadata атомарно публикуются в
`gold/propensity_dataset/snapshot_date=<последний cutoff>`. Покупки label window не
добавляются в признаки. Метрика `future_positive_pairs_outside_candidates` явно
показывает покупки, которые RecSys не покрыл и которые поэтому не имеют candidate-only
user-item features.

## Propensity training

Обучение запускается в отдельном one-shot контейнере и читает опубликованный temporal
dataset из HDFS:

```bash
make train-propensity DATASET_SNAPSHOT_DATE=2019-03-01
```

Первый запуск собирает pinned training-образ; отдельно его можно подготовить командой
`make build-trainer`.

Job сохраняет validation целиком и при необходимости детерминированно ограничивает train
так, чтобы общий объём не превышал `MAX_TRAINING_ROWS=2000000`. Идентификаторы, label,
split и observation cutoff не используются как признаки. CatBoost обучается с двумя
потоками и early stopping; калибровка только диагностируется через Brier score и десять
calibration bins.

Успешный запуск атомарно публикует `model.cbm`, `feature_manifest.json`, `metrics.json`
и `run_metadata.json` в `models/propensity/run_id=<run_id>`. После публикации предыдущий
propensity run удаляется. Это рабочий артефакт batch pipeline, а не model registry.

Проверить training runtime на малом синтетическом наборе:

```bash
make test-training
```

## Uplift dataset

Cutoff-safe клиентский dataset для будущего T-learner объединяет Silver campaign labels
с уже опубликованными Gold user features:

```bash
make build-uplift-dataset \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  UPLIFT_VALIDATION_RATIO=0.2
```

Каждая комбинация `treatment_flg × target` делится воспроизводимым hash-порядком:
около 20% клиентов попадает в validation, остальные — в train. Job требует оба класса
в treatment и control и полное покрытие labels пользовательскими признаками. Метаданные
содержат conversion rates, global uplift, missing ratios, числовые SMD и top-20 значений
категориальных признаков. `|SMD| > 0.1` создаёт warning, но не блокирует публикацию.

Data и metadata атомарно публикуются в
`gold/uplift_dataset/snapshot_date=<feature cutoff date>`. `uplift_test` не включается:
он используется отдельно от causal training labels.

## Uplift training

T-learner обучает отдельные CatBoost outcome-модели для treatment и control на
опубликованном uplift dataset:

```bash
make train-uplift DATASET_SNAPSHOT_DATE=2019-03-01
```

Если positive rate хотя бы одной train-ветви ниже 10%, обе модели используют одинаковый
`auto_class_weights=Balanced`. Validation сохраняет raw `p_treatment - p_control` без
обрезки отрицательных значений. Метрики включают качество обеих outcome-моделей,
uplift@10/20/30, cumulative gain curve, AUUC и Qini относительно random-policy baseline.
Непубликуемый treatment classifier записывает только overlap diagnostics.

Успешный run атомарно публикует `model_control.cbm`, `model_treatment.cbm`, feature
manifest, metrics и run metadata в `models/uplift/run_id=<run_id>`, после чего удаляет
предыдущий uplift run.

## Batch model scoring

Propensity и uplift рассчитываются одним запуском для общего Gold snapshot. Model run ID
передаются явно и доступны в JSON-результате соответствующих training-команд:

```bash
make score-models \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  PROPENSITY_MODEL_RUN_ID=<32-hex-run-id> \
  UPLIFT_MODEL_RUN_ID=<32-hex-run-id>
```

Propensity scorer покрывает все пары текущего `user_item_features`; uplift scorer — всех
клиентов, присутствующих среди этих кандидатов. Inference выполняется ограниченными
Arrow batches на Spark executors и не собирает полный набор на driver.

Оба набора и общая metadata публикуются одной атомарной операцией:

```text
gold/propensity_scores/snapshot_date=YYYY-MM-DD
gold/uplift_scores/snapshot_date=YYYY-MM-DD
gold/metadata/model_scores/snapshot_date=YYYY-MM-DD
```

Выход propensity содержит `p_base_purchase`, а uplift — `p_control`, `p_treatment` и
необрезанный `uplift_score`. Discount response, simulation и optimizer в этот шаг не
входят.

## Simulation layer

Следующий Spark job объединяет общий scoring snapshot с Gold-признаками и рассчитывает
сценарии для всей сетки скидок, не выбирая победителя:

```bash
make build-simulation \
  DIMENSIONS_SNAPSHOT_DATE=2026-07-01 \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  SIMULATION_CONFIG=/workspace/configs/simulation.yaml
```

Versioned YAML задаёт discount grid, uplift multipliers, веса promo-abuse, price
quantiles и ROI epsilon. Job требует точного совпадения cutoff и исходных Gold run IDs
со scoring metadata, поэтому после перестроения признаков scoring также нужно повторить.

Для каждого кандидата с ценой публикуются четыре строки `discount=0/0.05/0.10/0.15` с
`p_discount`, promo-abuse proxy, expected profit, discount cost, incremental profit и
ROI. `simulation_version` и `is_synthetic=true` явно отделяют расчёт от измеренных
бизнес-эффектов. Raw отрицательный uplift сохраняется, но не увеличивает purchase probability.
Если цена отсутствует после fallback item→level_3→level_2, остаётся только baseline
`discount=0` с `is_discount_eligible=false` и `null` economic metrics.

Результат и metadata атомарно заменяют:

```text
gold/simulation_candidates/snapshot_date=YYYY-MM-DD
gold/metadata/simulation_candidates/snapshot_date=YYYY-MM-DD
```

Этот слой не применяет margin/category/fraud constraints, не выбирает локальную скидку,
не распределяет глобальный бюджет и не формирует финальный top-N.

## Discount optimizer

Optimizer выбирает одну строку simulation для каждой candidate-пары и распределяет
expected-cost budget:

```bash
make optimize-discounts \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  OPTIMIZER_CONFIG=/workspace/configs/optimizer.yaml
```

Versioned policy задаёт минимальную остаточную маржу, category caps, promo-abuse
threshold, minimum ROI, user cap и global budget. Default category cap равен 10%; скидка
15% возможна только через override соответствующего `level_2`.

Локально выбирается минимальная скидка среди вариантов, чья incremental profit находится
не дальше 0.01 от максимума. Затем non-zero назначения сортируются по ROI, incremental
profit и expected cost. Потоковый allocator применяет user cap и budget: не поместившаяся
строка отклоняется, но более дешёвые последующие назначения всё ещё рассматриваются.

Результат содержит одну строку на `client_id × product_id` и атомарно публикуется в:

```text
gold/optimized_offers/snapshot_date=YYYY-MM-DD
gold/metadata/optimized_offers/snapshot_date=YYYY-MM-DD
```

`optimizer_decision` объясняет результат ограничения без использования финального
`reason_code`: `DISCOUNT_ACCEPTED`, `LOCAL_BASELINE`, `USER_CAP_REJECTED`,
`BUDGET_REJECTED` или `MISSING_PRICE`.

## Final ranking

Ranking job превращает полный optimizer output в diverse top-10 snapshot:

```bash
make rank-recommendations \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  RANKING_CONFIG=/workspace/configs/ranking.yaml
```

Profit и relevance percentile ranks считаются отдельно внутри каждого клиента. Итоговый
score использует веса `0.65 × profit + 0.30 × relevance − 0.05 × promo_abuse`. Кандидаты
без `expected_profit` исключаются до нормирования.

После deterministic ordering допускается не более трёх товаров одного `level_2`;
отсутствующая категория считается единой `__UNKNOWN__`. Diversity cap не ослабляется
для заполнения top-10, поэтому клиент может получить меньше десяти рекомендаций.

Reason code выбирается в порядке cold-start, accepted incremental profit, repeat purchase,
category relevance, затем organic no-discount. Для корректного cold-start job проверяет
цепочку optimizer→simulation→user-features run IDs.

Data и metadata атомарно публикуются в:

```text
gold/final_recommendations/snapshot_date=YYYY-MM-DD
gold/metadata/final_recommendations/snapshot_date=YYYY-MM-DD
```

Этот HDFS snapshot является входом для отдельного online publication шага.

## Redis publication и read API

Online-контур использует internal-only Redis с `maxmemory=384 MiB`, `noeviction` и
immutable namespace на основе ranking run ID. Поднять Redis и read-only API:

```bash
cp .env.example .env
# Заменить placeholder POSTGRES_PASSWORD.
make online-up
```

`online-up` поднимает также internal-only PostgreSQL и перед стартом API применяет
нумерованные SQL migrations. Отдельные команды доступны для диагностики:

```bash
make db-up
make db-migrate
```

Перед публикацией publisher проверяет `replication=2` у HDFS recommendation snapshot и
точное совпадение ranking/simulation/item lineage:

```bash
make publish-redis \
  FEATURE_CUTOFF=2019-03-01T00:00:00 \
  ONLINE_CONFIG=/workspace/configs/online_store.yaml
```

Для каждого клиента сохраняется compact JSON top-N с TTL 48 часов. Publisher оценивает
размер с коэффициентом Redis overhead 2.0 и лимитом 160 MiB на snapshot. Если top-10 не
помещается, единый published top-N уменьшается вплоть до 1; если не помещается top-1,
active snapshot не изменяется. Fallback строится из популярных неалкогольных товаров с
нулевой скидкой.

После batch-загрузки проверяются число user keys, JSON-схемы и TTL. Только затем одной
операцией `SET promo:active_snapshot` активируется новый namespace. Повторная публикация
активного ranking run идемпотентна; незавершённый неактивный namespace очищается.

API доступен на `http://localhost:8000`:

```text
GET  /health/live
GET  /health/ready
GET  /metrics
POST /v1/recommend
POST /v1/feedback
```

Пример запроса:

```bash
curl -X POST http://localhost:8000/v1/recommend \
  -H 'content-type: application/json' \
  -d '{"client_id":"000012768d","limit":10,"context":{"page":"main"}}'
```

Неизвестный клиент получает Redis fallback и `is_fallback=true`. Каждый фактически
подготовленный response синхронно записывается в `prediction_events` до отправки клиенту;
при сбое PostgreSQL recommend возвращает 503. Readiness требует Redis active snapshot и
доступный PostgreSQL.

Feedback принимает UUID события/запроса, client/product, тип `click|cart|purchase`,
показанную скидку и timezone-aware timestamp. Найденный prediction проверяется по клиенту,
товару и скидке. Если журнал показа уже отсутствует, событие сохраняется со статусом
`UNVERIFIED_MISSING_REQUEST` и warning. Идентичный повтор `event_id` возвращает 202 как
no-op, а конфликтующий payload — 409.

Publisher также транзакционно сохраняет глобальный fallback top-N в PostgreSQL и
переключает его active pointer перед активацией Redis namespace. При ошибке Redis,
отсутствующем active snapshot, metadata или payload, а также при невалидном JSON API
возвращает этот PostgreSQL fallback. Персональные рекомендации в PostgreSQL не
дублируются. `/health/ready` при деградации Redis по-прежнему возвращает 503.

Active snapshot и metadata кэшируются внутри API-процесса на 30 секунд. Немедленно
перечитать и проверить их можно защищённым endpoint:

```bash
make reload-api-cache ADMIN_API_KEY=<значение-из-.env>
```

`POST /v1/admin/cache/reload` требует заголовок `X-Admin-API-Key`, возвращает 204 после
успешного прогрева, 401 для неверного ключа и 503 при недоступном Redis. Неудачный reload
не удаляет последнее валидное значение локального кэша.

## Feedback export и Airflow

Feedback exporter читает один bounded batch до 100 000 событий из PostgreSQL по
watermark `(received_at, event_id)`. HDFS `event_date` выводится из `created_at` в UTC,
поэтому позднее событие безопасно обновляет историческую partition:

```bash
make export-feedback
```

Затронутые partitions атомарно переписываются в
`feedback/events/event_date=YYYY-MM-DD`: существующие и новые строки объединяются и
дедуплицируются по `event_id`, а несовпадающий fingerprint останавливает публикацию.
Watermark обновляется compare-and-swap только после успешных HDFS rename. Deterministic
batch ID делает безопасным повтор после сбоя между HDFS publication и DB commit.

Airflow использует отдельные database/user в том же PostgreSQL instance. Добавьте в
`.env` отдельные `AIRFLOW_DB_PASSWORD` и `AIRFLOW_ADMIN_PASSWORD`, затем запустите:

```bash
make airflow-up
make trigger-feedback-export
```

Web UI доступен на `http://localhost:8080`. DAG `feedback_export_pipeline` имеет
`schedule=None`, `max_active_runs=1`, две retries с exponential backoff и не передаёт
feedback через XCom. Каждый manual run экспортирует только один batch; backlog
обрабатывается последующими запусками.

Полный pipeline также доступен как ручной DAG `daily_discount_pipeline`. Он выполняет
HDFS preflight, ingest, Silver/Gold, обучение, scoring, optimization, offline evaluation,
replication gate и атомарную публикацию. Тяжёлые задачи используют Airflow pool
`heavy_compute` с одним slot:

```bash
make trigger-daily-pipeline
```

Состояние запуска, cutoff, config fingerprint, model/snapshot IDs и сводные метрики
сохраняются в PostgreSQL `pipeline_runs`. После успешной публикации остаются два последних
Gold snapshot, а NameNode metadata копируется в отдельный persistent volume.

## Monitoring

Prometheus хранит семь дней метрик API, Airflow, Spark, HDFS, Redis и batch pipeline.
Grafana datasource, overview dashboard и alert rules provisioned из репозитория:

```bash
make monitoring-up
```

Grafana доступна на `http://localhost:3000`, Prometheus остаётся во внутренней сети.
Правила обнаруживают недоступность API/HDFS, stale pipeline, высокий расход Redis memory
и аномальную долю скидок. Внешний Alertmanager намеренно не входит в MVP.

Остановить контейнеры без удаления данных:

```bash
make data-down
```

Интеграционная проверка использует отдельный Compose project и синтетические fixtures,
проверяет схемы Parquet, полный и выборочный purchases-ingest, rollback при невалидном
вводе, Silver data/reject/metadata, Gold cutoff и cold-start формулы, защиту от смешивания
snapshot, uplift dataset/model publication, FK failure, две реплики и деградацию одного
DataNode:

```bash
make test-hdfs
```

Обычный `make test` Docker не запускает. Полный набор проверок доступен через
`make test-all`.
