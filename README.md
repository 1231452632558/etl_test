# ETL Pipeline for PostgreSQL

Python-реализация nightly ETL-процесса для PostgreSQL, которая заменяет текущий `dump_restore.sh`, но не пересоздает рабочую базу каждую ночь.

В репозитории есть две равноправные версии pipeline:
- `scripts/legacy/etl_pipeline.py` — монолитная версия
- `scripts/etl_pipeline.py` — версия, разбитая на tasks

Обе версии выполняют один и тот же процесс и используют общий код из `scripts/pipeline_common.py`.

## Что делает pipeline

### Цель

Входящий дамп менять нельзя: он по-прежнему разворачивается только в новую временную базу.

Поэтому nightly-сценарий такой:
1. Получить nightly dump.
2. Развернуть его во временную staging БД.
3. Выполнить в staging БД трансформацию custom fields.
4. Выгрузить snapshot нужных таблиц из staging.
5. Влить snapshot в стабильную main БД через `UPSERT`.
6. Обновить ручные weekly-таблицы.
7. Очистить staging БД и временные файлы.

Главное отличие от `dump_restore.sh`: основная база больше не удаляется и не создается заново каждую ночь.

## Ключевые особенности

- Стабильная main БД без nightly `DROP DATABASE`
- Обязательная staging БД для каждого входящего дампа
- Одинаковая бизнес-логика в монолите и task-версии
- Трансформация `custom_values -> issues.cf_*` до загрузки в main
- `UPSERT` по snapshot-таблицам
- Отдельная обработка CSV/manual таблиц `asterisk_cdr`, `group_employee_count` и `users_active`
- Ротация архивов и подробное логирование
- Только стандартная библиотека Python и `psql`

## Структура проекта

```text
repo/
├── config/
│   └── etl_config.ini
├── docs/
│   └── OPERATIONS.md
├── dumps/
│   ├── test_dump_v1.sql
│   └── test_dump_v2.sql
├── scripts/
│   ├── etl_pipeline.py
│   ├── pipeline_common.py
│   ├── legacy/
│   │   ├── etl_pipeline.py
│   │   └── etl_pipeline_tasks.py
│   └── tasks/
│       ├── __init__.py
│       ├── base.py
│       ├── cleanup_task.py
│       ├── compare_task.py
│       ├── extract_task.py
│       ├── load_task.py
│       ├── restore_task.py
│       ├── task_runner.py
│       ├── transform_custom_values_task.py
│       └── weekly_task.py
├── dump_restore.sh
├── INSTALL.md
└── README.md
```

## Архитектура

### Общий слой

`scripts/pipeline_common.py` содержит:
- загрузку конфигурации
- логгер
- PostgreSQL-операции
- restore дампа
- export/import CSV
- `UPSERT`
- custom transform
- seed ручных таблиц
- weekly-upsert логику

Это нужно для того, чтобы монолит и task-версия оставались эквивалентными.

### Монолитная версия

Файл: `scripts/legacy/etl_pipeline.py`

Подходит, если нужен один исполняемый сценарий без orchestration-слоя.

### Task-версия

Файл: `scripts/etl_pipeline.py`

Подходит, если хочется явную последовательность шагов и независимые задачи:
- `ExtractTask`
- `RestoreTask`
- `TransformCustomValuesTask`
- `CompareTask`
- `LoadTask`
- `WeeklyTask`
- `CleanupTask`

## Как работает nightly run

### Инициализация (`--init`)

Используется один раз:
1. Создается main БД.
2. Дамп разворачивается сразу в main.
3. Создаются служебные таблицы, которых нет в дампе.
4. Если ручные CSV/manual таблицы пустые, они заполняются из seed CSV.
5. Выполняется custom transform в main.

### Ежедневный запуск

Используется каждый день:
1. Nightly dump разворачивается в новую staging БД.
2. Во staging создаются служебные таблицы и патчится `asterisk_cdr.project_id`.
3. Во staging выполняется custom transform.
4. Из staging экспортируются snapshot-таблицы, перечисленные в конфиге.
5. Эти snapshot-файлы вливаются в main БД через `UPSERT`.
6. В main БД выполняется weekly-update ручных таблиц.
7. Staging БД удаляется.

## Ручные таблицы

Речь про:
- `asterisk_cdr`
- `group_employee_count`
- `users_active`

Эти таблицы не приходят во входящем dump и исторически создавались вручную.

Новая логика такая:
- `asterisk_cdr` может подхватываться из отдельного CSV-файла `asterisk_csv_file`, если таблица в целевой БД пустая
- на `--init` они создаются, если отсутствуют
- если они пустые, данные можно подхватить из seed CSV рядом с логами
- дальше они живут в main БД постоянно
- nightly pipeline не перезаливает каждый день `group_employee_count` и `users_active`
- `asterisk_cdr` может подгружаться в staging из CSV, если во входящем dump этой таблицы нет
- weekly-обновление идет идемпотентно через `ON CONFLICT` только для `group_employee_count` и `users_active`

То есть старые данные из CSV сохраняются как стартовая точка, а дальнейшая жизнь таблиц становится инкрементальной.

## Custom transform

Цель шага:
- взять `issues`
- найти связанные строки в `custom_values`
- получить названия полей из `custom_fields`
- материализовать их в отдельные колонки `cf_*`

Текущая реализация:
- проверяет наличие таблиц `issues`, `custom_values`, `custom_fields`
- получает все issue custom fields
- создает недостающие колонки `cf_*`
- агрегирует значения по `customized_id`
- обновляет `issues` одним set-based SQL

Почему это важно:
- именно `issues` в уже преобразованном виде попадает в snapshot и затем в main БД
- это убирает зависимость от последующего JOIN при аналитике

## Конфигурация

Основной файл: `config/etl_config.ini`

Ключевые параметры:
- `[database].db_name` — имя стабильной main БД
- `[database].temp_db_prefix` — префикс staging БД
- `[paths].backup_storage_dir` — куда складывать архивы
- `[paths].temp_dir` — где хранить временные каталоги
- `[paths].log_file` — основной лог
- `[paths].asterisk_csv_file` — CSV-файл для ручной загрузки `asterisk_cdr`
- `[paths].group_employee_count_csv_file` — CSV-файл для начального наполнения `group_employee_count`
- `[paths].users_active_csv_file` — CSV-файл для начального наполнения `users_active`
- `[tables].incremental_tables` — список snapshot-таблиц для `UPSERT`
- `[tables].issues_table` — таблица для custom transform
- `[retention].max_backups` — сколько последних архивов хранить в `backup_storage_dir` (по умолчанию `3`)
- `[schedule].target_day` — день weekly-обновления

Важно: `issues_table` автоматически добавляется в snapshot-список, даже если её забыли явно указать.
Важно: `group_employee_count` и `users_active` не должны попадать в `incremental_tables`, потому что они обновляются отдельным weekly-процессом.

## Запуск

### Task-версия

Инициализация:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/dump.tar.gz
```

Nightly run:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/dump.tar.gz
```

### Монолитная версия

Инициализация:

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --init /path/to/dump.tar.gz
```

Nightly run:

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/dump.tar.gz
```

## Как выбрать между версиями

- Берите `scripts/etl_pipeline.py`, если нужна явная task-структура и удобство дальнейшего расширения.
- Берите `scripts/legacy/etl_pipeline.py`, если удобнее поддерживать один большой исполняемый сценарий.

С точки зрения nightly-логики они должны вести себя одинаково.

## Ограничения и допущения

- Входящий dump по-прежнему не меняется и всегда восстанавливается только в новую БД.
- Snapshot-режим означает, что для таблиц из `incremental_tables` в main БД подтягивается текущее состояние строк из staging.
- Удаления строк из dump сейчас не синхронизируются автоматически.
- Производительность custom transform зависит от объема `custom_values` и индексов в исходной базе.

## Что проверить перед prod rollout

1. Корректность `incremental_tables` в конфиге.
2. Наличие доступа `sudo -u postgres psql`.
3. Что seed CSV для `asterisk_cdr`, `group_employee_count` и `users_active` лежат там, где ожидается логикой.
4. Что weekly `ON CONFLICT` соответствует нужной бизнес-логике.
5. Что индексы на `custom_values` в исходной схеме действительно существуют и достаточны.

## Дополнительные документы

- Установка и окружение: `INSTALL.md`
- Пошаговая миграция с `dump_restore.sh`: `MIGRATION.md`
- Операционный процесс и runbook: `docs/OPERATIONS.md`
- Методика тестирования на ВМ: `docs/TESTING.md`

## Статус

`dump_restore.sh` остается в репозитории как описание текущего prod-процесса и точка сравнения.

Python-версии ориентированы на замену этого сценария новым инкрементальным nightly-flow.
