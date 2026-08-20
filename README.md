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
3. Выгрузить snapshot нужных таблиц из staging.
4. Влить snapshot в стабильную main БД через `UPSERT`.
5. Обновить ручные weekly-таблицы.
6. Очистить staging БД и временные файлы.

Главное отличие от `dump_restore.sh`: основная база больше не удаляется и не создается заново каждую ночь.

## Ключевые особенности

- Стабильная main БД без nightly `DROP DATABASE`
- Обязательная staging БД для каждого входящего дампа
- Одинаковая бизнес-логика в монолите и task-версии
- Запрет материализации `custom_values` в `issues/projects`
- `UPSERT` по snapshot-таблицам
- Отдельная обработка CSV/manual таблиц `asterisk_cdr`, `group_employee_count` и `users_active`
- Автоматическое удаление осиротевших staging-БД предыдущих запусков
- Ротация архивов и подробное логирование
- Только стандартная библиотека Python и `psql`

## Структура проекта

```text
repo/
├── config/
│   └── etl_config.ini
├── docs/
│   ├── CODE_ARCHITECTURE.md
│   ├── OPERATIONS.md
│   └── TESTING.md
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
- фильтрация служебных `cf_*`-колонок
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
- `SeedAsteriskTask`
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
5. Колонки `cf_*` исключаются из snapshot для `issues` и `projects`.

### Ежедневный запуск

Используется каждый день:
1. Nightly dump разворачивается в новую staging БД.
2. Во staging создаются только служебные объекты, необходимые для snapshot.
3. Из staging экспортируются snapshot-таблицы, перечисленные в конфиге.
4. Эти snapshot-файлы вливаются в main БД через `UPSERT`.
5. CSV `asterisk_cdr` напрямую дополняет стабильную main БД по отсутствующим `id`.
6. После загрузки основных таблиц в main выполняется weekly-update исторических таблиц.
7. Staging БД удаляется.

Перед началом обе версии также удаляют оставшиеся staging-БД с именами вида
`<temp_db_prefix>YYYYMMDD_HHMMSS`. Базы с активными подключениями и база,
явно переданная через `--temp-db-name`, не удаляются.

## Ручные таблицы

Речь про:
- `asterisk_cdr`
- `group_employee_count`
- `users_active`

Эти таблицы не приходят во входящем dump и исторически создавались вручную.

Новая логика такая:
- на `--init` таблицы создаются, если отсутствуют
- CSV для `group_employee_count` и `users_active` используются только для начального наполнения пустых таблиц
- nightly pipeline не выгружает и не перезаливает `group_employee_count` и `users_active`
- weekly формирует срез из актуальных `users`/`groups_users` непосредственно в main БД
- `asterisk_cdr` не участвует в staging snapshot; CSV напрямую добавляет в main только отсутствующие строки через `ON CONFLICT (id) DO NOTHING`
- повторный запуск за ту же дату идемпотентно обновляет weekly-срез, а не создает дубли

То есть старые данные из CSV сохраняются как стартовая точка, а дальнейшая жизнь таблиц становится инкрементальной.

## Custom values

Трансформация `custom_values` полностью удалена из обеих версий pipeline:
- стадии `transform_custom_values` нет в CLI и task runner
- pipeline не создает колонки `cf_*`
- `cf_*` исключаются из snapshot-экспорта `issues` и `projects`
- загрузчик отклоняет snapshot этих таблиц, если в нем обнаружены `cf_*`
- pipeline не удаляет существующие `cf_*` и вообще не меняет целевую схему ради custom values

Одноразовая очистка ранее созданных `cf_*` больше не является частью workflow.

## Конфигурация

Основной файл: `config/etl_config.ini`

Ключевые параметры:
- `[database].db_name` — имя стабильной main БД
- `[database].temp_db_prefix` — префикс staging БД
- `[paths].backup_storage_dir` — куда складывать архивы
- `[paths].temp_dir` — где хранить временные каталоги
- `[paths].log_file` — основной лог
- `[paths].asterisk_csv_file` — CSV-файл для append-only загрузки `asterisk_cdr`
- `[paths].group_employee_count_csv_file` — CSV-файл для начального наполнения `group_employee_count`
- `[paths].users_active_csv_file` — CSV-файл для начального наполнения `users_active`
- `[tables].incremental_tables` — список snapshot-таблиц для `UPSERT`
- `[tables].issues_table` — таблица задач для snapshot-загрузки
- `[tables].projects_table` — таблица проектов для snapshot-загрузки
- `[retention].max_backups` — сколько последних архивов хранить в `backup_storage_dir` (по умолчанию `3`)
- `[schedule].weekly_enabled` — включить автоматический weekly
- `[schedule].weekly_days` — дни запуска по ISO через запятую, например `1,4`
- `[schedule].target_day` — совместимый fallback, если `weekly_days` не задан

Точное время запуска задается cron/systemd; pipeline проверяет день недели в момент запуска.

Важно: `issues_table` и `projects_table` автоматически добавляются в snapshot-список, даже если их забыли явно указать.
Важно: `asterisk_cdr`, `group_employee_count` и `users_active` принудительно исключаются из `incremental_tables`, даже если ошибочно указаны в конфиге.

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

Запуск отдельных стадий:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly --force-weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk
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

Запуск отдельных стадий:

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks weekly --force-weekly
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk
```

### Стадии и частичный запуск

Обе версии поддерживают одинаковый набор стадий:
- `extract`
- `restore`
- `seed_asterisk`
- `compare`
- `load`
- `weekly`
- `cleanup`

Полезные флаги:
- `--tasks` — список стадий через запятую
- `--db-scope main|temp|auto` — куда применять `restore`; `seed_asterisk` всегда пишет в main
- `--temp-db-name` — имя уже существующей staging БД для частичного запуска без `restore`
- `--skip-weekly` — исключить weekly из полного nightly-потока
- `--force-weekly` — выполнить weekly сейчас, независимо от расписания и `weekly_enabled`

Практические правила:
- `restore` автоматически подтягивает `extract`, если вы его не указали
- `load` автоматически подтягивает `compare`, если вы его не указали
- для `compare` и `load` нужен `temp` scope
- для `weekly` и `seed_asterisk` dump не обязателен

## Как выбрать между версиями

- Берите `scripts/etl_pipeline.py`, если нужна явная task-структура и удобство дальнейшего расширения.
- Берите `scripts/legacy/etl_pipeline.py`, если удобнее поддерживать один большой исполняемый сценарий.

С точки зрения nightly-логики они должны вести себя одинаково.

## Ограничения и допущения

- Входящий dump по-прежнему не меняется и всегда восстанавливается только в новую БД.
- Snapshot-режим означает, что для таблиц из `incremental_tables` в main БД подтягивается текущее состояние строк из staging.
- Удаления строк из dump сейчас не синхронизируются автоматически.
- Существующие `cf_*` в main БД не удаляются; pipeline только не переносит новые custom values.

## Что проверить перед prod rollout

1. Корректность `incremental_tables` в конфиге.
2. Наличие доступа `sudo -u postgres psql`.
3. Что CSV для `asterisk_cdr` и init-only seed CSV лежат по путям из конфига.
4. Что `weekly_enabled`, `weekly_days` и `weekly_group_name_pattern` соответствуют расписанию.

## Документы

- Общая схема: `README.md`
- Установка и запуск: `INSTALL.md`
- Эксплуатация: `docs/OPERATIONS.md`
- Методика тестирования: `docs/TESTING.md`
- Миграция с shell-скрипта: `MIGRATION.md`
- Подробная документация по коду: `docs/CODE_ARCHITECTURE.md`

## Дополнительные документы

- Установка и окружение: `INSTALL.md`
- Пошаговая миграция с `dump_restore.sh`: `MIGRATION.md`
- Операционный процесс и runbook: `docs/OPERATIONS.md`
- Методика тестирования на ВМ: `docs/TESTING.md`

## Статус

`dump_restore.sh` остается в репозитории как описание текущего prod-процесса и точка сравнения.

Python-версии ориентированы на замену этого сценария новым инкрементальным nightly-flow.
