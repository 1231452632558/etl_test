# Code Architecture

Этот документ описывает устройство Python ETL-кода на уровне модулей, стадий и контекста выполнения.

## 1. Главная идея

В проекте есть две равноправные точки входа:
- `scripts/etl_pipeline.py` — task-версия
- `scripts/legacy/etl_pipeline.py` — монолит

Обе используют один и тот же общий слой:
- `scripts/pipeline_common.py`

Это значит, что:
- SQL-логика
- restore
- блокировка материализованных `cf_*`
- snapshot export/import
- weekly-логика
- seed ручных таблиц

должны оставаться едиными независимо от способа оркестрации.

## 2. Файлы и ответственность

### `scripts/pipeline_common.py`

Центральный модуль с общей логикой:
- `PipelineSettings` — чтение `etl_config.ini`
- `ETLLogger` — логирование
- `DatabaseOperations` — операции с PostgreSQL
- `copy_from_remote_or_local()` — обязательное скачивание remote dump без fallback на старый локальный файл; локальный режим разрешается только через `--local-dump`
- `finalize_remote_backup()` — публикация проверенного удаленного архива после успешного ETL
- `rotate_backups()` — хранение одного текущего и `max_backups - 1` старых архивов
- `extract_dump()` — распаковка входящего дампа
- `build_snapshot_exports()` — экспорт snapshot-таблиц в CSV
- `resolve_snapshot_tables()` — обнаружение всех таблиц и безопасных уникальных ключей
- `replace_tables_from_csv()` — групповая транзакционная синхронизация явно заданных таблиц
- `apply_snapshot_batch()` — единая транзакция UPSERT/REPLACE и проверка всех source-строк перед COMMIT
- `order_snapshot_tables()` — порядок UPSERT с учетом внешних ключей
- `add_weekly_records()` — weekly-историзация в `users_active` и `group_employee_count`

Ключевой принцип:
- все действия над БД должны жить здесь, а не дублироваться в task/monolith слое
- все таблицы загружаются одним атомарным batch; таблицы с ключом используют UPSERT, а `snapshot_replace_tables` — точную замену
- тип `json` в changed-row предикате приводится к `jsonb`, тип `xml` — к `text`

### `scripts/etl_pipeline.py`

Task-entrypoint.

Отвечает за:
- разбор CLI
- выбор стадий через `--tasks`
- выбор `db_scope`
- создание `TaskRunner`
- заполнение контекста

### `scripts/legacy/etl_pipeline.py`

Монолитный entrypoint.

Отвечает за:
- тот же CLI-контракт, что и task-версия
- выполнение тех же стадий, но без отдельного task runner

### `scripts/tasks/*.py`

Отдельные task-классы:
- `extract_task.py`
- `restore_task.py`
- `seed_asterisk_task.py`
- `compare_task.py`
- `load_task.py`
- `weekly_task.py`
- `cleanup_task.py`

### `scripts/tasks/task_runner.py`

Оркестратор task-версии:
- выполняет стадии по порядку
- передаёт общий context
- прерывает выполнение при ошибке
- запускает обязательный `cleanup` после fail-fast

## 3. Стадии

Обе версии поддерживают один и тот же набор стадий:

```text
extract
restore
seed_asterisk
compare
load
weekly
cleanup
```

### `extract`

Назначение:
- получить SQL-файлы из входящего `.tar.gz` или `.sql`

Результат в context:
- `sql_files`
- `extract_dir`

### `restore`

Назначение:
- создать `main` или `temp` БД
- восстановить туда SQL-файлы
- создать служебные таблицы
- при `main`-инициализации засидить `group_employee_count` и `users_active`, если они пусты

Зависимости:
- обычно требует `extract`

### `seed_asterisk`

Назначение:
- отдельно дополнить `asterisk_cdr` в стабильной main БД
- загрузить во временную таблицу CSV и вставить только отсутствующие `id`
- гарантировать наличие `project_id`
- снять schema snapshot после обработки

Почему отдельная стадия:
- пользователь может запускать её отдельно
- она не зависит от staging и не входит в общий snapshot

### `compare`

Назначение:
- обнаружить обычные таблицы `public` и их PK/UNIQUE-ключи
- поставить родительские таблицы раньше дочерних
- экспортировать snapshot-CSV из staging
- исключить `cf_*` из `issues/projects`

Работает только по `temp_db`.

Результат в context:
- `modifications`
- `export_dir`

### `load`

Назначение:
- применить все snapshot-CSV в `main_db` одной транзакцией и проверить source/main до COMMIT
- остановиться при расхождении схемы staging/main вместо тихого пропуска колонок
- отклонить `cf_*` в snapshot `issues/projects`, не меняя существующую схему main
- использовать отдельные лимиты для полного batch и ожидания блокировок; REPLACE после прямой вставки проверять по row count
- сериализовать snapshot-batch advisory-lock, не блокируя заранее все таблицы через `LOCK TABLE`

Работает только по staging-derived данным.

Если в CLI указать `load` без `compare`, entrypoint автоматически подставит `compare`.

### `weekly`

Назначение:
- добавить или обновить weekly-снимок в:
  - `group_employee_count`
  - `users_active`

Источники:
- `users`
- `groups_users`

Особенность:
- weekly должен выполняться после `load` в полном nightly-сценарии
- но может запускаться и отдельно

### `cleanup`

Назначение:
- удалить staging БД
- удалить временные каталоги

## 4. Контекст task-версии

Task-версия передаёт между шагами словарь `context`.

Основные ключи:
- `dump_file`
- `sql_files`
- `temp_dir`
- `main_db`
- `db_name`
- `temp_db`
- `tables`
- `weekly_enabled`
- `weekly_days`
- `force_weekly`
- `cleanup_temp_db`
- `create_database`
- `modifications`
- `export_dir`

Практический смысл:
- если стадия запускается отдельно, нужные ей ключи должны уже существовать или задаваться через CLI-режим
- `cf_*` никогда не должны присутствовать в `modifications`

## 5. CLI-контракт

Обе версии поддерживают:
- `--tasks`
- `--db-scope main|temp|auto`
- `--temp-db-name`
- `--skip-weekly`
- `--force-weekly`

### `--tasks`

Список стадий через запятую.

Пример:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly
```

### `--db-scope`

Используется для стадии `restore`.

Правила:
- `auto` = `main` для `--init`
- `auto` = `temp` для nightly
- `seed_asterisk` всегда дополняет `main_db`

### `--temp-db-name`

Нужен, если вы хотите запускать temp-стадии без нового `restore`.

Пример:

```bash
python3 scripts/etl_pipeline.py \
  --config config/etl_config.ini \
  --tasks compare \
  --db-scope temp \
  --temp-db-name temp_restore_20260507_123456
```

## 6. Автодобавление зависимостей

CLI автоматически дополняет план:
- `restore` -> добавляет `extract`, если его нет
- `load` -> добавляет `compare`, если его нет

Это сделано только для удобства запуска.

## 7. Manual tables

### `asterisk_cdr`

Живет в стабильной main БД и дополняется из отдельного CSV.

Стадия:
- `seed_asterisk`

Алгоритм:
- CSV копируется во временную таблицу PostgreSQL
- `INSERT ... ON CONFLICT (id) DO NOTHING` добавляет только новые строки
- таблица не экспортируется из main и не переносится через staging snapshot

### `group_employee_count`

Историческая weekly-таблица:
- `group_id`
- `group_name`
- `snapshot_date`
- `user_count`
- `created_at`

Источник:
- агрегация по `users` + `groups_users`

### `users_active`

Историческая weekly-таблица:
- `snapshot_date`
- `user_count`
- `created_at`

Источник:
- `COUNT(*)` по активным пользователям

Настройка weekly:
- `weekly_enabled` включает автоматический шаг
- `weekly_days` принимает ISO-дни через запятую
- `--force-weekly` запускает срез независимо от расписания

## 8. Логирование

Лог строится так, чтобы можно было понять:
- какой коммит исполнялся
- какой набор стадий выбран
- какая БД является target
- какие таблицы и индексы реально существуют
- какие sample данные были выбраны weekly

Особенно важные отладочные блоки:
- `Версия репозитория`
- `План стадий ...`
- `Schema snapshot`
- `Custom value columns исключены из snapshot`
- `Weekly debug ...`

## 9. Почему важна эквивалентность монолита и tasks

Требование проекта:
- task-версия и монолит должны быть равноправными
- отличие только в orchestration-слое

Поэтому любые изменения бизнес-логики нужно вносить:
1. в `pipeline_common.py`
2. затем убедиться, что оба entrypoint используют эту логику одинаково

Если новая возможность появляется только в одном entrypoint, это считается расхождением архитектуры.
