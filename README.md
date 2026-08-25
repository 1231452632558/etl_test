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
1. При настроенном `[remote]` новый dump обязательно скачивается через `scp` во временный `incoming_*`; существующий локальный путь из cron не используется как fallback.
2. В лог записываются `remote_mtime`, SHA256 и результат сравнения с текущим архивом.
3. Dump разворачивается в новую staging БД.
4. Pipeline автоматически находит обычные таблицы `public` и их PK/UNIQUE-ключи.
5. Все CSV предварительно загружаются во временные таблицы main, затем весь UPSERT/REPLACE выполняется одной транзакцией.
6. До COMMIT проверяется, что каждая строка staging присутствует в main с теми же значениями; при ошибке весь batch откатывается.
7. CSV `asterisk_cdr` напрямую дополняет стабильную main БД по отсутствующим `id`.
8. После загрузки основных таблиц в main выполняется weekly-update исторических таблиц.
9. Staging удаляется только после успешного запуска; при ошибке сохраняется для диагностики.

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
- `[database].snapshot_batch_timeout_seconds` — максимальное время атомарного load (по умолчанию `14400`, 4 часа)
- `[database].snapshot_lock_timeout_seconds` — отдельный предел ожидания блокировок main БД (по умолчанию `300`, 5 минут)
- `[paths].backup_storage_dir` — где хранить один текущий проверенный архив
- `[paths].old_backup_dir` — где хранить предыдущие проверенные архивы (по умолчанию `/workspace/old_backup`)
- `[paths].temp_dir` — где хранить временные каталоги
- `[paths].log_file` — основной лог
- `[paths].asterisk_csv_file` — CSV-файл для append-only загрузки `asterisk_cdr`
- `[paths].group_employee_count_csv_file` — CSV-файл для начального наполнения `group_employee_count`
- `[paths].users_active_csv_file` — CSV-файл для начального наполнения `users_active`
- `[tables].auto_discover_tables` — автоматически обрабатывать все таблицы `public`
- `[tables].fail_on_unkeyed_tables` — останавливать запуск, если таблицу нельзя безопасно UPSERT-ить
- `[tables].snapshot_excluded_tables` — дополнительные осознанные исключения
- `[tables].snapshot_replace_tables` — таблицы для точной атомарной синхронизации из dump
- `[tables].incremental_tables` — явные overrides ключей или полный список при отключенном автообнаружении
- `[tables].issues_table` — таблица задач для snapshot-загрузки
- `[tables].projects_table` — таблица проектов для snapshot-загрузки
- `[retention].max_backups` — общее число копий: текущая + старые (по умолчанию `3`)
- `[schedule].weekly_enabled` — включить автоматический weekly
- `[schedule].weekly_days` — дни запуска по ISO через запятую, например `1,4`
- `[schedule].target_day` — совместимый fallback, если `weekly_days` не задан

Точное время запуска задается cron/systemd; pipeline проверяет день недели в момент запуска.

Важно: `issues_table` и `projects_table` автоматически добавляются в snapshot-список, даже если их забыли явно указать.
Важно: `asterisk_cdr`, `group_employee_count` и `users_active` принудительно исключаются из общего snapshot, даже если ошибочно указаны в конфиге.

Таблицы из `snapshot_replace_tables` не пропускаются: их CSV сначала полностью
загружается во временную таблицу, затем содержимое соответствующей main-таблицы
атомарно заменяется внутри транзакции. Это режим для таблиц без однозначного ключа,
либо для связанных таблиц, где UPSERT по одному ключу может конфликтовать с другим
уникальным ограничением. Вся перечисленная группа заменяется одной транзакцией.
Для больших REPLACE-таблиц проверяется число строк после прямого `INSERT ... SELECT`;
повторное двойное сравнение миллионов строк через `EXCEPT ALL` не выполняется.

## Запуск

### Task-версия

Инициализация:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/dump.tar.gz
```

Nightly run:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup
```

Если секция `[remote]` заполнена, pipeline всегда скачивает новый архив, даже если
cron продолжает передавать существующий локальный путь. При ошибке `scp` запуск
останавливается без fallback на старый файл. Для осознанного ручного запуска с
локальным dump нужен явный флаг:

```bash
python3 scripts/etl_pipeline.py /path/to/dump.tar.gz --local-dump --config config/etl_config.ini --cleanup
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
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup
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

## Хранение удаленных архивов

Для полного запуска с `[remote]` порядок безопасный:

1. Новый архив обязательно скачивается в `temp_dir`; логируются mtime, SHA256 и отличие от текущего архива.
2. Данные успешно восстанавливаются в staging и вливаются в main БД.
3. Предыдущий текущий архив переносится в `old_backup_dir`.
4. Новый архив становится текущим в `backup_storage_dir`.
5. Ротация оставляет всего `max_backups` копий.

При `max_backups=3` это один текущий архив и два предыдущих каталога в
`/workspace/old_backup/`. Если ETL завершился ошибкой, текущий архив и старые
копии не ротируются.

При ошибке ETL staging БД и snapshot-файлы также не удаляются, чтобы можно было
сопоставить source и main до следующего запуска.

## Ограничения и допущения

- Входящий dump по-прежнему не меняется и всегда восстанавливается только в новую БД.
- Snapshot-режим означает, что для всех автообнаруженных таблиц в main БД подтягивается текущее состояние строк из staging.
- Колонки PostgreSQL `json` сравниваются как `jsonb`, поэтому changed-row проверка не требует отсутствующего оператора `json = json`.
- Удаления строк из dump сейчас не синхронизируются автоматически.
- Исключение — `snapshot_replace_tables`: они точно зеркалируются, включая удаления.
- Атомарный load ограничен `snapshot_batch_timeout_seconds`, а ожидание блокировки — более коротким `snapshot_lock_timeout_seconds`.
- Pipeline не запрашивает предварительную блокировку всех snapshot-таблиц: необходимые блокировки PostgreSQL берет непосредственно для `INSERT/UPDATE/DELETE`. Advisory-lock предотвращает только параллельный запуск второго snapshot-batch.
- Таблица должна иметь PRIMARY KEY или простой UNIQUE index; иначе pipeline останавливается и требует явного решения.
- Новые таблицы/колонки в staging не создаются автоматически в main: расхождение схемы останавливает загрузку.
- Существующие `cf_*` в main БД не удаляются; pipeline только не переносит новые custom values.

## Что проверить перед prod rollout

1. Что автообнаружение нашло ожидаемое количество таблиц, а исключения заданы осознанно.
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
