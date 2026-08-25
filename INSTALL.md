# Установка и настройка

## Требования

- Ubuntu 22.04
- PostgreSQL 14
- Python 3.x
- Доступ к `sudo -u postgres psql`
- SSH-доступ к серверу с nightly dump, если dump забирается по `scp`

Проект не требует внешних Python-зависимостей.

## 1. Установка PostgreSQL

```bash
sudo apt update
sudo apt install -y postgresql-14 postgresql-client-14
```

Проверьте, что сервис поднят:

```bash
sudo systemctl enable postgresql
sudo systemctl start postgresql
sudo systemctl status postgresql
```

## 2. Проверка доступа к psql

Pipeline использует системный `psql` и `sudo -u postgres`.

Проверьте:

```bash
sudo -u postgres psql -c "SELECT version();"
sudo -u postgres psql -c "SELECT 1;"
```

Если это не работает, сначала нужно настроить права на sudo и доступ к локальному PostgreSQL.

## 3. Размещение проекта

Пример:

```bash
mkdir -p /workspace
cd /workspace
git clone <repo-url> etl_test
cd etl_test
```

## 4. Подготовка директорий

```bash
sudo mkdir -p /var/backups/postgres
sudo mkdir -p /tmp/pg_etl_temp
mkdir -p /workspace/logs
```

Если проект будет запускаться под конкретным пользователем, убедитесь, что ему доступны:
- директория логов
- временная директория
- директория архивов

## 5. Настройка конфигурации

Отредактируйте `config/etl_config.ini`.

Минимальный пример:

```ini
[database]
db_name = your_main_db
db_user = your_db_user
db_host = localhost
db_port = 5432
temp_db_prefix = temp_restore_

[paths]
backup_storage_dir = /var/backups/postgres
old_backup_dir = /workspace/old_backup
temp_dir = /tmp/pg_etl_temp
log_dir = /workspace/logs
log_file = /workspace/logs/etl_pipeline.log
asterisk_csv_file = /workspace/logs/asterisk_cdr.csv
group_employee_count_csv_file = /workspace/logs/group_employee_count_backup.csv
users_active_csv_file = /workspace/logs/users_active_backup.csv

[remote]
remote_user = your_remote_user
remote_host = your.remote.host
remote_path = /path/to/dumps/
backup_tar = nightly_dump.tar.gz

[tables]
auto_discover_tables = true
fail_on_unkeyed_tables = true
snapshot_excluded_tables =
snapshot_replace_tables = members, member_roles, changeset_parents, custom_fields_db_types, custom_fields_hrm_user_types, custom_workflows_projects, global_note_templates_projects
incremental_tables =
issues_table = issues
projects_table = projects

[retention]
# Всего 3 копии: текущая + 2 предыдущих
max_backups = 3
cleanup_temp_db = true

[schedule]
weekly_enabled = true
weekly_days = 1
target_day = 1
```

### Важно про snapshot-таблицы

При `auto_discover_tables=true` pipeline сам находит все обычные таблицы `public`
и использует их PRIMARY KEY либо UNIQUE index.

Таблицы без такого ключа, а также связанные таблицы с конфликтующими уникальными
ограничениями перечислите в `snapshot_replace_tables`. Вся группа будет атомарно
заменена полным срезом из staging. Порядок задаётся от родителя к дочерней таблице,
например `members, member_roles`. `snapshot_excluded_tables` используйте только
для данных, которые действительно не нужны в main.

`incremental_tables` нужен только для явного override ключа либо при отключенном
автообнаружении. Пример:

```ini
incremental_tables = users:id,orders:id,products:id
```

Если `issues` или `projects` не указать, они всё равно будут автоматически добавлены логикой pipeline через `issues_table` и `projects_table`.
`asterisk_cdr`, `group_employee_count` и `users_active` принудительно исключаются из `incremental_tables`: они принадлежат стабильной main БД и не выгружаются из staging.

## 6. Seed-файлы для ручных таблиц

Для CSV/manual таблиц используются seed CSV:
- `asterisk_cdr.csv`
- `group_employee_count_backup.csv`
- `users_active_backup.csv`

Рекомендуемые пути:

```bash
/workspace/logs/asterisk_cdr.csv
/workspace/logs/group_employee_count_backup.csv
/workspace/logs/users_active_backup.csv
```

Для `asterisk_cdr` CSV читается при каждом запуске стадии `seed_asterisk`, но в main добавляются только отсутствующие `id`.

Для `group_employee_count` и `users_active` seed применяется только если соответствующая таблица в main БД пустая.

## 7. Первый запуск

Инициализация создает main БД и загружает в нее первый dump.

### Task-версия

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/initial_dump.tar.gz
```

### Монолитная версия

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --init /path/to/initial_dump.tar.gz
```

После инициализации проверьте:

```bash
sudo -u postgres psql -d your_main_db -c "\dt"
sudo -u postgres psql -d your_main_db -c "SELECT COUNT(*) FROM issues;"
```

Если используются ручные таблицы:

```bash
sudo -u postgres psql -d your_main_db -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d your_main_db -c "SELECT COUNT(*) FROM group_employee_count;"
sudo -u postgres psql -d your_main_db -c "SELECT COUNT(*) FROM users_active;"
```

## 8. Nightly запуск

### Task-версия

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup
```

### Монолитная версия

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup
```

При заполненном `[remote]` архив всегда заново скачивается в `temp_dir`, даже если
cron передает существующий локальный путь. Ошибка `scp` останавливает pipeline:
fallback на предыдущий локальный архив запрещен. Локальный dump разрешается
только явно:

```bash
python3 scripts/etl_pipeline.py /path/to/dump.tar.gz --local-dump --config config/etl_config.ini --cleanup
```

Для `--local-dump` автоматическая ротация удаленных архивов не выполняется.

Флаг `--cleanup` удаляет staging БД после успешного завершения. При ошибке staging
и временные snapshot-файлы сохраняются для диагностики.

Перед каждым запуском pipeline дополнительно ищет и удаляет осиротевшие staging-БД
предыдущих запусков. В логе это отражается строкой `Staging cleanup завершен`.

## 8.1. Частичный запуск стадий

Обе версии поддерживают одинаковые стадии:

```text
extract, restore, seed_asterisk, compare, load, weekly, cleanup
```

Примеры:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly --force-weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk
```

И те же команды для монолита:

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks weekly --force-weekly
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk
```

Пояснения:
- `--tasks` задаёт только нужные стадии
- `--db-scope main|temp|auto` определяет БД для `restore`; `seed_asterisk` всегда дополняет main
- `--temp-db-name` нужен, если вы хотите запускать temp-стадии отдельно, без нового `restore`
- `--force-weekly` запускает weekly сейчас независимо от `weekly_days` и `weekly_enabled`
- `compare` и `load` работают только с `temp` scope
- `restore` автоматически добавит `extract`, если он не указан
- `load` автоматически добавит `compare`, если он не указан

## 9. Cron

Пример для task-версии:

```cron
0 2 * * * cd /workspace/etl_test && /usr/bin/python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup >> /workspace/logs/cron.log 2>&1
```

Пример для монолита:

```cron
0 2 * * * cd /workspace/etl_test && /usr/bin/python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup >> /workspace/logs/cron_legacy.log 2>&1
```

До включения cron создайте каталоги и выдайте пользователю запуска права на них:

```bash
sudo mkdir -p /var/backups/postgres /workspace/old_backup /workspace/logs
sudo chown -R tehpod:tehpod /var/backups/postgres /workspace/old_backup /workspace/logs
```

При `max_backups=3` pipeline хранит текущий архив в `backup_storage_dir` и два
предыдущих архива в `/workspace/old_backup/`. Ротация выполняется только после
успешного восстановления и переноса данных в main БД.

## 10. Что проверить перед включением в прод

1. Main БД уже не должна удаляться nightly-процессом.
2. Nightly dump должен успешно подниматься во временной staging БД.
3. Все рабочие таблицы должны иметь PRIMARY KEY/UNIQUE index либо быть явно исключены.
4. CSV для append `asterisk_cdr` и init-only seed CSV должны лежать в ожидаемом месте.
5. Пользователь, который запускает cron, должен иметь доступ к `sudo -u postgres`.
6. Snapshot `issues/projects` не должен содержать колонки `cf_*`.

## 11. Диагностика

### Проверка логов

```bash
tail -f /workspace/logs/etl_pipeline.log
```

### Список баз

```bash
sudo -u postgres psql -c "\l"
```

### Проверка временных БД

```bash
sudo -u postgres psql -c "SELECT datname FROM pg_database WHERE datname LIKE 'temp_restore_%';"
```

### Проверка блокировки custom values

```bash
sudo -u postgres psql -d your_main_db -c "\d issues"
sudo -u postgres psql -d your_main_db -c "\d projects"
```

Ранее созданные `cf_*` остаются в main БД. Pipeline не удаляет их, не обновляет
из custom values и не переносит через snapshot.

## 12. Частые проблемы

### `database does not exist`

Сначала выполните `--init`.

### `Permission denied` при `scp`

Проверьте SSH-доступ:

```bash
ssh your_remote_user@your_remote_host
```

### `psql` не выполняется через sudo

Проверьте:

```bash
sudo -u postgres psql -c "SELECT 1;"
```

### В snapshot обнаружены `cf_*`

Pipeline намеренно остановит UPSERT `issues/projects`, если входной snapshot
содержит `cf_*`. Проверьте, что используется актуальный код и snapshot создан
новой стадией `compare`.

### Старые `cf_*` остались в main

Это ожидаемо: одноразовое удаление больше не входит в pipeline. Если понадобится
отдельная очистка схемы, выполняйте ее как контролируемую миграцию вне nightly.
Pipeline намеренно не использует `CASCADE`.
