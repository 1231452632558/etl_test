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
incremental_tables = asterisk_cdr:id
issues_table = issues
projects_table = projects

[retention]
max_backups = 3
cleanup_temp_db = true

[schedule]
target_day = 1
```

### Важно про `incremental_tables`

Сюда надо включать таблицы, которые должны попадать из staging в main через snapshot + `UPSERT`.

Пример:

```ini
incremental_tables = users:id,orders:id,products:id,asterisk_cdr:id
```

Если `issues` или `projects` не указать, они всё равно будут автоматически добавлены логикой pipeline через `issues_table` и `projects_table`.
`group_employee_count` и `users_active` не добавляются в `incremental_tables`, потому что это ручные weekly-таблицы, а не snapshot-таблицы из nightly dump.

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

Для `asterisk_cdr` seed применяется если таблица пустая в main или staging БД.

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
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

### Монолитная версия

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

Флаг `--cleanup` удаляет staging БД после завершения.

## 9. Cron

Пример для task-версии:

```cron
0 2 * * * cd /workspace/etl_test && /usr/bin/python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz >> /workspace/logs/cron.log 2>&1
```

Пример для монолита:

```cron
0 2 * * * cd /workspace/etl_test && /usr/bin/python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz >> /workspace/logs/cron_legacy.log 2>&1
```

## 10. Что проверить перед включением в прод

1. Main БД уже не должна удаляться nightly-процессом.
2. Nightly dump должен успешно подниматься во временной staging БД.
3. Таблицы из `incremental_tables` должны иметь корректные PK для `UPSERT`.
4. Seed CSV для ручных таблиц должны лежать в ожидаемом месте.
5. Пользователь, который запускает cron, должен иметь доступ к `sudo -u postgres`.
6. На тестовом прогоне нужно вручную сравнить `issues`, `projects` и их `cf_*` колонки после custom transform.

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

### Проверка custom transform

```bash
sudo -u postgres psql -d your_main_db -c "\d issues"
sudo -u postgres psql -d your_main_db -c "\d projects"
sudo -u postgres psql -d your_main_db -c "SELECT id, * FROM issues LIMIT 5;"
sudo -u postgres psql -d your_main_db -c "SELECT id, * FROM projects LIMIT 5;"
```

### Проверка индексов на custom_values

```bash
sudo -u postgres psql -d your_main_db -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'custom_values';"
```

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

### Не создаются `cf_*` колонки

Проверьте наличие:
- `issues`
- `projects`
- `custom_values`
- `custom_fields`

И посмотрите индексы на `custom_values`.
