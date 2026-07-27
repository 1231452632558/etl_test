# Testing Methodology

Документ описывает, как проверить новый Python pipeline на виртуальной машине, где уже есть исходники проекта.

Цель тестирования:
- убедиться, что main БД больше не пересоздается nightly
- проверить restore в staging БД
- проверить загрузку `asterisk_cdr` из CSV при необходимости
- проверить, что materialized `cf_*` не попадают в `issues/projects`
- проверить `UPSERT` snapshot-таблиц
- проверить weekly/manual таблицы
- проверить очистку staging БД

## 1. Что должно быть на ВМ

На виртуальной машине должны быть:
- исходники репозитория
- PostgreSQL 14
- доступ к `sudo -u postgres psql`
- тестовый dump
- seed CSV для ручных/CSV таблиц, если они нужны

Минимальные проверки:

```bash
cd /path/to/repo
python3 --version
sudo -u postgres psql -c "SELECT version();"
sudo -u postgres psql -c "SELECT 1;"
```

## 2. Какие данные нужны для проверки

### Обязательные

1. Один dump для инициализации.
2. Один более новый dump для nightly прогона.

### Желательные

1. Дамп, содержащий `issues` и `projects`; желательно с тестовыми `cf_*`,
   чтобы проверить их исключение из snapshot.
2. CSV для `asterisk_cdr`, если `asterisk_cdr` не приходит во входящем dump.
3. CSV:
   - `group_employee_count_backup.csv`
   - `users_active_backup.csv`

## 3. Подготовка конфига

Откройте `config/etl_config.ini` и проверьте:

```ini
[database]
db_name = test_main_db
temp_db_prefix = temp_restore_

[paths]
log_file = /workspace/logs/etl_pipeline.log
asterisk_csv_file = /workspace/logs/asterisk_cdr.csv
group_employee_count_csv_file = /workspace/logs/group_employee_count_backup.csv
users_active_csv_file = /workspace/logs/users_active_backup.csv

[tables]
incremental_tables = asterisk_cdr:id
issues_table = issues
projects_table = projects
```

Важно:
- `group_employee_count` и `users_active` не должны лежать в `incremental_tables`
- `issues` и `projects` можно не добавлять руками, если заданы `issues_table` и `projects_table`

## 4. Подготовка директорий и seed CSV

```bash
mkdir -p /workspace/logs
sudo mkdir -p /tmp/pg_etl_temp
```

Если у вас есть seed CSV, положите их сюда:

```text
/workspace/logs/asterisk_cdr.csv
/workspace/logs/group_employee_count_backup.csv
/workspace/logs/users_active_backup.csv
```

Важно:
- `asterisk_cdr.csv` должна лежать ровно по пути, указанному в `asterisk_csv_file`
- если файл лежит, например, в `/workspace/asterisk_cdr.csv`, а в конфиге указано `/workspace/logs/asterisk_cdr.csv`, pipeline его не найдет
- `group_employee_count_backup.csv` и `users_active_backup.csv` должны лежать ровно по путям `group_employee_count_csv_file` и `users_active_csv_file`

## 5. Очистка тестового окружения

Перед прогоном зафиксируйте старые тестовые БД:

```bash
sudo -u postgres psql -c \"SELECT datname FROM pg_database WHERE datname LIKE 'temp_restore_%';\"
```

После старта pipeline проверьте, что осиротевшие staging-БД удалены автоматически.
База с активным подключением должна быть пропущена с предупреждением, а
`--temp-db-name` должна быть защищена от удаления.

## 6. Тест 1. Smoke-check на простых дампах

В репозитории уже есть `run_tests.sh`, но это только быстрый smoke-test на простых таблицах `users/orders/products`.

Запуск:

```bash
./run_tests.sh
```

Что он проверяет:
- базовый запуск `--init`
- базовый nightly run
- работоспособность task-entrypoint

Что он не проверяет полноценно:
- блокировку `cf_*` для `issues/projects`
- `asterisk_cdr` CSV-source
- weekly/manual таблицы
- продовую схему данных

## 7. Тест 2. Инициализация новой main БД

Запустите `--init`.

### Task-версия

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/initial_dump.tar.gz
```

### Монолит

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --init /path/to/initial_dump.tar.gz
```

Проверить:

```bash
sudo -u postgres psql -d test_main_db -c "\dt"
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM group_employee_count;"
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM users_active;"
```

Ожидаемый результат:
- main БД создана
- служебные таблицы существуют
- `asterisk_cdr` заполнена данными только если:
  - таблица пришла во входящем dump
  - или CSV найдена по пути `asterisk_csv_file`
- `group_employee_count` и `users_active` существуют как таблицы даже если соответствующие CSV не найдены
- `group_employee_count` и `users_active` могут остаться пустыми, если seed CSV отсутствуют

## 8. Тест 3. Проверка блокировки custom values

После `compare/load` pipeline не должен создавать, обновлять или переносить
колонки `cf_*` в `issues/projects`.

Проверьте:

```bash
sudo -u postgres psql -d test_main_db -c "\d issues"
sudo -u postgres psql -d test_main_db -c "\d projects"
```

Ожидаемый результат:
1. В плане стадий нет `transform_custom_values`.
2. В логах snapshot для `issues/projects` колонки `cf_*` отсутствуют.
3. Если staging уже содержит `cf_*`, появляется сообщение об их исключении.
4. Если в ручной CSV для UPSERT подмешаны `cf_*`, загрузка останавливается.

Существующие старые `cf_*` в main БД могут оставаться неизменными. Их удаление
не является частью pipeline и тестируется как отдельная миграция.

## 9. Тест 4. Nightly run без удаления main БД

Запустите nightly dump.

### Task-версия

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

### Монолит

```bash
python3 scripts/legacy/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

Проверить:

```bash
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname = 'test_main_db';"
sudo -u postgres psql -c "SELECT datname FROM pg_database WHERE datname LIKE 'temp_restore_%';"
```

Ожидаемый результат:
- `test_main_db` продолжает существовать
- staging БД создается на время прогона
- после `--cleanup` staging БД удаляется

## 10. Тест 5. Проверка snapshot-upsert

Если в nightly dump есть обновления по snapshot-таблицам:

```bash
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d test_main_db -c "SELECT * FROM asterisk_cdr ORDER BY id LIMIT 20;"
```

Проверить:
1. Новые строки появились.
2. Измененные строки обновились.
3. Main БД не пересоздавалась.

Если в конфиге есть другие snapshot-таблицы, проверьте их так же.

## 11. Тест 6. Проверка `asterisk_cdr` CSV-source

Нужен сценарий, где:
- входящий dump не содержит `asterisk_cdr`
- есть `asterisk_cdr.csv`

Проверить:
1. Очистить `asterisk_cdr` в тестовой БД или взять пустую цель.
2. Запустить pipeline.
3. Убедиться, что `asterisk_cdr` наполнилась из CSV.
4. Проверить, что `project_id` проставлен.

Проверки:

```bash
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d test_main_db -c "SELECT COUNT(*) FROM asterisk_cdr WHERE project_id IS NULL;"
```

Ожидаемый результат:
- строки есть
- `project_id IS NULL` дает `0`

## 12. Тест 7. Проверка weekly/manual таблиц

Нужно проверить:
- сохранение старых данных
- отсутствие дублей
- корректный upsert за текущую дату

Проверки:

```bash
sudo -u postgres psql -d test_main_db -c "SELECT * FROM group_employee_count ORDER BY snapshot_date DESC, group_id LIMIT 20;"
sudo -u postgres psql -d test_main_db -c "SELECT * FROM users_active ORDER BY snapshot_date DESC LIMIT 20;"
```

Ожидаемый результат:
- старые записи сохранились
- за одну дату нет дублей по бизнес-ключам

## 13. Тест 8. Сравнение task и монолита

Для полной проверки стоит прогнать обе версии на одинаковых входных данных.

Подход:
1. Создать две отдельные тестовые main БД.
2. На первой прогнать task-версию.
3. На второй прогнать монолит.
4. Сравнить:
   - набор таблиц
   - row count в snapshot-таблицах
   - отсутствие стадии transform в обоих вариантах
   - отсутствие `cf_*` в snapshot и UPSERT
   - `group_employee_count`
   - `users_active`

Пример запросов:

```bash
sudo -u postgres psql -d test_main_db_task -c "SELECT COUNT(*) FROM issues;"
sudo -u postgres psql -d test_main_db_mono -c "SELECT COUNT(*) FROM issues;"
sudo -u postgres psql -d test_main_db_task -c "SELECT COUNT(*) FROM projects;"
sudo -u postgres psql -d test_main_db_mono -c "SELECT COUNT(*) FROM projects;"
```

## 14. Что смотреть в логах

Основной лог:

```bash
tail -f /workspace/logs/etl_pipeline.log
```

Ищите:
- автоматическую очистку старых staging-БД
- создание staging БД
- restore dump
- отсутствие стадии `transform_custom_values`
- исключение `cf_*` из snapshot `issues/projects`
- snapshot export
- upsert в main
- weekly update
- cleanup staging БД

## 14.1. Проверка отдельных стадий

Для targeted-проверок можно гонять только нужный шаг.

Примеры:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk --db-scope main
```

Если нужно проверить snapshot на уже существующей staging БД:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks compare --db-scope temp --temp-db-name temp_restore_20260507_123456
```

Что проверять:
1. В начале лога печатается план стадий.
2. Для `compare` видно исключение `cf_*`, если такие колонки есть.
3. Для `weekly` видны `snapshot_date`, counts и debug sample.
4. Для `seed_asterisk` виден статус `seeded` или `skipped`.

## 15. Критерии успешного теста

Тест считается успешным, если:
1. Main БД не удаляется nightly.
2. Staging БД создается и очищается.
3. Стадии custom transform нет, а `cf_*` не попадают в snapshot/UPSERT.
4. `asterisk_cdr` присутствует после прогона.
5. `group_employee_count` и `users_active` не теряют историю.
6. Нет дублей в weekly-таблицах.
7. Task и монолит дают эквивалентный результат.

## 16. Если найден дефект

Зафиксируйте:
1. Какой dump использовался.
2. Какая версия запускалась: task или монолит.
3. Команду запуска.
4. Фрагмент лога.
5. SQL, которым подтверждается расхождение.

Минимальный шаблон:

```text
Version: task | monolith
Command:
Dump:
Expected:
Actual:
Relevant log lines:
SQL proof:
```
