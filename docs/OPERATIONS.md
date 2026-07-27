# Operations Runbook

## Назначение документа

Этот документ описывает:
- фактический nightly-процесс
- роль main и staging БД
- поведение ручных таблиц
- смысл custom transform
- что проверять при rollout и поддержке

## 1. Модель данных и поток

### Main database

`db_name` из конфига — это стабильная рабочая БД.

Она:
- не удаляется каждый день
- накапливает актуальное состояние snapshot-таблиц
- хранит ручные таблицы
- хранит материализованные `cf_*` колонки в `issues` и `projects`

### Staging database

Каждый nightly dump поднимается во временной БД:
- имя строится из `temp_db_prefix + timestamp`
- после обработки staging БД можно удалить

Staging нужна потому, что входящий dump нельзя менять и нельзя разворачивать прямо в main.

## 2. Порядок nightly-обработки

1. Получить dump локально или через `scp`.
2. Распаковать dump.
3. Создать staging БД.
4. Восстановить SQL-файлы в staging БД.
5. Создать служебные таблицы, если они нужны.
6. Если `asterisk_cdr` отсутствует в dump, подгрузить её из отдельного CSV в staging.
7. Добавить `project_id` в `asterisk_cdr`, если нужно.
8. Экспортировать snapshot-таблицы в CSV.
9. Выполнить `UPSERT` этих snapshot-таблиц в main.
10. Выполнить weekly-upsert для ручных таблиц.
11. Выдать права.
12. Очистить staging БД и временные каталоги.

## 2.1. Операционные стадии

И монолит, и task-версия теперь поддерживают одинаковые стадии:
- `extract`
- `restore`
- `seed_asterisk`
- `transform_custom_values`
- `compare`
- `load`
- `weekly`
- `cleanup`

Это позволяет запускать не только полный nightly-поток, но и отдельные шаги для диагностики или ручного обслуживания.

Типовые сценарии:
- пересчитать `cf_*` в основной БД: `--tasks transform_custom_values --db-scope main`
- принудительно выполнить weekly-логику: `--tasks weekly`
- отдельно догрузить `asterisk_cdr`: `--tasks seed_asterisk --db-scope main`
- поднять staging и остановиться после transform: `--tasks restore,seed_asterisk,transform_custom_values --db-scope temp <dump>`

## 3. Snapshot-таблицы

Snapshot-таблицы задаются через:

```ini
[tables]
incremental_tables = table_a:id,table_b:id
issues_table = issues
projects_table = projects
```

`issues` и `projects` можно не дублировать в `incremental_tables`, если они заданы через `issues_table` и `projects_table`.

`group_employee_count` и `users_active` не должны включаться в `incremental_tables`, потому что они живут в main БД как ручные weekly-таблицы.

Смысл:
- таблица из dump считается текущим срезом данных
- этот срез выгружается из staging
- затем вливается в main через `INSERT ... ON CONFLICT DO UPDATE`

### Что это означает practically

- новые строки появятся в main
- измененные строки обновятся в main
- удаленные строки автоматически не удаляются

Если нужна поддержка удалений, её надо проектировать отдельно.

## 4. Ручные таблицы

### Какие это таблицы

- `group_employee_count`
- `users_active`
- `asterisk_cdr`

### Почему они особенные

`group_employee_count` и `users_active` не приходят из внешнего dump и не должны зависеть от nightly full restore.

`asterisk_cdr` в текущем процессе тоже может жить как внешний CSV-источник, если в nightly dump её нет.

Исторически в `dump_restore.sh` они:
- выгружались в CSV
- main БД пересоздавалась
- CSV заливались обратно

Отдельно `asterisk_cdr` загружалась из внешнего `CSV_FILE` с разделителем `;`.

Теперь это не нужно, потому что main БД больше не удаляется.

### Новая логика

При `--init`:
- таблицы создаются, если отсутствуют
- если они пустые, можно загрузить их из seed CSV
- `asterisk_cdr` может быть импортирована из `asterisk_csv_file`
- `group_employee_count` и `users_active` могут быть импортированы из `group_employee_count_csv_file` и `users_active_csv_file`

При nightly run:
- pipeline не перезаливает эти таблицы из CSV
- pipeline только выполняет weekly-upsert
- `asterisk_cdr` при необходимости подгружается в staging из CSV и затем попадает в main как snapshot-таблица

### Seed CSV

Seed CSV нужны только как стартовая точка, если надо сохранить старые ручные данные:
- `asterisk_cdr.csv`
- `group_employee_count_backup.csv`
- `users_active_backup.csv`

Для `asterisk_cdr` seed может применяться в пустую staging/main БД.

Для `group_employee_count` и `users_active` seed применяется один раз, если таблица пустая.

## 5. Weekly logic

### `group_employee_count`

В целевой день недели:
- агрегируется актуальное количество пользователей по группе
- запись вставляется по `group_id + snapshot_date`
- если запись за день уже есть, она обновляется

### `users_active`

В целевой день недели:
- считается число активных пользователей
- запись вставляется по `snapshot_date`
- если запись за день уже есть, она обновляется

Это поведение идемпотентно: повторный запуск в тот же день не плодит дубликаты.

Важно:
- weekly всегда должен выполняться после `load`, если нужен полный nightly-сценарий
- weekly можно запускать отдельно, без dump, потому что источник данных уже находится в `main_db`

## 6. Custom transform

Custom transform не входит в стандартный `--init` или nightly-поток. Стадия сохранена только для явного ручного запуска через `--tasks transform_custom_values`.

### Что трансформируется

Из:
- `issues`
- `projects`
- `custom_values`
- `custom_fields`

В:
- `issues.cf_*`
- `projects.cf_*`

### Зачем это нужно

Чтобы аналитика читала уже материализованные custom-поля напрямую из `issues` и `projects`, без постоянных JOIN к `custom_values`.

### Текущий подход

1. Получить все custom fields для `Issue` и `Project`.
2. Сгенерировать для них имена `cf_*`.
3. Создать недостающие колонки.
4. Агрегировать значения по `customized_id`.
5. Обновить `issues` и `projects` отдельными set-based SQL.

### Если шаг запускается в staging

Чтобы результат попал в main, после ручного transform в staging нужно выполнить `compare,load`. Без явного transform стандартный nightly не создаёт и не обновляет `cf_*`.

## 7. Индексы и производительность

`custom transform` потенциально самое дорогое место.

Перед rollout стоит проверить индексы на:
- `custom_values`
- `issues`
- `projects`
- PK таблиц из `incremental_tables`

Минимум стоит посмотреть:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('custom_values', 'issues');
-- при необходимости добавьте 'projects'
```

Особенно полезны индексы, покрывающие:
- `customized_type`
- `customized_id`
- `custom_field_id`

## 8. Что смотреть при первом rollout

### После `--init`

Проверить:
- main БД создана
- ручные таблицы существуют
- `cf_*` проверяются только если custom transform был запущен вручную
- seed CSV импортировались, если таблицы были пусты

### После первого nightly

Проверить:
- staging БД создалась и удалилась
- `issues` и `projects` в main обновились
- `UPSERT` сработал по нужным таблицам
- weekly-таблицы не получили дубликаты

## 9. Operational checklist

Перед nightly:
- доступен PostgreSQL
- доступен remote dump или локальный dump
- в конфиге корректен `db_name`
- в конфиге корректен `incremental_tables`

После nightly:
- нет висящих staging БД
- нет ошибок в `etl_pipeline.log`
- размеры экспортов выглядят ожидаемо
- `issues` и `projects` визуально корректны; `cf_*` проверяются только для ручного transform

## 10. Когда использовать монолит, когда tasks

### Монолит

Используйте, если:
- нужен один сценарий без orchestration-слоя
- удобнее дебажить один файл верхнего уровня

### Task-версия

Используйте, если:
- нужно расширять процесс отдельными шагами
- важна наблюдаемая последовательность задач
- хочется тестировать этапы отдельно

С точки зрения бизнес-потока обе версии должны оставаться одинаковыми.
