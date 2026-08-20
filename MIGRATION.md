# Migration Guide

Пошаговый план перехода с текущего `dump_restore.sh` на новый Python nightly pipeline.

Документ рассчитан на продовый сценарий, где:
- входящий dump нельзя изменить
- dump можно развернуть только в новую БД
- рабочая БД больше не должна пересоздаваться каждую ночь

## Цель миграции

Заменить процесс:
- nightly `DROP DATABASE`
- nightly `CREATE DATABASE`
- обратная заливка CSV после полного восстановления

на процесс:
- restore dump во временную staging БД
- `UPSERT` snapshot-таблиц в стабильную main БД
- weekly-обновление ручных таблиц

## Что не меняется

- Источник nightly dump
- Формат dump
- Необходимость поднимать dump в новую staging БД
- Логика weekly-таблиц как отдельного доменного шага

## Что меняется

- Main БД становится долгоживущей
- `issues` и `projects` загружаются в main без автоматической материализации custom values
- `asterisk_cdr` дополняется напрямую из CSV в main по отсутствующим `id`
- `group_employee_count` и `users_active` больше не живут через nightly export/import

## Выбор Python-версии

Варианты:
- `scripts/etl_pipeline.py` — task-версия
- `scripts/legacy/etl_pipeline.py` — монолит

Обе версии должны делать одно и то же.

Для rollout лучше выбрать одну как основную. Рекомендуемый вариант: `scripts/etl_pipeline.py`.

При этом монолит и task-версия должны оставаться равноправными:
- одинаковые стадии
- одинаковая nightly-логика
- одинаковые ключи `--tasks`, `--db-scope`, `--temp-db-name`, `--skip-weekly`, `--force-weekly`

## Перед началом

Подготовьте:
1. Доступ к текущему prod-серверу и текущей main БД.
2. Последний успешный nightly dump.
3. CSV seed-файлы:
   - `asterisk_cdr.csv`
   - `group_employee_count_backup.csv`
   - `users_active_backup.csv`
4. Окно сопровождения на первый переключаемый запуск.
5. План отката на `dump_restore.sh`.

## Этап 1. Инвентаризация текущего состояния

Нужно зафиксировать:
1. Имя текущей рабочей БД.
2. Какие таблицы реально должны идти через snapshot nightly-поток.
3. Где сейчас лежит CSV для `asterisk_cdr`.
4. Где лежат CSV для `group_employee_count` и `users_active`.
5. Какие индексы уже есть на `issues` и `projects`.

Проверки:

```bash
sudo -u postgres psql -d <main_db> -c "\dt"
sudo -u postgres psql -d <main_db> -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('issues', 'projects');"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM group_employee_count;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM users_active;"
```

## Этап 2. Подготовка конфигурации

Проверьте `config/etl_config.ini`.

Критичные настройки:
- `db_name` — текущая рабочая БД
- `temp_db_prefix` — staging prefix
- `asterisk_csv_file` — путь к CSV для `asterisk_cdr`
- `group_employee_count_csv_file` — путь к CSV для `group_employee_count`
- `users_active_csv_file` — путь к CSV для `users_active`
- `issues_table = issues`
- `projects_table = projects`
- `incremental_tables` — только snapshot-таблицы
- `weekly_enabled` и `weekly_days` — включение и дни weekly-среза

Важно:
- `issues` и `projects` можно не добавлять в `incremental_tables`, они подтянутся автоматически через `issues_table` и `projects_table`
- `asterisk_cdr`, `group_employee_count` и `users_active` не должны лежать в `incremental_tables`

Минимальный пример:

```ini
[tables]
incremental_tables =
issues_table = issues
projects_table = projects

[schedule]
weekly_enabled = true
weekly_days = 1
```

## Этап 3. Подготовка seed CSV

Убедитесь, что seed-файлы лежат в ожидаемых местах.

По умолчанию:

```text
/workspace/logs/asterisk_cdr.csv
/workspace/logs/group_employee_count_backup.csv
/workspace/logs/users_active_backup.csv
```

Правила:
- `asterisk_cdr.csv` является append-only источником для стабильной main БД
- `group_employee_count_backup.csv` и `users_active_backup.csv` нужны как стартовый seed для пустой main БД

## Этап 4. Прогон в тестовом окружении

Перед prod rollout обязательно прогоните сценарий на тестовой копии:

1. Создайте тестовую main БД.
2. Выполните `--init`.
3. Выполните один nightly run на свежем dump.
4. Сравните данные с ожидаемым результатом.

Команды:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/dump.tar.gz
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

Что проверить:
- `issues` и `projects` обновились через snapshot-поток
- `asterisk_cdr` загружена корректно
- `group_employee_count` и `users_active` не потеряли исторические данные
- staging БД очищается после завершения

## Этап 5. Бэкап перед prod switch

Перед первым продовым переключением сделайте резервную копию текущей main БД:

```bash
pg_dump -U postgres <main_db> > backup_before_python_migration.sql
```

Если база большая, используйте принятый у вас способ полного бэкапа.

## Этап 6. Первый запуск в проде

### Вариант A. Main БД уже существует и должна остаться

Это основной сценарий для миграции с `dump_restore.sh`.

В этом случае:
- `--init` для текущей prod main БД не нужен
- нужен первый nightly run новым Python pipeline

Команда:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz
```

Что произойдет:
1. dump поднимется во staging
2. в main добавятся отсутствующие строки `asterisk_cdr` из CSV
3. snapshot-таблицы будут влиты в main через `UPSERT`
4. weekly-таблицы обновятся отдельным шагом

### Вариант B. Нужна новая инициализация новой main БД

Если вы поднимаете новую рабочую БД с нуля:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --init /path/to/initial_dump.tar.gz
```

После этого уже переходите на nightly run.

## Этап 7. Валидация после первого запуска

Проверить:

```bash
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM issues;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM projects;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM asterisk_cdr;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM group_employee_count;"
sudo -u postgres psql -d <main_db> -c "SELECT COUNT(*) FROM users_active;"
sudo -u postgres psql -d <main_db> -c "\d issues"
sudo -u postgres psql -d <main_db> -c "\d projects"
```

Дополнительно:
1. Визуально проверить несколько `issues` и `projects`.
2. Проверить, что `project_id` в `asterisk_cdr` задан.
3. Проверить, что weekly-таблицы не задублировались.

## Этап 8. Переключение cron

После успешного первого прогона:
1. Отключите cron с `dump_restore.sh`.
2. Добавьте cron на Python pipeline.

Пример:

```cron
0 2 * * * cd /workspace/etl_test && /usr/bin/python3 scripts/etl_pipeline.py --config config/etl_config.ini --cleanup /path/to/nightly_dump.tar.gz >> /workspace/logs/cron.log 2>&1
```

На первую неделю полезно оставить старый shell-скрипт рядом, но выключенным.

## Этап 9. Наблюдение после переключения

В первые 3-7 дней отслеживайте:
1. Ошибки в `etl_pipeline.log`
2. Появление неожиданных staging БД
3. Корректность `asterisk_cdr`
4. Дубли в weekly-таблицах

Для диагностики после переключения можно запускать отдельные стадии:

```bash
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks weekly --force-weekly
python3 scripts/etl_pipeline.py --config config/etl_config.ini --tasks seed_asterisk
```

Проверки:

```bash
tail -f /workspace/logs/etl_pipeline.log
sudo -u postgres psql -c "SELECT datname FROM pg_database WHERE datname LIKE 'temp_restore_%';"
```

## Этап 10. Откат

Если новый pipeline ведет себя некорректно:
1. Остановите новый cron.
2. Верните cron на `dump_restore.sh`.
3. При необходимости восстановите main БД из предмиграционного бэкапа.

Минимальный rollback:
- вернуть старый cron
- не использовать новые nightly Python-запуски до разбора проблемы

## Checklist

Перед переключением:
- есть backup main БД
- проверен `sudo -u postgres psql`
- проверен доступ к nightly dump
- seed CSV лежат в нужных местах
- `incremental_tables` не содержит три ручные таблицы
- `issues_table` и `projects_table` заданы

После переключения:
- nightly run завершился без ошибок
- staging БД удалена
- `issues` и `projects` обновлены; новые custom values не перенесены, существующие `cf_*` не удалялись
- `asterisk_cdr` на месте
- `group_employee_count` и `users_active` не потеряли историю

## Связанные документы

- Общее описание: `README.md`
- Установка: `INSTALL.md`
- Операционный runbook: `docs/OPERATIONS.md`
