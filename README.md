# ETL Pipeline для PostgreSQL (Модульная версия)

Python-скрипт для обработки nightly SQL-дампов PostgreSQL с инкрементальным обновлением данных.
**Версия 2.0**: Рефакторинг с модульной архитектурой на основе задач (tasks).

## Особенности

### Архитектура на основе задач (Task-Based)

ETL pipeline разделен на независимые модульные задачи:

1. **ExtractTask** - Извлечение и подготовка дампа
2. **RestoreTask** - Восстановление дампа во временную базу
3. **CompareTask** - Сравнение данных и поиск новых/измененных строк
4. **TransformCustomValuesTask** - Трансформация custom values в столбцы таблицы issues
5. **LoadTask** - Загрузка изменений в основную базу
6. **WeeklyTask** - Еженедельные задачи (добавление агрегированных записей)
7. **CleanupTask** - Очистка временных ресурсов

### Ключевые возможности

- **Инкрементальное обновление**: Сохраняет основную базу, добавляет только изменения
- **Трансформация custom fields**: Автоматическое создание колонок и заполнение данными из custom_values
- **Умное сравнение**: Находит новые и измененные строки по первичным ключам
- **Еженедельная агрегация**: Добавление записей в group_employee_count и users_active
- **Ротация бэкапов**: Хранение последних N бэкапов
- **Детальное логирование**: Все операции логируются с временными метками

### Что делает новый скрипт:

#### Первый запуск (--init):
- Создает основную базу данных
- Загружает начальный дамп полностью
- Создает необходимые таблицы и индексы

#### Ночная обработка:
1. Разворачивает новый дамп во временной базе
2. Сравнивает данные по таблицам из `incremental_tables`
3. Находит **новые строки** (есть во временной, нет в основной)
4. Находит **измененные строки** (есть в обеих, но значения отличаются)
5. **Трансформирует custom values**: 
   - Для каждой задачи из `issues` находит соответствующие записи в `custom_values`
   - Получает имена полей из `custom_fields` по `custom_field_id`
   - Транспонирует строки custom_values в столбцы таблицы issues
6. Применяет изменения в основную базу через UPSERT
7. Выполняет еженедельные задачи (если сегодня целевой день)
8. Удаляет временную базу после завершения

## Требования

- **ОС**: Ubuntu 22.04
- **PostgreSQL**: 14
- **Python**: 3.x (стандартная библиотека, без внешних зависимостей)
- **Доступ**: права на выполнение команд от имени пользователя postgres

## Установка

### Структура проекта

```bash
/workspace/
├── scripts/
│   ├── etl_pipeline.py          # Основной скрипт (оркестратор)
│   ├── etl_pipeline_tasks.py    # Конфигурация задач пайплайна
│   └── tasks/
│       ├── __init__.py          # Инициализация модуля задач
│       ├── base.py              # Базовый класс BaseTask
│       ├── task_runner.py       # Оркестратор задач (TaskRunner)
│       ├── extract_task.py      # Задача извлечения дампа
│       ├── restore_task.py      # Задача восстановления во временную базу
│       ├── compare_task.py      # Задача сравнения данных
│       ├── transform_custom_values_task.py  # Трансформация custom values
│       ├── load_task.py         # Загрузка изменений в основную базу
│       ├── weekly_task.py       # Еженедельные задачи
│       └── cleanup_task.py      # Очистка временных ресурсов
├── config/
│   └── etl_config.ini           # Конфигурация
├── logs/                         # Логи (создается автоматически)
└── dumps/                        # Тестовые дампы (опционально)
```

### Шаги установки

1. Скопируйте файлы в нужные директории (см. структуру выше)

2. Отредактируйте конфигурационный файл `/workspace/config/etl_config.ini`:

```ini
[database]
db_name = ваше_имя_базы
db_user = ваш_пользователь
db_host = localhost
db_port = 5432
temp_db_prefix = temp_restore_

[paths]
backup_storage_dir = /var/backups/postgres
temp_dir = /tmp/pg_etl_temp
log_dir = /workspace/logs
log_file = /workspace/logs/etl_pipeline.log

[remote]
remote_user = пользователь_remote
remote_host = host.remote.com
remote_path = /path/to/dumps/
backup_tar = backup.tar.gz
backup_sql = backup.sql

[tables]
# Таблицы для инкрементального обновления
incremental_tables = group_employee_count:id,users_active:id,asterisk_cdr:id

# Таблица для трансформации custom values
issues_table = issues

[retention]
max_backups = 5
cleanup_temp_db = true

[schedule]
target_day = 1  # 1-понедельник, 7-воскресенье
```

3. Создайте директорию для логов:
```bash
sudo mkdir -p /workspace/logs
sudo chown $USER:$USER /workspace/logs
```

4. Создайте директорию для бэкапов:
```bash
sudo mkdir -p /var/backups/postgres
sudo chown $USER:$USER /var/backups/postgres
```

## Использование

### Первый запуск (инициализация)

```bash
python3 /workspace/scripts/etl_pipeline.py --init /path/to/initial_dump.tar.gz
```

Этот режим:
- Создает основную базу данных
- Загружает дамп полностью
- Создает необходимые таблицы и индексы
- Предоставляет права пользователю

### Ночная обработка (ежедневный запуск)

```bash
python3 /workspace/scripts/etl_pipeline.py /path/to/nightly_dump.tar.gz
```

Или с явной очисткой временной базы:
```bash
python3 /workspace/scripts/etl_pipeline.py --cleanup /path/to/nightly_dump.tar.gz
```

Этот режим выполняет последовательность задач:
1. **ExtractTask**: Копирует дамп с удаленного сервера (если настроено)
2. **RestoreTask**: 
   - Выполняет ротацию старых бэкапов
   - Создает временную базу и разворачивает дамп в ней
3. **CompareTask**: 
   - Выгружает текущие данные в CSV (для безопасности)
   - **Находит новые строки** по каждой таблице из incremental_tables
   - **Находит измененные строки** сравнивая значения
4. **TransformCustomValuesTask**:
   - Получает список custom fields для Issue из таблицы custom_fields
   - Добавляет новые колонки в таблицу issues (если их нет)
   - Транспонирует custom_values в столбцы issues
5. **LoadTask**:
   - Применяет изменения в основную базу через UPSERT
   - Загружает обратно сохраненные CSV
   - Предоставляет права пользователю
6. **WeeklyTask**: Добавляет еженедельные записи (если сегодня целевой день)
7. **CleanupTask**: Удаляет временную базу

### Настройка автоматического запуска (cron)

Для ежедневного запуска в 2:00 ночи:

```bash
crontab -e
```

Добавьте строку:
```cron
0 2 * * * /usr/bin/python3 /workspace/scripts/etl_pipeline.py /path/to/nightly_dump.tar.gz --cleanup >> /workspace/logs/cron.log 2>&1
```

## Конфигурация

### Основные параметры

#### [database]
- `db_name` - имя основной базы данных
- `db_user` - пользователь базы данных
- `db_host` - хост PostgreSQL (по умолчанию localhost)
- `db_port` - порт PostgreSQL (по умолчанию 5432)
- `temp_db_prefix` - префикс для временных баз (по умолчанию temp_restore_)

#### [paths]
- `backup_storage_dir` - директория для хранения бэкапов
- `temp_dir` - директория для временных файлов
- `log_file` - путь к файлу лога

#### [remote]
- `remote_user` - пользователь удаленного сервера
- `remote_host` - хост удаленного сервера
- `remote_path` - путь к дампу на удаленном сервере
- `backup_tar` - имя файла архива

#### [tables]
- `incremental_tables` - таблицы для инкрементального обновления
  - Формат: `table_name:primary_key`
  - Пример: `group_employee_count:id,users_active:id,asterisk_cdr:id`
- `issues_table` - таблица для трансформации custom values (по умолчанию `issues`)

#### [retention]
- `max_backups` - максимальное количество хранимых бэкапов (по умолчанию 5)
- `cleanup_temp_db` - очищать ли временную базу после завершения (true/false)

#### [schedule]
- `target_day` - день недели для еженедельных задач (1-понедельник, 7-воскресенье)

## Логирование

Все операции логируются в файл, указанный в конфиге (по умолчанию `/workspace/logs/etl_pipeline.log`).

Лог содержит:
- Временные метки
- Уровень сообщения (INFO, WARNING, ERROR)
- Описание операций
- Количество найденных новых/измененных строк

Сообщения "already exists" автоматически фильтруются.

## Как работает поиск изменений

### Новые строки
Находятся через LEFT JOIN:
```sql
SELECT t.*
FROM temp_db.table t
LEFT JOIN main_db.table m ON t.id = m.id
WHERE m.id IS NULL
```

### Измененные строки
Находятся через INNER JOIN с сравнением полей:
```sql
SELECT t.*
FROM temp_db.table t
INNER JOIN main_db.table m ON t.id = m.id
WHERE t.field1 IS DISTINCT FROM m.field1
   OR t.field2 IS DISTINCT FROM m.field2
   ...
```

### Применение изменений (UPSERT)
```sql
INSERT INTO main_db.table (columns...)
SELECT columns FROM temp_import
ON CONFLICT (id) 
DO UPDATE SET field1 = EXCLUDED.field1, field2 = EXCLUDED.field2, ...
```

## Отличия в обработке таблиц

### group_employee_count и users_active
- Выгружаются в CSV перед обновлением
- Загружаются обратно после применения изменений
- В целевой день недели добавляются новые записи с агрегированными данными

### asterisk_cdr
- Добавляется столбец project_id если отсутствует
- Устанавливается значение project_id = 3840 для всех записей

### issues (Трансформация custom values)
- Для каждого custom field типа Issue создается отдельная колонка в таблице issues
- Имя колонки формируется как `cf_{sanitized_name}` где sanitized_name - имя поля из custom_fields с заменой спецсимволов на подчеркивания
- Значения берутся из таблицы custom_values где:
  - `customized_id` = `issues.id`
  - `customized_type` = 'Issue'
  - `custom_field_id` соответствует полю в custom_fields
- Трансформация происходит после загрузки данных во временную базу и перед применением изменений

## Как работает трансформация custom values

### Шаг 1: Получение списка custom fields
```sql
SELECT id, name
FROM custom_fields
WHERE type = 1  -- Type 1 = Issue custom field
ORDER BY id;
```

### Шаг 2: Добавление колонок в issues
Для каждого custom field создается колонка:
```sql
ALTER TABLE issues 
ADD COLUMN IF NOT EXISTS cf_{field_name} TEXT;
```

### Шаг 3: Транспонирование и обновление
Для каждого custom field выполняется UPDATE:
```sql
UPDATE issues
SET cf_{field_name} = cv.value
FROM custom_values cv
WHERE cv.customized_id = issues.id
  AND cv.customized_type = 'Issue'
  AND cv.custom_field_id = {custom_field_id};
```

## Решение проблем

### Ошибка доступа к PostgreSQL
Убедитесь что у пользователя есть права на выполнение команд от имени postgres:
```bash
sudo -u postgres psql -c "SELECT 1;"
```

### Ошибка копирования с remote сервера
Проверьте SSH ключи:
```bash
ssh remote_user@remote_host
```

### Временная база не удаляется
Запустите с флагом --cleanup или удалите вручную:
```bash
sudo -u postgres psql -c "DROP DATABASE IF EXISTS temp_restore_*;"
```

### Мало места на диске
Очистите временные файлы:
```bash
rm -rf /tmp/pg_etl_temp/*
```

## Миграция со старого скрипта

1. Сделайте полный бэкап текущей базы:
```bash
pg_dump -U postgres db_name > backup_before_migration.sql
```

2. Запустите новый скрипт в режиме инициализации с последним дампом:
```bash
python3 etl_pipeline.py --init latest_dump.tar.gz
```

3. Проверьте что данные корректно загружены

4. Обновите crontab для использования нового скрипта

5. Оставьте старый скрипт как резервный вариант на первую неделю

## Лицензия

Внутренний инструмент компании.
