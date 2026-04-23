# ETL Pipeline для PostgreSQL

Python-скрипт для обработки nightly SQL-дампов PostgreSQL с инкрементальным обновлением данных.
Заменяет функционал bash-скрипта `dump_restore.sh` с улучшенной логикой обработки изменений.

## Особенности

### Что делает этот скрипт:
1. **Первый запуск (--init)**: Создает основную базу данных и загружает начальный дамп
2. **Ночная обработка**: 
   - Разворачивает новый дамп во временной базе данных
   - Сравнивает данные по первичным ключам между временной и основной базами
   - Находит **новые строки** (есть во временной, нет в основной)
   - Находит **измененные строки** (есть в обеих, но значения отличаются)
   - Применяет только изменения в основную базу через UPSERT
   - Удаляет временную базу после завершения

### Ключевые отличия от старого dump_restore.sh:
| Старый скрипт | Новый ETL Pipeline |
|--------------|-------------------|
| Полностью пересоздает базу каждую ночь | Сохраняет основную базу, добавляет только изменения |
| Теряет историю изменений | Сохраняет все данные, обновляет только измененные строки |
| Нет сравнения данных | Умное сравнение по первичным ключам |
| Простое восстановление из дампа | Инкрементальное обновление с UPSERT |

## Требования

- **ОС**: Ubuntu 22.04
- **PostgreSQL**: 14
- **Python**: 3.x (стандартная библиотека, без внешних зависимостей)
- **Доступ**: права на выполнение команд от имени пользователя postgres

## Установка

1. Скопируйте файлы в нужные директории:
```bash
# Основная структура
/workspace/
├── scripts/
│   └── etl_pipeline.py      # Основной скрипт
├── config/
│   └── etl_config.ini       # Конфигурация
├── logs/                     # Логи (создается автоматически)
└── dumps/                    # Тестовые дампы (опционально)
```

2. Отредактируйте конфигурационный файл `/workspace/config/etl_config.ini`:
```ini
[database]
db_name = ваше_имя_базы
db_user = ваш_пользователь
db_host = localhost
db_port = 5432

[remote]
remote_user = пользователь_remote
remote_host = host.remote.com
remote_path = /path/to/dumps/
backup_tar = backup.tar.gz

[tables]
# Таблицы для инкрементального обновления
incremental_tables = group_employee_count:id,users_active:id,asterisk_cdr:id
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
- Создает необходимые таблицы (asterisk_cdr, group_employee_count, users_active)
- Добавляет столбец project_id в asterisk_cdr
- Предоставляет права пользователю

### Ночная обработка (ежедневный запуск)

```bash
python3 /workspace/scripts/etl_pipeline.py /path/to/nightly_dump.tar.gz
```

Или с явной очисткой временной базы:
```bash
python3 /workspace/scripts/etl_pipeline.py --cleanup /path/to/nightly_dump.tar.gz
```

Этот режим:
- Копирует дамп с удаленного сервера (если настроено)
- Выполняет ротацию старых бэкапов
- Выгружает текущие данные в CSV (для безопасности)
- Создает временную базу и разворачивает дамп в ней
- **Находит новые строки** по каждой таблице из incremental_tables
- **Находит измененные строки** сравнивая значения
- Применяет изменения в основную базу через UPSERT
- Загружает обратно сохраненные CSV (совместимость со старым скриптом)
- Добавляет еженедельные записи (если сегодня целевой день)
- Предоставляет права пользователю
- Удаляет временную базу

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
