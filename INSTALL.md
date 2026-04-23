# Инструкция по установке и настройке

## Предварительные требования

Для работы ETL pipeline необходимо установить PostgreSQL 14 на Ubuntu 22.04.

## Установка PostgreSQL 14

### Вариант 1: Установка из репозитория Ubuntu (если доступен)

```bash
sudo apt update
sudo apt install -y postgresql-14 postgresql-client-14
```

### Вариант 2: Установка из официального репозитория PostgreSQL

```bash
# Добавляем репозиторий
sudo apt install -y wget gnupg lsb-release
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

# Устанавливаем
sudo apt update
sudo apt install -y postgresql-14 postgresql-client-14
```

## Настройка PostgreSQL

### 1. Запуск службы

```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
sudo systemctl status postgresql
```

### 2. Настройка аутентификации

Отредактируйте файл `/etc/postgresql/14/main/pg_hba.conf`:

```bash
sudo nano /etc/postgresql/14/main/pg_hba.conf
```

Найдите строки и измените на:

```
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust

# IPv6 local connections:
host    all             all             ::1/128                 trust

# Local connections:
local   all             postgres                                peer
local   all             all                                     peer
```

Перезапустите PostgreSQL:

```bash
sudo systemctl restart postgresql
```

### 3. Проверка подключения

```bash
psql -h localhost -U postgres -c "SELECT version();"
```

Или для проверки доступности:

```bash
pg_isready -h localhost -p 5432 -U postgres
```

## Настройка прав доступа для ETL pipeline

### Вариант A: Использование пользователя postgres (рекомендуется для тестирования)

ETL pipeline будет запускаться от root с использованием `sudo -u postgres`:

```bash
# Тестирование
sudo -u postgres psql -c "SELECT 1;"
```

### Вариант B: Создание отдельного пользователя БД

```bash
sudo -u postgres psql -c "CREATE USER etl_user WITH PASSWORD 'your_password';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE main_db TO etl_user;"
sudo -u postgres psql -c "ALTER DATABASE main_db OWNER TO etl_user;"
```

Затем обновите `/workspace/config/etl_config.ini`:

```ini
[database]
db_user = etl_user
```

## Проверка установки

Выполните тестовый скрипт:

```bash
cd /workspace
./run_tests.sh
```

## Настройка автоматического запуска (cron)

### 1. Откройте crontab

```bash
crontab -e
```

### 2. Добавьте задание

Для запуска каждый день в 2:00 ночи:

```
0 2 * * * cd /workspace && python3 /workspace/scripts/etl_pipeline.py --cleanup /path/to/nightly/dump.sql >> /workspace/logs/cron.log 2>&1
```

### 3. Проверьте cron

```bash
# Просмотр заданий
crontab -l

# Проверка логов cron
grep CRON /var/log/syslog
```

## Решение проблем

### Ошибка: "psycopg2 не найден"

ETL pipeline использует только стандартную библиотеку Python и утилиту командной строки `psql`. 
Убедитесь что postgresql-client установлен:

```bash
sudo apt install -y postgresql-client-14
```

### Ошибка: "connection refused"

Проверьте что PostgreSQL запущен:

```bash
sudo systemctl status postgresql
sudo systemctl start postgresql
```

### Ошибка: "authentication failed"

Проверьте настройки в `/etc/postgresql/14/main/pg_hba.conf` и перезапустите PostgreSQL.

### Ошибка: "database does not exist"

При первом запуске используйте флаг `--init`:

```bash
python3 /workspace/scripts/etl_pipeline.py --init /path/to/dump.sql
```

## Мониторинг

### Просмотр логов ETL

```bash
ls -lt /workspace/logs/
tail -f /workspace/logs/etl_*.log
```

### Просмотр состояния баз данных

```bash
sudo -u postgres psql -c "\l"  # Список баз данных
sudo -u postgres psql -d main_db -c "\dt"  # Таблицы в main_db
sudo -u postgres psql -d main_db -c "SELECT COUNT(*) FROM users;"  # Количество строк
```

### Проверка размера баз данных

```bash
sudo -u postgres psql -c "SELECT datname, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database ORDER BY pg_database_size(datname) DESC;"
```
