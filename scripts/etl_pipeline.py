#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline для обработки nightly SQL-дампов PostgreSQL
Заменяет функционал dump_restore.sh с поддержкой инкрементального обновления

Особенности:
- При первом запуске (--init) создает основную базу и загружает дамп
- При последующих запусках разворачивает дамп во временной базе
- Сравнивает данные по первичным ключам и находит новые/измененные строки
- Применяет только изменения в основную базу через UPSERT
- Поддерживает еженедельные задачи (добавление строк в group_employee_count и users_active)
- Работает с PostgreSQL 14 на Ubuntu 22.04 без внешних зависимостей

Использование:
    # Первый запуск (инициализация):
    python3 etl_pipeline.py --init /path/to/initial_dump.tar.gz
    
    # Ночная обработка:
    python3 etl_pipeline.py /path/to/nightly_dump.tar.gz
    
    # С очисткой временной базы после завершения:
    python3 etl_pipeline.py --cleanup /path/to/nightly_dump.tar.gz
"""

import os
import sys
import subprocess
import shutil
import tempfile
import tarfile
import configparser
import argparse
from datetime import datetime, date
from pathlib import Path


class ETLLogger:
    """Простой логгер без внешних зависимостей"""
    
    def __init__(self, log_file, log_level='INFO'):
        self.log_file = log_file
        self.log_level = log_level
        # Создаем директорию для лога если не существует
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Очищаем старые логи (оставляем последние 10 МБ)
        self._rotate_log_file()
    
    def _rotate_log_file(self):
        """Ротация логов"""
        try:
            if os.path.exists(self.log_file):
                file_size = os.path.getsize(self.log_file)
                if file_size > 10 * 1024 * 1024:  # 10 MB
                    backup_file = f"{self.log_file}.old"
                    if os.path.exists(backup_file):
                        os.remove(backup_file)
                    os.rename(self.log_file, backup_file)
        except Exception as e:
            pass
    
    def _should_log(self, message):
        """Фильтрация сообщений (игнорируем 'already exists')"""
        if 'already exists' in message.lower():
            return False
        return True
    
    def log(self, message, level='INFO'):
        """Запись в лог и вывод в stdout"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - {level} - {message}"
        
        if self._should_log(message):
            # Вывод в stdout
            print(log_message)
            # Запись в файл
            try:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(log_message + '\n')
            except Exception as e:
                print(f"Warning: Could not write to log file: {e}", file=sys.stderr)
    
    def info(self, message):
        self.log(message, 'INFO')
    
    def error(self, message):
        self.log(message, 'ERROR')
    
    def warning(self, message):
        self.log(message, 'WARNING')
    
    def debug(self, message):
        if self.log_level == 'DEBUG':
            self.log(message, 'DEBUG')


class DatabaseOperations:
    """Операции с базой данных PostgreSQL"""
    
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.db_name = config.get('database', 'db_name')
        self.db_user = config.get('database', 'db_user')
        self.db_host = config.get('database', 'db_host', fallback='localhost')
        self.db_port = config.get('database', 'db_port', fallback='5432')
        self.temp_db_prefix = config.get('database', 'temp_db_prefix', fallback='temp_restore_')
    
    def _run_psql(self, command, db_name=None, capture_output=False, ignore_errors=False):
        """Выполнение psql команды"""
        target_db = db_name if db_name else self.db_name
        
        cmd = [
            'sudo', '-u', 'postgres', 'psql',
            '-h', self.db_host,
            '-p', self.db_port,
            '-d', target_db,
            '-c', command
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=capture_output,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0 and not ignore_errors:
                stderr_filtered = '\n'.join(
                    line for line in result.stderr.split('\n')
                    if 'already exists' not in line.lower()
                )
                if stderr_filtered.strip():
                    self.logger.error(f"PSQL error: {stderr_filtered}")
                return False
            
            if capture_output:
                return result.stdout
            
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timed out: {command[:100]}")
            return False
        except Exception as e:
            self.logger.error(f"Error executing command: {e}")
            if not ignore_errors:
                return False
            return True
    
    def _run_psql_file(self, sql_file, db_name=None):
        """Выполнение SQL файла"""
        target_db = db_name if db_name else self.db_name
        
        cmd = [
            'sudo', '-u', 'postgres', 'psql',
            '-h', self.db_host,
            '-p', self.db_port,
            '-d', target_db,
            '-f', sql_file
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600
            )
            
            # Фильтруем сообщения "already exists"
            stderr_lines = [
                line for line in result.stderr.split('\n')
                if 'already exists' not in line.lower()
            ]
            stderr_filtered = '\n'.join(stderr_lines)
            
            if result.returncode != 0 and stderr_filtered.strip():
                self.logger.error(f"SQL file error: {stderr_filtered}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"SQL file execution timed out: {sql_file}")
            return False
        except Exception as e:
            self.logger.error(f"Error executing SQL file: {e}")
            return False
    
    def terminate_connections(self, db_name):
        """Завершение всех соединений с базой"""
        self.logger.info(f"Завершение всех соединений с базой {db_name}...")
        
        command = f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{db_name}'
            AND pid <> pg_backend_pid();
        """
        
        self._run_psql(command, db_name='postgres', ignore_errors=True)
        
        # Ждем немного чтобы соединения завершились
        import time
        time.sleep(2)
    
    def database_exists(self, db_name):
        """Проверка существования базы данных"""
        command = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';"
        result = self._run_psql(command, db_name='postgres', capture_output=True)
        return result and '1' in result
    
    def create_database(self, db_name):
        """Создание базы данных"""
        self.terminate_connections(db_name)
        
        self.logger.info(f"Удаление существующей базы {db_name} (если существует)...")
        self._run_psql(f'DROP DATABASE IF EXISTS "{db_name}";', db_name='postgres', ignore_errors=True)
        
        self.logger.info(f"Создание новой базы {db_name} с владельцем {self.db_user}...")
        command = f'CREATE DATABASE "{db_name}" OWNER "{self.db_user}";'
        
        if self._run_psql(command, db_name='postgres'):
            self.logger.info(f"База {db_name} успешно создана")
            return True
        else:
            self.logger.error(f"Ошибка при создании базы {db_name}")
            return False
    
    def drop_database(self, db_name):
        """Удаление базы данных"""
        self.terminate_connections(db_name)
        self.logger.info(f"Удаление базы {db_name}...")
        return self._run_psql(f'DROP DATABASE IF EXISTS "{db_name}";', db_name='postgres', ignore_errors=True)
    
    def restore_dump(self, db_name, sql_files):
        """Восстановление дампа из SQL файлов"""
        self.logger.info(f"Восстановление дампа в базу {db_name}...")
        
        for sql_file in sql_files:
            if os.path.isfile(sql_file):
                self.logger.info(f"Выполняю файл: {os.path.basename(sql_file)}")
                if self._run_psql_file(sql_file, db_name):
                    self.logger.info(f"Файл {os.path.basename(sql_file)} успешно выполнен")
                else:
                    self.logger.warning(f"Ошибка выполнения файла {os.path.basename(sql_file)}, продолжаем...")
            else:
                self.logger.warning(f"Файл не найден: {sql_file}")
        
        return True
    
    def get_table_columns(self, table_name, db_name):
        """Получение списка колонок таблицы"""
        command = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """
        result = self._run_psql(command, db_name=db_name, capture_output=True)
        if result:
            columns = [line.strip() for line in result.strip().split('\n') if line.strip()]
            # Убираем заголовок и разделители
            columns = [c for c in columns if c and not c.startswith('-') and c != 'column_name']
            return columns
        return []
    
    def get_primary_key(self, table_name, db_name):
        """Получение первичного ключа таблицы"""
        command = f"""
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{table_name}'::regclass
            AND    i.indisprimary;
        """
        result = self._run_psql(command, db_name=db_name, capture_output=True)
        if result:
            pk_columns = [line.strip() for line in result.strip().split('\n') if line.strip() and not line.startswith('-')]
            pk_columns = [c for c in pk_columns if c and c != 'attname']
            return pk_columns
        return []
    
    def export_table_to_csv(self, table_name, csv_file, db_name):
        """Экспорт таблицы в CSV"""
        self.logger.info(f"Выгрузка таблицы {table_name} из {db_name} в {csv_file}...")
        
        command = f"\\copy (SELECT * FROM {table_name}) TO STDOUT WITH (FORMAT CSV, HEADER true)"
        
        cmd = [
            'sudo', '-u', 'postgres', 'psql',
            '-h', self.db_host,
            '-p', self.db_port,
            '-d', db_name,
            '-c', command
        ]
        
        try:
            with open(csv_file, 'w', encoding='utf-8') as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=300
                )
                
                if result.returncode != 0:
                    stderr_filtered = '\n'.join(
                        line for line in result.stderr.split('\n')
                        if 'already exists' not in line.lower()
                    )
                    if stderr_filtered.strip():
                        self.logger.error(f"Export error: {stderr_filtered}")
                        return False
                
                if os.path.getsize(csv_file) > 0:
                    self.logger.info(f"Выгрузка успешна: {csv_file}")
                    return True
                else:
                    self.logger.warning(f"Файл пуст: {csv_file}")
                    return True  # Не считаем ошибкой если таблица пуста
                    
        except Exception as e:
            self.logger.error(f"Error exporting table: {e}")
            return False
    
    def import_table_from_csv(self, table_name, csv_file, db_name):
        """Импорт таблицы из CSV с игнорированием дубликатов"""
        if not os.path.exists(csv_file):
            self.logger.warning(f"CSV файл не найден: {csv_file}")
            return False
        
        self.logger.info(f"Загрузка данных из {csv_file} в таблицу {table_name}...")
        
        # Используем COPY с обработкой конфликтов
        command = f"""
            CREATE TEMP TABLE temp_import AS 
            SELECT * FROM {table_name} LIMIT 0;
            
            \\copy temp_import FROM '{csv_file}' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');
            
            INSERT INTO {table_name} 
            SELECT * FROM temp_import
            ON CONFLICT DO NOTHING;
            
            DROP TABLE temp_import;
        """
        
        return self._run_psql(command, db_name=db_name, ignore_errors=True)
    
    def find_new_and_changed_rows(self, table_name, primary_key, main_db, temp_db):
        """Поиск новых и измененных строк между временной и основной базой"""
        self.logger.info(f"Поиск новых и измененных строк в таблице {table_name}...")
        
        pk_cols = primary_key if isinstance(primary_key, list) else [primary_key]
        pk_cols_str = ', '.join(pk_cols)
        
        # Получаем все колонки кроме служебных
        all_columns = self.get_table_columns(table_name, temp_db)
        if not all_columns:
            self.logger.warning(f"Не удалось получить колонки таблицы {table_name}")
            return []
        
        # Исключаем автогенерируемые и служебные колонки из сравнения
        exclude_cols = {'id', 'created_at', 'updated_at', 'created_ts', 'updated_ts'}
        data_columns = [c for c in all_columns if c.lower() not in exclude_cols or c in pk_cols]
        data_columns_str = ', '.join(data_columns)
        
        # Находим новые строки (есть во временной, нет в основной)
        new_rows_query = f"""
            SELECT t.{data_columns_str}
            FROM {temp_db}.public.{table_name} t
            LEFT JOIN {main_db}.public.{table_name} m 
                ON {(' AND '.join([f't.{pk} = m.{pk}' for pk in pk_cols]))}
            WHERE m.{pk_cols[0]} IS NULL;
        """
        
        # Находим измененные строки (есть в обеих, но данные отличаются)
        # Сравниваем по всем колонкам кроме PK
        compare_columns = [c for c in data_columns if c not in pk_cols]
        if compare_columns:
            changed_conditions = ' OR '.join([
                f"(t.{col} IS DISTINCT FROM m.{col})" for col in compare_columns
            ])
            changed_rows_query = f"""
                SELECT t.{data_columns_str}
                FROM {temp_db}.public.{table_name} t
                INNER JOIN {main_db}.public.{table_name} m 
                    ON {(' AND '.join([f't.{pk} = m.{pk}' for pk in pk_cols]))}
                WHERE {changed_conditions};
            """
        else:
            changed_rows_query = None
        
        # Экспортируем новые строки во временный CSV
        temp_dir = tempfile.mkdtemp()
        new_rows_csv = os.path.join(temp_dir, f'{table_name}_new.csv')
        changed_rows_csv = os.path.join(temp_dir, f'{table_name}_changed.csv')
        
        modified_tables = []
        
        # Экспорт новых строк
        new_count = self._export_query_to_csv(new_rows_query, new_rows_csv, temp_db)
        if new_count > 0:
            self.logger.info(f"Найдено {new_count} новых строк в {table_name}")
            modified_tables.append(('new', table_name, primary_key, new_rows_csv))
        else:
            self.logger.info(f"Новых строк в {table_name} не найдено")
        
        # Экспорт измененных строк
        if changed_rows_query:
            changed_count = self._export_query_to_csv(changed_rows_query, changed_rows_csv, temp_db)
            if changed_count > 0:
                self.logger.info(f"Найдено {changed_count} измененных строк в {table_name}")
                modified_tables.append(('changed', table_name, primary_key, changed_rows_csv))
            else:
                self.logger.info(f"Измененных строк в {table_name} не найдено")
        
        # Очищаем временные файлы
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
        
        return modified_tables
    
    def _export_query_to_csv(self, query, csv_file, db_name):
        """Экспорт результата запроса в CSV и возврат количества строк"""
        cmd = [
            'sudo', '-u', 'postgres', 'psql',
            '-h', self.db_host,
            '-p', self.db_port,
            '-d', db_name,
            '-c', f"\\copy ({query}) TO STDOUT WITH (FORMAT CSV, HEADER true)",
            '-t', '-A'
        ]
        
        try:
            # Сначала получаем количество строк
            count_query = f"SELECT COUNT(*) FROM ({query}) AS subq;"
            count_result = self._run_psql(count_query, db_name=db_name, capture_output=True)
            count = int(count_result.strip()) if count_result and count_result.strip().isdigit() else 0
            
            if count > 0:
                # Экспортируем данные
                export_cmd = [
                    'sudo', '-u', 'postgres', 'psql',
                    '-h', self.db_host,
                    '-p', self.db_port,
                    '-d', db_name,
                    '-c', f"\\copy ({query}) TO STDOUT WITH (FORMAT CSV, HEADER true)"
                ]
                
                with open(csv_file, 'w', encoding='utf-8') as f:
                    result = subprocess.run(export_cmd, stdout=f, stderr=subprocess.PIPE, text=True, timeout=300)
                    if result.returncode != 0:
                        self.logger.warning(f"Export warning: {result.stderr}")
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error exporting query: {e}")
            return 0
    
    def upsert_from_csv(self, table_name, primary_key, csv_file, db_name):
        """UPSERT данных из CSV в таблицу"""
        if not os.path.exists(csv_file):
            self.logger.warning(f"CSV файл не найден: {csv_file}")
            return False
        
        self.logger.info(f"Выполнение UPSERT в таблицу {table_name} из {csv_file}...")
        
        pk_cols = primary_key if isinstance(primary_key, list) else [primary_key]
        pk_cols_str = ', '.join(pk_cols)
        
        # Получаем все колонки
        all_columns = self.get_table_columns(table_name, db_name)
        if not all_columns:
            self.logger.error(f"Не удалось получить колонки таблицы {table_name}")
            return False
        
        columns_str = ', '.join(all_columns)
        
        # Создаем временную таблицу и загружаем данные
        conflict_columns = ', '.join([f'{pk}' for pk in pk_cols])
        
        command = f"""
            CREATE TEMP TABLE temp_upsert AS 
            SELECT * FROM {table_name} LIMIT 0;
            
            \\copy temp_upsert FROM '{csv_file}' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');
            
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM temp_upsert
            ON CONFLICT ({conflict_columns}) 
            DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in all_columns if col not in pk_cols])};
            
            DROP TABLE temp_upsert;
        """
        
        return self._run_psql(command, db_name=db_name, ignore_errors=True)
    
    def grant_privileges(self, db_name):
        """Предоставление прав пользователю"""
        self.logger.info(f"Предоставление прав пользователю {self.db_user}...")
        
        commands = [
            f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{self.db_user}";',
            f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{self.db_user}";',
            f'GRANT USAGE ON SCHEMA public TO "{self.db_user}";'
        ]
        
        for command in commands:
            self._run_psql(command, db_name=db_name, ignore_errors=True)
    
    def add_project_id_column(self, db_name):
        """Добавление столбца project_id в asterisk_cdr"""
        self.logger.info("Добавление столбца project_id в таблицу asterisk_cdr...")
        
        # Добавляем колонку если не существует
        command = """
            ALTER TABLE asterisk_cdr 
            ADD COLUMN IF NOT EXISTS project_id INTEGER;
        """
        self._run_psql(command, db_name=db_name, ignore_errors=True)
        
        # Обновляем значения
        command = """
            UPDATE asterisk_cdr 
            SET project_id = 3840 
            WHERE project_id IS NULL;
        """
        self._run_psql(command, db_name=db_name, ignore_errors=True)
        
        self.logger.info("Столбец project_id добавлен и обновлен")
    
    def create_required_tables(self, db_name):
        """Создание необходимых таблиц если они не существуют"""
        self.logger.info("Создание необходимых таблиц...")
        
        tables_sql = """
        CREATE TABLE IF NOT EXISTS asterisk_cdr (
            id INTEGER PRIMARY KEY,
            calldate TIMESTAMP WITHOUT TIME ZONE,
            clid TEXT,
            src TEXT,
            dst TEXT,
            dcontext TEXT,
            channel TEXT,
            dstchannel TEXT,
            lastapp TEXT,
            lastdata TEXT,
            duration INTEGER,
            billsec INTEGER,
            disposition TEXT,
            amaflags TEXT,
            accountcode TEXT,
            uniqueid TEXT,
            userfield TEXT,
            cdr_start TIMESTAMP WITHOUT TIME ZONE,
            answer TIMESTAMP WITHOUT TIME ZONE,
            cdr_end TIMESTAMP WITHOUT TIME ZONE,
            linkedid TEXT,
            peeraccount TEXT,
            sequence TEXT
        );
        
        CREATE TABLE IF NOT EXISTS group_employee_count (
            id SERIAL PRIMARY KEY,
            group_id INTEGER NOT NULL,
            group_name VARCHAR(255) NOT NULL,
            snapshot_date DATE NOT NULL,
            user_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (group_id, snapshot_date)
        );
        
        CREATE TABLE IF NOT EXISTS users_active (
            id SERIAL PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            user_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (id, snapshot_date)
        );
        """
        
        # Разделяем на отдельные команды
        for statement in tables_sql.split(';'):
            statement = statement.strip()
            if statement:
                self._run_psql(statement, db_name=db_name, ignore_errors=True)
        
        self.logger.info("Таблицы созданы (если не существовали)")
    
    def add_weekly_records(self, db_name, target_day):
        """Добавление еженедельных записей (из оригинального скрипта)"""
        today = date.today()
        current_day = today.isocalendar()[2]  # 1-7 (понедельник-воскресенье)
        
        if current_day != target_day:
            self.logger.info(f"Сегодня не целевой день ({current_day} != {target_day}), пропускаем добавление недельных записей")
            return
        
        self.logger.info(f"Шаг 5: Еженедельный день ({target_day}). Добавление строк...")
        
        # Добавляем запись в group_employee_count
        command = """
        WITH max_id AS (
            SELECT COALESCE(MAX(id), 0) AS val FROM group_employee_count
        ),
        new_rows AS (
            SELECT
                m.val + ROW_NUMBER() OVER() AS new_id,
                g.id as gid, 
                g.lastname as gname, 
                COUNT(DISTINCT u.id) as u_count
            FROM users u, max_id m
            JOIN groups_users gu ON u.id = gu.user_id
            JOIN users g ON gu.group_id = g.id
            WHERE u.status = 1 and g.lastname ILIKE 'masked'
            GROUP BY gid, gname
        )
        INSERT INTO group_employee_count (id, group_id, group_name, snapshot_date, user_count)
        SELECT new_id, gid, gname, CURRENT_DATE, u_count
        FROM new_rows
        WHERE NOT EXISTS (
            SELECT 1 FROM group_employee_count gec 
            WHERE gec.group_id = new_rows.gid 
            AND gec.snapshot_date = CURRENT_DATE
        );
        """
        
        self._run_psql(command, db_name=db_name, ignore_errors=True)
        self.logger.info("Добавлена запись в group_employee_count")
        
        # Добавляем запись в users_active
        command = """
        INSERT INTO users_active (snapshot_date, user_count) 
        SELECT CURRENT_DATE, COUNT(DISTINCT u.id) 
        FROM users u 
        WHERE u.status = 1
        AND NOT EXISTS (
            SELECT 1 FROM users_active ua 
            WHERE ua.snapshot_date = CURRENT_DATE
        );
        """
        
        self._run_psql(command, db_name=db_name, ignore_errors=True)
        self.logger.info("Добавлена запись в users_active")


class ETLPipeline:
    """Основной класс ETL pipeline"""
    
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
        
        # Инициализация логгера
        log_file = self.config.get('paths', 'log_file', fallback='/workspace/logs/etl_pipeline.log')
        log_level = self.config.get('logging', 'log_level', fallback='INFO')
        self.logger = ETLLogger(log_file, log_level)
        
        # Инициализация операций с БД
        self.db_ops = DatabaseOperations(self.config, self.logger)
        
        # Пути
        self.backup_storage_dir = self.config.get('paths', 'backup_storage_dir', fallback='/var/backups/postgres')
        self.temp_dir = self.config.get('paths', 'temp_dir', fallback='/tmp/pg_etl_temp')
        
        # Настройки
        self.max_backups = self.config.getint('retention', 'max_backups', fallback=5)
        self.cleanup_temp_db = self.config.getboolean('retention', 'cleanup_temp_db', fallback=True)
        self.target_day = self.config.getint('schedule', 'target_day', fallback=1)
        
        # Таблицы для инкрементального обновления
        self.incremental_tables = self._parse_incremental_tables()
    
    def _parse_incremental_tables(self):
        """Парсинг конфигурации инкрементальных таблиц"""
        tables_config = self.config.get('tables', 'incremental_tables', fallback='')
        tables = {}
        
        if tables_config:
            for item in tables_config.split(','):
                item = item.strip()
                if ':' in item:
                    table_name, pk = item.split(':', 1)
                    # Поддержка составных ключей
                    pk_columns = [col.strip() for col in pk.split(',')]
                    tables[table_name.strip()] = pk_columns if len(pk_columns) > 1 else pk_columns[0]
        
        return tables
    
    def rotate_backups(self):
        """Ротация старых бэкапов (из оригинального скрипта)"""
        self.logger.info("Шаг 0: Ротация старых бэкапов...")
        
        backup_dir = self.backup_storage_dir
        os.makedirs(backup_dir, exist_ok=True)
        
        # Находим старые копии
        old_dirs = []
        try:
            for item in os.listdir(backup_dir):
                if item.startswith('old_'):
                    full_path = os.path.join(backup_dir, item)
                    if os.path.isdir(full_path):
                        old_dirs.append((os.path.getmtime(full_path), full_path))
        except Exception as e:
            self.logger.warning(f"Ошибка при сканировании директории бэкапов: {e}")
            return
        
        # Сортируем по времени (новые первые)
        old_dirs.sort(reverse=True)
        
        # Удаляем старые если превышен лимит
        if len(old_dirs) >= self.max_backups:
            to_delete = old_dirs[self.max_backups - 1:]
            self.logger.info(f"Удаление {len(to_delete)} старых копий бэкапов...")
            for _, dir_path in to_delete:
                try:
                    shutil.rmtree(dir_path)
                    self.logger.info(f"Удалена старая копия: {dir_path}")
                except Exception as e:
                    self.logger.warning(f"Ошибка удаления {dir_path}: {e}")
        else:
            self.logger.info(f"Ротация не требуется, всего старых копий: {len(old_dirs)}")
    
    def copy_from_remote(self, tar_file):
        """Копирование файла с удаленного сервера"""
        remote_user = self.config.get('remote', 'remote_user', fallback='')
        remote_host = self.config.get('remote', 'remote_host', fallback='')
        remote_path = self.config.get('remote', 'remote_path', fallback='')
        backup_tar = self.config.get('remote', 'backup_tar', fallback='')
        
        if remote_user and remote_host and remote_path:
            self.logger.info(f"Шаг 1: Копирование файлов с удаленного сервера...")
            self.logger.info(f"Источник: {remote_user}@{remote_host}:{remote_path}")
            self.logger.info(f"Назначение: {self.backup_storage_dir}")
            
            source = f"{remote_user}@{remote_host}:{remote_path}{backup_tar}"
            dest = f"{self.backup_storage_dir}/"
            
            cmd = ['scp', source, dest]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0:
                    self.logger.info(f"Файл {backup_tar} успешно скопирован")
                    return os.path.join(dest, backup_tar)
                else:
                    self.logger.error(f"Ошибка копирования: {result.stderr}")
            except Exception as e:
                self.logger.error(f"Ошибка при копировании: {e}")
        
        # Если файл уже локальный или scp не нужен
        if os.path.exists(tar_file):
            return tar_file
        
        self.logger.error(f"Файл не найден: {tar_file}")
        return None
    
    def extract_tar(self, tar_file):
        """Распаковка tar.gz архива"""
        self.logger.info(f"Распаковка архива: {tar_file}")
        
        extract_dir = os.path.join(self.temp_dir, f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            with tarfile.open(tar_file, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            
            self.logger.info(f"Архив успешно распакован в {extract_dir}")
            
            # Находим все SQL файлы
            sql_files = []
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    if file.endswith('.sql'):
                        sql_files.append(os.path.join(root, file))
            
            if not sql_files:
                self.logger.warning("SQL файлы не найдены в архиве")
            
            return extract_dir, sql_files
            
        except Exception as e:
            self.logger.error(f"Ошибка распаковки архива: {e}")
            return None, []
    
    def download_data_to_csv(self, db_name):
        """Выгрузка данных в CSV перед сбросом (из оригинального скрипта)"""
        self.logger.info("=== Шаг 1: Выгрузка данных в CSV (перед сбросом) ===")
        
        csv_dir = os.path.dirname(self.config.get('paths', 'log_file'))
        csv_file1 = os.path.join(csv_dir, 'group_employee_count_backup.csv')
        csv_file2 = os.path.join(csv_dir, 'users_active_backup.csv')
        
        tables_to_export = ['group_employee_count', 'users_active']
        
        for table in tables_to_export:
            # Проверяем есть ли данные
            count_query = f"SELECT COUNT(*) FROM {table};"
            count_result = self.db_ops._run_psql(count_query, db_name=db_name, capture_output=True)
            
            if count_result:
                count = count_result.strip()
                self.logger.info(f"В БД найдено записей в {table}: {count}")
            
            csv_file = csv_file1 if table == 'group_employee_count' else csv_file2
            self.db_ops.export_table_to_csv(table, csv_file, db_name)
        
        return csv_file1, csv_file2
    
    def upload_data_from_csv(self, db_name, csv_files):
        """Загрузка данных из CSV (из оригинального скрипта)"""
        self.logger.info("Шаг 5: Загрузка данных из CSV...")
        
        table_files = [
            ('group_employee_count', csv_files[0]),
            ('users_active', csv_files[1])
        ]
        
        for table_name, csv_file in table_files:
            if csv_file and os.path.exists(csv_file):
                self.logger.info(f"Загрузка данных из {csv_file} в таблицу {table_name}...")
                self.db_ops.import_table_from_csv(table_name, csv_file, db_name)
                self.logger.info(f"Данные загружены из {csv_file}")
            else:
                self.logger.warning(f"Файл не найден ({csv_file}), загрузка пропущена")
    
    def run_init(self, dump_file):
        """Первичная инициализация базы данных"""
        self.logger.info("=== ЗАПУСК В РЕЖИМЕ ИНИЦИАЛИЗАЦИИ ===")
        
        # Копируем файл если нужно
        actual_file = self.copy_from_remote(dump_file)
        if not actual_file:
            return False
        
        # Распаковываем архив
        extract_dir, sql_files = self.extract_tar(actual_file)
        if not extract_dir:
            return False
        
        try:
            # Создаем основную базу
            main_db = self.config.get('database', 'db_name')
            if not self.db_ops.create_database(main_db):
                return False
            
            # Восстанавливаем дамп
            if not self.db_ops.restore_dump(main_db, sql_files):
                return False
            
            # Создаем необходимые таблицы
            self.db_ops.create_required_tables(main_db)
            
            # Добавляем project_id
            self.db_ops.add_project_id_column(main_db)
            
            # Предоставляем права
            self.db_ops.grant_privileges(main_db)
            
            self.logger.info("=== ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО ===")
            return True
            
        finally:
            # Очищаем временные файлы
            try:
                shutil.rmtree(extract_dir)
            except:
                pass
    
    def run_nightly(self, dump_file, cleanup=True):
        """Ночная обработка дампа с инкрементальным обновлением"""
        self.logger.info("=== ЗАПУСК НОЧНОЙ ОБРАБОТКИ ===")
        
        # Копируем файл если нужно
        actual_file = self.copy_from_remote(dump_file)
        if not actual_file:
            return False
        
        # Ротация бэкапов
        self.rotate_backups()
        
        # Выгрузка данных перед обновлением
        main_db = self.config.get('database', 'db_name')
        csv_files = self.download_data_to_csv(main_db)
        
        # Распаковываем архив
        extract_dir, sql_files = self.extract_tar(actual_file)
        if not extract_dir:
            return False
        
        # Создаем временную базу
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_db = f"{self.config.get('database', 'temp_db_prefix', fallback='temp_restore_')}{timestamp}"
        
        try:
            # Создаем временную базу
            if not self.db_ops.create_database(temp_db):
                return False
            
            # Восстанавливаем дамп во временную базу
            if not self.db_ops.restore_dump(temp_db, sql_files):
                return False
            
            # Создаем необходимые таблицы во временной базе
            self.db_ops.create_required_tables(temp_db)
            
            # Добавляем project_id во временной базе
            self.db_ops.add_project_id_column(temp_db)
            
            # Находим и применяем изменения для каждой таблицы
            for table_name, primary_key in self.incremental_tables.items():
                self.logger.info(f"Обработка таблицы {table_name}...")
                
                # Находим новые и измененные строки
                modified_tables = self.db_ops.find_new_and_changed_rows(
                    table_name, primary_key, main_db, temp_db
                )
                
                # Применяем изменения
                for change_type, tbl_name, pk, csv_file in modified_tables:
                    self.logger.info(f"Применение {change_type} строк в {tbl_name}...")
                    self.db_ops.upsert_from_csv(tbl_name, pk, csv_file, main_db)
            
            # Загружаем обратно сохраненные CSV (для совместимости со старым скриптом)
            self.upload_data_from_csv(main_db, csv_files)
            
            # Добавляем еженедельные записи если сегодня целевой день
            self.db_ops.add_weekly_records(main_db, self.target_day)
            
            # Предоставляем права
            self.db_ops.grant_privileges(main_db)
            
            self.logger.info("=== НОЧНАЯ ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО ===")
            return True
            
        finally:
            # Очищаем временную базу если требуется
            if cleanup:
                self.logger.info(f"Очистка временной базы {temp_db}...")
                self.db_ops.drop_database(temp_db)
            
            # Очищаем временные файлы
            try:
                shutil.rmtree(extract_dir)
            except:
                pass
    
    def run(self, dump_file, init_mode=False, cleanup=True):
        """Запуск pipeline"""
        self.logger.info(f"Дата запуска: {datetime.now()}")
        self.logger.info(f"Пользователь запуска: {os.getenv('USER', 'unknown')}")
        self.logger.info(f"Хост: {os.uname().nodename}")
        
        if init_mode:
            return self.run_init(dump_file)
        else:
            return self.run_nightly(dump_file, cleanup)


def main():
    parser = argparse.ArgumentParser(
        description='ETL Pipeline для обработки nightly SQL-дампов PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Первый запуск (инициализация):
  python3 etl_pipeline.py --init /path/to/initial_dump.tar.gz
  
  # Ночная обработка:
  python3 etl_pipeline.py /path/to/nightly_dump.tar.gz
  
  # С очисткой временной базы:
  python3 etl_pipeline.py --cleanup /path/to/nightly_dump.tar.gz
        """
    )
    
    parser.add_argument('dump_file', nargs='?', help='Путь к SQL-дампу (tar.gz)')
    parser.add_argument('--init', action='store_true', help='Режим инициализации (первый запуск)')
    parser.add_argument('--cleanup', action='store_true', help='Очистить временную базу после завершения')
    parser.add_argument('--config', default='/workspace/config/etl_config.ini', help='Путь к конфигурационному файлу')
    
    args = parser.parse_args()
    
    if not args.dump_file:
        parser.print_help()
        sys.exit(1)
    
    # Проверка существования конфига
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    
    # Создание и запуск pipeline
    pipeline = ETLPipeline(args.config)
    
    success = pipeline.run(args.dump_file, init_mode=args.init, cleanup=args.cleanup)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
