#!/usr/bin/env python3
"""
ETL Pipeline для обработки nightly SQL-дампов PostgreSQL 14
Работает на Ubuntu 22.04 без внешних зависимостей (использует только стандартную библиотеку)

Использование:
    # Первый запуск - инициализация основной базы
    python3 etl_pipeline.py --init /path/to/initial_dump.sql
    
    # Последующие запуски - обработка ночных дампов
    python3 etl_pipeline.py /path/to/nightly_dump.sql
    
    # С очисткой временной базы после завершения
    python3 etl_pipeline.py --cleanup /path/to/nightly_dump.sql
"""

import os
import sys
import subprocess
import logging
import argparse
import configparser
from datetime import datetime
from pathlib import Path


class ETLConfig:
    """Класс для управления конфигурацией ETL pipeline"""
    
    def __init__(self, config_path: str = "/workspace/config/etl_config.ini"):
        self.config = configparser.ConfigParser()
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
        self.config.read(config_path, encoding='utf-8')
        
        # Database settings
        self.main_db_name = self.config.get('database', 'main_db_name')
        self.temp_db_name = self.config.get('database', 'temp_db_name')
        self.db_user = self.config.get('database', 'db_user')
        self.db_host = self.config.get('database', 'db_host')
        self.db_port = self.config.get('database', 'db_port')
        
        # Paths
        self.dumps_dir = Path(self.config.get('paths', 'dumps_dir'))
        self.logs_dir = Path(self.config.get('paths', 'logs_dir'))
        self.temp_dir = Path(self.config.get('paths', 'temp_dir'))
        
        # Logging
        self.log_level = self.config.get('logging', 'log_level')
        self.log_format = self.config.get('logging', 'log_format')
        
        # ETL settings
        self.batch_size = self.config.getint('etl', 'batch_size')
        self.connection_timeout = self.config.getint('etl', 'connection_timeout')
        self.parallel_workers = self.config.getint('etl', 'parallel_workers')
    
    def get_connection_string(self, db_name: str) -> str:
        """Возвращает строку подключения к базе данных"""
        return f"postgresql://{self.db_user}@{self.db_host}:{self.db_port}/{db_name}"
    
    def get_psql_env(self) -> dict:
        """Возвращает переменные окружения для psql"""
        env = os.environ.copy()
        env['PGUSER'] = self.db_user
        env['PGHOST'] = self.db_host
        env['PGPORT'] = str(self.db_port)
        return env


class ETLLogger:
    """Класс для управления логированием"""
    
    def __init__(self, config: ETLConfig, log_file_prefix: str = "etl"):
        self.config = config
        
        # Создаем директорию для логов если не существует
        self.config.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Формируем имя файла лога с датой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.config.logs_dir / f"{log_file_prefix}_{timestamp}.log"
        
        # Настраиваем логгер
        self.logger = logging.getLogger("ETLPipeline")
        self.logger.setLevel(getattr(logging, config.log_level.upper()))
        
        # File handler
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(getattr(logging, config.log_level.upper()))
        fh.setFormatter(logging.Formatter(config.log_format))
        self.logger.addHandler(fh)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, config.log_level.upper()))
        ch.setFormatter(logging.Formatter(config.log_format))
        self.logger.addHandler(ch)
        
        self.log_file = log_file
    
    def get_logger(self) -> logging.Logger:
        return self.logger


class DatabaseManager:
    """Класс для управления операциями с базами данных PostgreSQL"""
    
    def __init__(self, config: ETLConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def run_psql_command(self, command: str, db_name: str = None, 
                         input_file: str = None, capture_output: bool = False) -> tuple:
        """
        Выполняет psql команду
        
        Args:
            command: SQL команда или мета-команда
            db_name: Имя базы данных (опционально)
            input_file: Путь к файлу для ввода (опционально)
            capture_output: Захватить вывод (по умолчанию False)
        
        Returns:
            tuple: (success: bool, output: str)
        """
        env = self.config.get_psql_env()
        
        cmd = ["psql", "-q", "-t"]
        
        if db_name:
            cmd.extend(["-d", db_name])
        
        if input_file:
            cmd.extend(["-f", input_file])
        else:
            cmd.extend(["-c", command])
        
        try:
            if capture_output:
                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=self.config.connection_timeout
                )
            else:
                result = subprocess.run(
                    cmd,
                    env=env,
                    timeout=self.config.connection_timeout
                )
            
            if result.returncode == 0:
                return True, result.stdout if capture_output else ""
            else:
                error_msg = result.stderr if capture_output else "Ошибка выполнения psql"
                self.logger.error(f"Ошибка psql: {error_msg}")
                return False, error_msg
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"Таймаут при выполнении команды: {command[:50]}...")
            return False, "Timeout expired"
        except Exception as e:
            self.logger.error(f"Исключение при выполнении psql: {str(e)}")
            return False, str(e)
    
    def database_exists(self, db_name: str) -> bool:
        """Проверяет существование базы данных"""
        query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';"
        success, output = self.run_psql_command(query, db_name="postgres", capture_output=True)
        return success and output.strip() == "1"
    
    def create_database(self, db_name: str) -> bool:
        """Создает базу данных"""
        if self.database_exists(db_name):
            self.logger.info(f"База данных {db_name} уже существует")
            return True
        
        self.logger.info(f"Создание базы данных: {db_name}")
        success, _ = self.run_psql_command(f"CREATE DATABASE {db_name};", db_name="postgres")
        return success
    
    def drop_database(self, db_name: str) -> bool:
        """Удаляет базу данных"""
        if not self.database_exists(db_name):
            self.logger.info(f"База данных {db_name} не существует")
            return True
        
        self.logger.info(f"Удаление базы данных: {db_name}")
        # Закрываем активные подключения перед удалением
        self.run_psql_command(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{db_name}' AND pid <> pg_backend_pid();",
            db_name="postgres"
        )
        success, _ = self.run_psql_command(f"DROP DATABASE {db_name};", db_name="postgres")
        return success
    
    def restore_dump(self, dump_file: str, db_name: str) -> bool:
        """
        Восстанавливает дамп в указанную базу данных
        
        Args:
            dump_file: Путь к файлу дампа
            db_name: Имя базы данных для восстановления
        
        Returns:
            bool: Успешность операции
        """
        if not os.path.exists(dump_file):
            self.logger.error(f"Файл дампа не найден: {dump_file}")
            return False
        
        self.logger.info(f"Восстановление дампа {dump_file} в базу {db_name}")
        
        # Сначала создаем базу если не существует
        if not self.create_database(db_name):
            return False
        
        # Очищаем базу перед восстановлением (на случай повторного использования)
        self.logger.info(f"Очистка базы данных {db_name} перед восстановлением")
        self.drop_all_tables(db_name)
        
        # Восстанавливаем дамп
        success, _ = self.run_psql_command("", db_name=db_name, input_file=dump_file)
        
        if success:
            self.logger.info(f"Дамп успешно восстановлен в {db_name}")
        else:
            self.logger.error(f"Ошибка при восстановлении дампа в {db_name}")
        
        return success
    
    def drop_all_tables(self, db_name: str) -> bool:
        """Удаляет все таблицы в базе данных"""
        query = """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
        """
        success, _ = self.run_psql_command(query, db_name=db_name)
        return success
    
    def get_table_list(self, db_name: str) -> list:
        """Получает список таблиц в базе данных"""
        query = """
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public' 
        ORDER BY tablename;
        """
        success, output = self.run_psql_command(query, db_name=db_name, capture_output=True)
        if success:
            tables = [line.strip() for line in output.strip().split('\n') if line.strip()]
            return tables
        return []
    
    def get_primary_keys(self, db_name: str, table_name: str) -> list:
        """Получает имена столбцов первичного ключа для таблицы"""
        query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{}'::regclass
        AND i.indisprimary;
        """.format(table_name)
        
        success, output = self.run_psql_command(query, db_name=db_name, capture_output=True)
        if success:
            keys = [line.strip() for line in output.strip().split('\n') if line.strip()]
            return keys
        return []
    
    def get_table_columns(self, db_name: str, table_name: str) -> list:
        """Получает список столбцов таблицы"""
        query = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{}'
        ORDER BY ordinal_position;
        """.format(table_name)
        
        success, output = self.run_psql_command(query, db_name=db_name, capture_output=True)
        if success:
            columns = [line.strip() for line in output.strip().split('\n') if line.strip()]
            return columns
        return []
    
    def find_new_and_changed_rows(self, source_db: str, target_db: str, 
                                   table_name: str, pk_columns: list) -> tuple:
        """
        Находит новые и измененные строки между исходной и целевой базами
        
        Args:
            source_db: Исходная база данных (временная с новым дампом)
            target_db: Целевая база данных (основная)
            table_name: Имя таблицы для сравнения
            pk_columns: Столбцы первичного ключа
        
        Returns:
            tuple: (new_rows_count, changed_rows_count)
        """
        pk_join = " AND ".join([f"s.{col} = t.{col}" for col in pk_columns])
        pk_select = ", ".join([f"s.{col}" for col in pk_columns])
        
        # Получаем список всех колонок кроме первичного ключа для сравнения
        all_columns = self.get_table_columns(source_db, table_name)
        non_pk_columns = [col for col in all_columns if col not in pk_columns]
        
        if not non_pk_columns:
            # Если нет колонок кроме PK, сравнивать нечего
            # Просто находим новые строки
            new_query = f"""
            SELECT COUNT(*) FROM {source_db}.public.{table_name} s
            LEFT JOIN {target_db}.public.{table_name} t ON {pk_join}
            WHERE t.{pk_columns[0]} IS NULL;
            """
            success, output = self.run_psql_command(new_query, db_name=source_db, capture_output=True)
            if success:
                count = int(output.strip()) if output.strip() else 0
                return count, 0
            return 0, 0
        
        # Находим новые строки (есть в source, нет в target)
        new_query = f"""
        SELECT COUNT(*) FROM {source_db}.public.{table_name} s
        LEFT JOIN {target_db}.public.{table_name} t ON {pk_join}
        WHERE t.{pk_columns[0]} IS NULL;
        """
        
        # Находим измененные строки (есть в обоих, но данные отличаются)
        change_conditions = " OR ".join([
            f"s.{col} IS DISTINCT FROM t.{col}" for col in non_pk_columns
        ])
        
        changed_query = f"""
        SELECT COUNT(*) FROM {source_db}.public.{table_name} s
        INNER JOIN {target_db}.public.{table_name} t ON {pk_join}
        WHERE {change_conditions};
        """
        
        # Выполняем запросы
        success, new_output = self.run_psql_command(new_query, db_name=source_db, capture_output=True)
        new_count = int(new_output.strip()) if success and new_output.strip() else 0
        
        success, changed_output = self.run_psql_command(changed_query, db_name=source_db, capture_output=True)
        changed_count = int(changed_output.strip()) if success and changed_output.strip() else 0
        
        return new_count, changed_count
    
    def sync_table(self, source_db: str, target_db: str, table_name: str, 
                   pk_columns: list) -> dict:
        """
        Синхронизирует таблицу между исходной и целевой базами данных
        
        Использует UPSERT (INSERT ... ON CONFLICT) для добавления новых
        и обновления измененных строк
        
        Args:
            source_db: Исходная база данных
            target_db: Целевая база данных
            table_name: Имя таблицы
            pk_columns: Столбцы первичного ключа
        
        Returns:
            dict: Статистика синхронизации
        """
        stats = {
            'inserted': 0,
            'updated': 0,
            'errors': 0
        }
        
        all_columns = self.get_table_columns(source_db, table_name)
        columns_str = ", ".join(all_columns)
        values_str = ", ".join([f"EXCLUDED.{col}" for col in all_columns])
        pk_conflict = ", ".join(pk_columns)
        
        # Строим условие для UPDATE - обновляем только если есть изменения
        non_pk_columns = [col for col in all_columns if col not in pk_columns]
        
        if non_pk_columns:
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in non_pk_columns])
            update_condition = " AND ".join([
                f"({target_db}.public.{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col})"
                for col in non_pk_columns
            ])
            
            sync_query = f"""
            INSERT INTO {target_db}.public.{table_name} ({columns_str})
            SELECT {columns_str} FROM {source_db}.public.{table_name}
            ON CONFLICT ({pk_conflict}) DO UPDATE SET {update_set}
            WHERE {update_condition};
            """
        else:
            # Если нет колонок кроме PK, просто игнорируем конфликты
            sync_query = f"""
            INSERT INTO {target_db}.public.{table_name} ({columns_str})
            SELECT {columns_str} FROM {source_db}.public.{table_name}
            ON CONFLICT ({pk_conflict}) DO NOTHING;
            """
        
        self.logger.info(f"Синхронизация таблицы {table_name}")
        success, output = self.run_psql_command(sync_query, db_name=source_db, capture_output=True)
        
        if success:
            # Получаем статистику из OUTPUT clause (если поддерживается)
            # Для простоты считаем что всё прошло успешно
            stats['inserted'] = -1  # Точное количество требует более сложной логики
            stats['updated'] = -1
            self.logger.info(f"Таблица {table_name} успешно синхронизирована")
        else:
            stats['errors'] = 1
            self.logger.error(f"Ошибка при синхронизации таблицы {table_name}")
        
        return stats


class ETLPipeline:
    """Основной класс ETL pipeline"""
    
    def __init__(self, config_path: str = "/workspace/config/etl_config.ini"):
        self.config = ETLConfig(config_path)
        self.logger_setup = ETLLogger(self.config)
        self.logger = self.logger_setup.get_logger()
        self.db_manager = DatabaseManager(self.config, self.logger)
        
        self.logger.info("=" * 60)
        self.logger.info("ETL Pipeline запущен")
        self.logger.info(f"Основная БД: {self.config.main_db_name}")
        self.logger.info(f"Временная БД: {self.config.temp_db_name}")
        self.logger.info("=" * 60)
    
    def initialize(self, dump_file: str) -> bool:
        """
        Первый запуск - создание основной базы и загрузка начального дампа
        
        Args:
            dump_file: Путь к начальному дампу
        
        Returns:
            bool: Успешность инициализации
        """
        self.logger.info("=== ИНИЦИАЛИЗАЦИЯ ===")
        self.logger.info(f"Начальный дамп: {dump_file}")
        
        # Создаем основную базу
        if not self.db_manager.create_database(self.config.main_db_name):
            self.logger.error("Не удалось создать основную базу данных")
            return False
        
        # Восстанавливаем начальный дамп
        if not self.db_manager.restore_dump(dump_file, self.config.main_db_name):
            self.logger.error("Не удалось восстановить начальный дамп")
            return False
        
        self.logger.info("Инициализация завершена успешно")
        return True
    
    def process_nightly_dump(self, dump_file: str, cleanup: bool = False) -> bool:
        """
        Обработка ночного дампа
        
        План:
        1. Развернуть новый дамп во временной базе
        2. Найти новые и измененные строки
        3. Применить изменения к основной базе
        4. Очистить временную базу (опционально)
        
        Args:
            dump_file: Путь к ночному дампу
            cleanup: Очистить временную базу после завершения
        
        Returns:
            bool: Успешность обработки
        """
        self.logger.info("=== ОБРАБОТКА НОЧНОГО ДАМПА ===")
        self.logger.info(f"Файл дампа: {dump_file}")
        
        # Шаг 1: Разворачиваем дамп во временной базе
        self.logger.info("Шаг 1: Развертывание дампа во временной базе")
        if not self.db_manager.restore_dump(dump_file, self.config.temp_db_name):
            self.logger.error("Не удалось развернуть дамп во временной базе")
            return False
        
        # Шаг 2: Получаем список таблиц во временной базе
        tables = self.db_manager.get_table_list(self.config.temp_db_name)
        if not tables:
            self.logger.warning("Нет таблиц для обработки")
            return True
        
        self.logger.info(f"Найдено таблиц: {len(tables)}")
        self.logger.info(f"Таблицы: {', '.join(tables)}")
        
        # Шаг 3: Для каждой таблицы находим изменения и синхронизируем
        total_stats = {
            'tables_processed': 0,
            'total_inserted': 0,
            'total_updated': 0,
            'errors': 0
        }
        
        for table_name in tables:
            self.logger.info(f"\n--- Обработка таблицы: {table_name} ---")
            
            # Получаем первичные ключи
            pk_columns = self.db_manager.get_primary_keys(
                self.config.temp_db_name, table_name
            )
            
            if not pk_columns:
                self.logger.warning(
                    f"Таблица {table_name} не имеет первичного ключа. Пропускаем."
                )
                continue
            
            self.logger.info(f"Первичный ключ: {', '.join(pk_columns)}")
            
            # Находим новые и измененные строки
            new_count, changed_count = self.db_manager.find_new_and_changed_rows(
                self.config.temp_db_name,
                self.config.main_db_name,
                table_name,
                pk_columns
            )
            
            self.logger.info(f"Найдено новых строк: {new_count}")
            self.logger.info(f"Найдено измененных строк: {changed_count}")
            
            # Синхронизируем если есть изменения
            if new_count > 0 or changed_count > 0:
                stats = self.db_manager.sync_table(
                    self.config.temp_db_name,
                    self.config.main_db_name,
                    table_name,
                    pk_columns
                )
                
                total_stats['tables_processed'] += 1
                if stats['errors'] > 0:
                    total_stats['errors'] += 1
            else:
                self.logger.info("Изменений не найдено")
        
        # Шаг 4: Очистка временной базы
        if cleanup:
            self.logger.info("\nОчистка временной базы данных")
            self.db_manager.drop_database(self.config.temp_db_name)
        
        # Вывод итоговой статистики
        self.logger.info("\n" + "=" * 60)
        self.logger.info("ИТОГИ ОБРАБОТКИ")
        self.logger.info("=" * 60)
        self.logger.info(f"Обработано таблиц: {total_stats['tables_processed']}")
        self.logger.info(f"Ошибок: {total_stats['errors']}")
        self.logger.info(f"Лог сохранен в: {self.logger_setup.log_file}")
        
        return total_stats['errors'] == 0


def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline для обработки nightly SQL-дампов PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Первый запуск - инициализация
  python3 etl_pipeline.py --init /path/to/initial_dump.sql
  
  # Обработка ночного дампа
  python3 etl_pipeline.py /path/to/nightly_dump.sql
  
  # Обработка с очисткой временной базы
  python3 etl_pipeline.py --cleanup /path/to/nightly_dump.sql
        """
    )
    
    parser.add_argument(
        "dump_file",
        nargs="?",
        help="Путь к SQL-дампу"
    )
    
    parser.add_argument(
        "--init",
        action="store_true",
        help="Режим инициализации (первый запуск)"
    )
    
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Очистить временную базу после обработки"
    )
    
    parser.add_argument(
        "--config",
        default="/workspace/config/etl_config.ini",
        help="Путь к конфигурационному файлу (по умолчанию: /workspace/config/etl_config.ini)"
    )
    
    args = parser.parse_args()
    
    # Проверка аргументов
    if not args.dump_file:
        parser.print_help()
        print("\nОшибка: Необходимо указать путь к SQL-дампу")
        sys.exit(1)
    
    if not os.path.exists(args.dump_file):
        print(f"Ошибка: Файл дампа не найден: {args.dump_file}")
        sys.exit(1)
    
    try:
        # Создаем и запускаем pipeline
        pipeline = ETLPipeline(config_path=args.config)
        
        if args.init:
            success = pipeline.initialize(args.dump_file)
        else:
            success = pipeline.process_nightly_dump(args.dump_file, cleanup=args.cleanup)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
