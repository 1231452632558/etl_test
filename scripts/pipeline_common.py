#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Общие компоненты ETL pipeline для монолитной и task-ориентированной версий.
"""

from __future__ import annotations

import configparser
import csv
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


def _validate_identifier(identifier: str, kind: str = "identifier") -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError(f"Некорректный {kind}: {identifier}")
    return identifier


def _quote_identifier(identifier: str) -> str:
    _validate_identifier(identifier)
    return f'"{identifier}"'


def _read_csv_header(csv_file: str, delimiter: str = ",") -> List[str]:
    with open(csv_file, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        return next(reader, [])


def _normalize_csv_column_name(name: str) -> str:
    return _validate_identifier(name.strip().lower(), "column")


@dataclass
class PipelineSettings:
    config_path: str
    parser: configparser.ConfigParser
    db_name: str
    db_user: str
    db_host: str
    db_port: str
    temp_db_prefix: str
    backup_storage_dir: str
    temp_dir: str
    log_file: str
    log_level: str
    max_backups: int
    cleanup_temp_db: bool
    target_day: int
    remote_user: str
    remote_host: str
    remote_path: str
    backup_tar: str
    issues_table: str
    snapshot_tables: Dict[str, Sequence[str] | str]
    manual_seed_files: Dict[str, str]
    asterisk_csv_file: str

    @classmethod
    def from_file(cls, config_path: str) -> "PipelineSettings":
        parser = configparser.ConfigParser()
        parser.read(config_path, encoding="utf-8")

        log_file = parser.get("paths", "log_file", fallback="/workspace/logs/etl_pipeline.log")
        log_dir = os.path.dirname(log_file)
        manual_seed_files = {
            "group_employee_count": parser.get(
                "paths",
                "group_employee_count_csv_file",
                fallback=os.path.join(log_dir, "group_employee_count_backup.csv"),
            ),
            "users_active": parser.get(
                "paths",
                "users_active_csv_file",
                fallback=os.path.join(log_dir, "users_active_backup.csv"),
            ),
        }

        snapshot_tables = cls._parse_snapshot_tables(parser)
        issues_table = parser.get("tables", "issues_table", fallback="issues").strip() or "issues"
        snapshot_tables.setdefault(issues_table, "id")

        return cls(
            config_path=config_path,
            parser=parser,
            db_name=parser.get("database", "db_name"),
            db_user=parser.get("database", "db_user"),
            db_host=parser.get("database", "db_host", fallback="localhost"),
            db_port=parser.get("database", "db_port", fallback="5432"),
            temp_db_prefix=parser.get("database", "temp_db_prefix", fallback="temp_restore_"),
            backup_storage_dir=parser.get("paths", "backup_storage_dir", fallback="/var/backups/postgres"),
            temp_dir=parser.get("paths", "temp_dir", fallback="/tmp/pg_etl_temp"),
            log_file=log_file,
            log_level=parser.get("logging", "log_level", fallback="INFO"),
            max_backups=parser.getint("retention", "max_backups", fallback=5),
            cleanup_temp_db=parser.getboolean("retention", "cleanup_temp_db", fallback=True),
            target_day=parser.getint("schedule", "target_day", fallback=1),
            remote_user=parser.get("remote", "remote_user", fallback=""),
            remote_host=parser.get("remote", "remote_host", fallback=""),
            remote_path=parser.get("remote", "remote_path", fallback=""),
            backup_tar=parser.get("remote", "backup_tar", fallback=""),
            issues_table=issues_table,
            snapshot_tables=snapshot_tables,
            manual_seed_files=manual_seed_files,
            asterisk_csv_file=parser.get(
                "paths",
                "asterisk_csv_file",
                fallback=os.path.join(log_dir, "asterisk_cdr.csv"),
            ),
        )

    @staticmethod
    def _parse_snapshot_tables(parser: configparser.ConfigParser) -> Dict[str, Sequence[str] | str]:
        tables_config = parser.get("tables", "incremental_tables", fallback="")
        tables: Dict[str, Sequence[str] | str] = {}
        if not tables_config:
            return tables

        for item in tables_config.split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            table_name, pk = item.split(":", 1)
            pk_columns = [col.strip() for col in pk.split("+") if col.strip()]
            tables[table_name.strip()] = pk_columns if len(pk_columns) > 1 else pk_columns[0]
        return tables


class ETLLogger:
    """Простой файловый логгер без внешних зависимостей."""

    def __init__(self, log_file: str, log_level: str = "INFO"):
        self.log_file = log_file
        self.log_level = log_level.upper()
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        self._rotate_log_file()

    def _rotate_log_file(self) -> None:
        try:
            if os.path.exists(self.log_file) and os.path.getsize(self.log_file) > 10 * 1024 * 1024:
                backup_file = f"{self.log_file}.old"
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                os.rename(self.log_file, backup_file)
        except OSError:
            pass

    def _should_log(self, message: str) -> bool:
        return "already exists" not in message.lower()

    def log(self, message: str, level: str = "INFO") -> None:
        if level == "DEBUG" and self.log_level != "DEBUG":
            return
        if not self._should_log(message):
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} - {level} - {message}"
        print(line)
        try:
            with open(self.log_file, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError:
            pass

    def info(self, message: str) -> None:
        self.log(message, "INFO")

    def warning(self, message: str) -> None:
        self.log(message, "WARNING")

    def error(self, message: str) -> None:
        self.log(message, "ERROR")

    def debug(self, message: str) -> None:
        self.log(message, "DEBUG")


class DatabaseOperations:
    """Операции с PostgreSQL для инкрементального nightly pipeline."""

    def __init__(self, settings: PipelineSettings, logger: ETLLogger):
        self.settings = settings
        self.logger = logger

    def _psql_base_cmd(self, db_name: str) -> List[str]:
        return [
            "sudo",
            "-u",
            "postgres",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self.settings.db_host,
            "-p",
            self.settings.db_port,
            "-d",
            db_name,
        ]

    def _run_psql(
        self,
        command: str,
        db_name: Optional[str] = None,
        capture_output: bool = False,
        ignore_errors: bool = False,
        tuples_only: bool = False,
    ) -> bool | str:
        target_db = db_name or self.settings.db_name
        cmd = self._psql_base_cmd(target_db)
        if tuples_only:
            cmd.extend(["-t", "-A", "-F", "\t"])
        cmd.extend(["-c", command])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            self.logger.error(f"Команда psql превысила лимит времени: {command[:120]}")
            return "" if capture_output else False
        except Exception as exc:
            self.logger.error(f"Ошибка запуска psql: {exc}")
            return "" if capture_output else False

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            if stderr and not ignore_errors:
                self.logger.error(f"PSQL error: {stderr}")
            elif stderr:
                self.logger.warning(f"PSQL warning: {stderr}")
            return "" if capture_output else False

        if capture_output:
            return result.stdout
        return True

    def run_scalar(self, query: str, db_name: Optional[str] = None) -> str:
        result = self._run_psql(query, db_name=db_name, capture_output=True, tuples_only=True)
        if not result:
            return ""
        return str(result).strip().splitlines()[0].strip() if str(result).strip() else ""

    def run_rows(self, query: str, db_name: Optional[str] = None) -> List[List[str]]:
        result = self._run_psql(query, db_name=db_name, capture_output=True, tuples_only=True)
        if not result:
            return []
        rows: List[List[str]] = []
        for line in str(result).splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append([part.strip() for part in line.split("\t")])
        return rows

    def _run_psql_file(self, sql_file: str, db_name: Optional[str] = None) -> bool:
        target_db = db_name or self.settings.db_name
        cmd = self._psql_base_cmd(target_db) + ["-f", sql_file]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        except subprocess.TimeoutExpired:
            self.logger.error(f"SQL file execution timed out: {sql_file}")
            return False
        except Exception as exc:
            self.logger.error(f"Error executing SQL file: {exc}")
            return False

        if result.returncode != 0:
            stderr = "\n".join(
                line for line in result.stderr.splitlines() if "already exists" not in line.lower()
            ).strip()
            if stderr:
                self.logger.error(f"SQL file error: {stderr}")
                return False
        return True

    def terminate_connections(self, db_name: str) -> None:
        self.logger.info(f"Завершение соединений с базой {db_name}...")
        command = f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{db_name}'
              AND pid <> pg_backend_pid();
        """
        self._run_psql(command, db_name="postgres", ignore_errors=True)

    def database_exists(self, db_name: str) -> bool:
        query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';"
        return self.run_scalar(query, db_name="postgres") == "1"

    def create_database(self, db_name: str, recreate: bool = False) -> bool:
        if recreate and self.database_exists(db_name):
            self.drop_database(db_name)
        if self.database_exists(db_name):
            self.logger.info(f"База {db_name} уже существует")
            return True

        command = f'CREATE DATABASE "{db_name}" OWNER "{self.settings.db_user}";'
        if self._run_psql(command, db_name="postgres"):
            self.logger.info(f"База {db_name} успешно создана")
            return True
        return False

    def drop_database(self, db_name: str) -> bool:
        self.terminate_connections(db_name)
        return bool(
            self._run_psql(
                f'DROP DATABASE IF EXISTS "{db_name}";',
                db_name="postgres",
                ignore_errors=True,
            )
        )

    def restore_dump(self, db_name: str, sql_files: Sequence[str]) -> bool:
        self.logger.info(f"Восстановление дампа в базу {db_name}...")
        for sql_file in sql_files:
            if not os.path.isfile(sql_file):
                self.logger.warning(f"Файл не найден: {sql_file}")
                continue
            self.logger.info(f"Выполняю файл: {os.path.basename(sql_file)}")
            if not self._run_psql_file(sql_file, db_name=db_name):
                return False
        return True

    def get_table_columns(self, table_name: str, db_name: str) -> List[str]:
        table_name = _validate_identifier(table_name, "table_name")
        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
        return [row[0] for row in self.run_rows(query, db_name=db_name) if row]

    def table_exists(self, table_name: str, db_name: str) -> bool:
        table_name = _validate_identifier(table_name, "table_name")
        query = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = '{table_name}';
        """
        return self.run_scalar(query, db_name=db_name) == "1"

    def count_rows(self, table_name: str, db_name: str) -> int:
        table_name = _validate_identifier(table_name, "table_name")
        value = self.run_scalar(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)};", db_name=db_name)
        return int(value or 0)

    def export_query_to_csv(self, query: str, csv_file: str, db_name: str) -> int:
        count_query = f"SELECT COUNT(*) FROM ({query}) AS subq;"
        count = int(self.run_scalar(count_query, db_name=db_name) or 0)
        if count == 0:
            return 0

        export_cmd = self._psql_base_cmd(db_name) + [
            "-c",
            f"\\copy ({query}) TO STDOUT WITH (FORMAT CSV, HEADER true)",
        ]
        with open(csv_file, "w", encoding="utf-8") as handle:
            result = subprocess.run(export_cmd, stdout=handle, stderr=subprocess.PIPE, text=True, timeout=1800)
        if result.returncode != 0:
            self.logger.error(f"Export error: {result.stderr.strip()}")
            return 0
        return count

    def export_table_to_csv(self, table_name: str, csv_file: str, db_name: str) -> int:
        table_name = _validate_identifier(table_name, "table_name")
        query = f"SELECT * FROM {_quote_identifier(table_name)}"
        return self.export_query_to_csv(query, csv_file, db_name)

    def upsert_from_csv(self, table_name: str, primary_key: Sequence[str] | str, csv_file: str, db_name: str) -> bool:
        if not os.path.exists(csv_file):
            self.logger.warning(f"CSV файл не найден: {csv_file}")
            return False

        table_name = _validate_identifier(table_name, "table_name")
        pk_cols = list(primary_key) if isinstance(primary_key, (list, tuple)) else [primary_key]
        pk_cols = [_validate_identifier(pk, "primary_key") for pk in pk_cols]

        csv_columns = [_validate_identifier(col, "column") for col in _read_csv_header(csv_file)]
        table_columns = set(self.get_table_columns(table_name, db_name))
        columns = [col for col in csv_columns if col in table_columns]
        if not columns:
            self.logger.error(f"В CSV {csv_file} нет колонок, совпадающих с {table_name}")
            return False

        quoted_columns = ", ".join(_quote_identifier(col) for col in columns)
        update_columns = [col for col in columns if col not in pk_cols]
        conflict_columns = ", ".join(_quote_identifier(pk) for pk in pk_cols)
        update_clause = ", ".join(
            f'{_quote_identifier(col)} = EXCLUDED.{_quote_identifier(col)}' for col in update_columns
        )

        command = f"""
            CREATE TEMP TABLE temp_upsert AS
            SELECT {quoted_columns}
            FROM {_quote_identifier(table_name)}
            LIMIT 0;

            \\copy temp_upsert ({quoted_columns}) FROM '{csv_file}'
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"', ENCODING 'UTF8');

            INSERT INTO {_quote_identifier(table_name)} ({quoted_columns})
            SELECT {quoted_columns}
            FROM temp_upsert
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_clause};

            DROP TABLE temp_upsert;
        """
        return bool(self._run_psql(command, db_name=db_name))

    def create_required_tables(self, db_name: str) -> None:
        statements = [
            """
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
            """,
            """
            CREATE TABLE IF NOT EXISTS group_employee_count (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL,
                group_name VARCHAR(255) NOT NULL,
                snapshot_date DATE NOT NULL,
                user_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (group_id, snapshot_date)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS users_active (
                id SERIAL PRIMARY KEY,
                snapshot_date DATE NOT NULL,
                user_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (snapshot_date)
            );
            """,
        ]
        for statement in statements:
            self._run_psql(statement, db_name=db_name, ignore_errors=True)

    def add_project_id_column(self, db_name: str) -> None:
        command = """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'asterisk_cdr'
                ) THEN
                    ALTER TABLE asterisk_cdr ADD COLUMN IF NOT EXISTS project_id INTEGER;
                    UPDATE asterisk_cdr
                    SET project_id = 3840
                    WHERE project_id IS NULL;
                END IF;
            END $$;
        """
        self._run_psql(command, db_name=db_name, ignore_errors=True)

    def grant_privileges(self, db_name: str) -> None:
        commands = [
            f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{self.settings.db_user}";',
            f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{self.settings.db_user}";',
            f'GRANT USAGE ON SCHEMA public TO "{self.settings.db_user}";',
        ]
        for command in commands:
            self._run_psql(command, db_name=db_name, ignore_errors=True)

    def import_csv_if_table_empty(self, table_name: str, csv_file: str, db_name: str) -> bool:
        return self.import_csv_to_table_if_empty(table_name, csv_file, db_name, delimiter=",")

    def import_csv_to_table_if_empty(self, table_name: str, csv_file: str, db_name: str, delimiter: str = ",") -> bool:
        if not os.path.exists(csv_file):
            return False
        if not self.table_exists(table_name, db_name):
            return False
        if self.count_rows(table_name, db_name) > 0:
            return False

        table_name = _validate_identifier(table_name, "table_name")
        table_columns = self.get_table_columns(table_name, db_name)
        table_columns_by_lower = {column.lower(): column for column in table_columns}
        csv_columns = [_normalize_csv_column_name(col) for col in _read_csv_header(csv_file, delimiter=delimiter)]
        mapped_columns = []
        for csv_column in csv_columns:
            matched_column = table_columns_by_lower.get(csv_column.lower())
            if not matched_column:
                self.logger.warning(
                    f"Колонка {csv_column} из {csv_file} отсутствует в таблице {table_name}, импорт пропущен"
                )
                return False
            mapped_columns.append(matched_column)

        quoted_columns = ", ".join(_quote_identifier(col) for col in mapped_columns)
        command = (
            f"\\copy {_quote_identifier(table_name)} ({quoted_columns}) "
            f"FROM '{csv_file}' "
            f"WITH (FORMAT csv, HEADER true, DELIMITER '{delimiter}', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');"
        )
        return bool(self._run_psql(command, db_name=db_name, ignore_errors=True))

    def seed_manual_tables_if_needed(self, db_name: str, seed_files: Dict[str, str]) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for table_name, csv_file in seed_files.items():
            imported = self.import_csv_if_table_empty(table_name, csv_file, db_name)
            results[table_name] = imported
            if imported:
                self.logger.info(f"Импортирован seed для {table_name}: {csv_file}")
        return results

    def seed_asterisk_cdr_if_needed(self, db_name: str, csv_file: Optional[str] = None) -> bool:
        csv_file = csv_file or self.settings.asterisk_csv_file
        imported = self.import_csv_to_table_if_empty("asterisk_cdr", csv_file, db_name, delimiter=";")
        if imported:
            self.logger.info(f"Импортирован seed для asterisk_cdr: {csv_file}")
        return imported

    def list_indexes(self, table_name: str, db_name: str) -> List[str]:
        table_name = _validate_identifier(table_name, "table_name")
        query = f"""
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = '{table_name}'
            ORDER BY indexname;
        """
        return [row[0] for row in self.run_rows(query, db_name=db_name) if row]

    def _sanitize_custom_column_name(self, name: str, field_id: str, used_names: set[str]) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
        generic_names = {"field", "custom_field", "customfield", "custom_value", "customvalue", "value"}
        if not sanitized or sanitized in generic_names:
            base = f"cf_{field_id}"
        else:
            base = f"cf_{sanitized[:45]}"
        candidate = base
        if candidate in used_names:
            candidate = f"{base}_{field_id}"
        used_names.add(candidate)
        return candidate

    def transform_custom_values(self, db_name: str, issues_table: str = "issues") -> Dict[str, object]:
        issues_table = _validate_identifier(issues_table, "issues_table")
        required_tables = [issues_table, "custom_values", "custom_fields"]
        if not all(self.table_exists(table_name, db_name) for table_name in required_tables):
            self.logger.info("Таблицы для custom transform отсутствуют, шаг пропущен")
            return {
                "processed_count": 0,
                "custom_fields_count": 0,
                "columns_added": [],
                "index_count": 0,
            }

        custom_fields = self.run_rows(
            """
            SELECT DISTINCT cf.id, cf.name
            FROM custom_fields cf
            JOIN custom_values cv
              ON cv.custom_field_id = cf.id
            WHERE cv.customized_type = 'Issue'
            ORDER BY cf.id;
            """,
            db_name=db_name,
        )
        if not custom_fields:
            return {
                "processed_count": 0,
                "custom_fields_count": 0,
                "columns_added": [],
                "index_count": len(self.list_indexes("custom_values", db_name)),
            }

        existing_columns = set(self.get_table_columns(issues_table, db_name))
        used_names = set(existing_columns)
        field_mappings: List[Tuple[str, str, str]] = []
        added_columns: List[str] = []

        for field_id, field_name in custom_fields:
            column_name = self._sanitize_custom_column_name(field_name, field_id, used_names)
            field_mappings.append((field_id, field_name, column_name))
            if column_name not in existing_columns:
                alter = f"ALTER TABLE {_quote_identifier(issues_table)} ADD COLUMN IF NOT EXISTS {_quote_identifier(column_name)} TEXT;"
                if self._run_psql(alter, db_name=db_name, ignore_errors=True):
                    added_columns.append(column_name)
                    existing_columns.add(column_name)

        set_clauses = ",\n                ".join(
            f'{_quote_identifier(column_name)} = src.{_quote_identifier(column_name)}'
            for _, _, column_name in field_mappings
        )
        select_clauses = ",\n                    ".join(
            (
                "STRING_AGG(DISTINCT NULLIF(cv.value, ''), ', ' ORDER BY NULLIF(cv.value, '')) "
                f"FILTER (WHERE cv.custom_field_id = {int(field_id)}) AS {_quote_identifier(column_name)}"
            )
            for field_id, _, column_name in field_mappings
        )

        update_query = f"""
            WITH aggregated AS (
                SELECT
                    cv.customized_id AS issue_id,
                    {select_clauses}
                FROM custom_values cv
                WHERE cv.customized_type = 'Issue'
                GROUP BY cv.customized_id
            )
            UPDATE {_quote_identifier(issues_table)} AS issues
            SET
                {set_clauses}
            FROM aggregated AS src
            WHERE src.issue_id = issues.id;
        """
        self._run_psql(update_query, db_name=db_name)

        processed_count = int(
            self.run_scalar(
                """
                SELECT COUNT(DISTINCT customized_id)
                FROM custom_values
                WHERE customized_type = 'Issue';
                """,
                db_name=db_name,
            )
            or 0
        )
        indexes = self.list_indexes("custom_values", db_name)
        return {
            "processed_count": processed_count,
            "custom_fields_count": len(field_mappings),
            "columns_added": added_columns,
            "index_count": len(indexes),
        }

    def add_weekly_records(self, db_name: str, target_day: int) -> Dict[str, int | bool]:
        today = date.today()
        current_day = today.isocalendar()[2]
        if current_day != target_day:
            return {"skipped": True, "current_day": current_day, "target_day": target_day}

        group_query = """
            WITH new_rows AS (
                SELECT
                    g.id AS group_id,
                    g.lastname AS group_name,
                    COUNT(DISTINCT u.id) AS user_count
                FROM users u
                JOIN groups_users gu ON u.id = gu.user_id
                JOIN users g ON gu.group_id = g.id
                WHERE u.status = 1
                  AND g.lastname ILIKE 'masked'
                GROUP BY g.id, g.lastname
            )
            INSERT INTO group_employee_count (group_id, group_name, snapshot_date, user_count)
            SELECT group_id, group_name, CURRENT_DATE, user_count
            FROM new_rows
            ON CONFLICT (group_id, snapshot_date)
            DO UPDATE SET
                group_name = EXCLUDED.group_name,
                user_count = EXCLUDED.user_count;
        """
        users_query = """
            INSERT INTO users_active (snapshot_date, user_count)
            SELECT CURRENT_DATE, COUNT(DISTINCT u.id)
            FROM users u
            WHERE u.status = 1
            ON CONFLICT (snapshot_date)
            DO UPDATE SET user_count = EXCLUDED.user_count;
        """
        self._run_psql(group_query, db_name=db_name, ignore_errors=True)
        self._run_psql(users_query, db_name=db_name, ignore_errors=True)
        return {"skipped": False, "current_day": current_day, "target_day": target_day}


def copy_from_remote_or_local(settings: PipelineSettings, logger: ETLLogger, dump_file: str) -> Optional[str]:
    if settings.remote_user and settings.remote_host and settings.remote_path and settings.backup_tar:
        logger.info("Шаг 1: Копирование архива с удаленного сервера...")
        source = f"{settings.remote_user}@{settings.remote_host}:{settings.remote_path}{settings.backup_tar}"
        destination = f"{settings.backup_storage_dir}/"
        os.makedirs(settings.backup_storage_dir, exist_ok=True)
        result = subprocess.run(["scp", source, destination], capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            return os.path.join(destination, settings.backup_tar)
        logger.error(f"Ошибка копирования: {(result.stderr or '').strip()}")

    if os.path.exists(dump_file):
        return dump_file

    logger.error(f"Файл не найден: {dump_file}")
    return None


def rotate_backups(settings: PipelineSettings, logger: ETLLogger) -> None:
    logger.info("Шаг 0: Ротация старых бэкапов...")
    os.makedirs(settings.backup_storage_dir, exist_ok=True)
    old_dirs: List[Tuple[float, str]] = []
    for item in os.listdir(settings.backup_storage_dir):
        if not item.startswith("old_"):
            continue
        full_path = os.path.join(settings.backup_storage_dir, item)
        if os.path.isdir(full_path):
            old_dirs.append((os.path.getmtime(full_path), full_path))

    old_dirs.sort(reverse=True)
    if len(old_dirs) < settings.max_backups:
        return

    for _, dir_path in old_dirs[settings.max_backups - 1 :]:
        shutil.rmtree(dir_path, ignore_errors=True)


def extract_dump(dump_file: str, temp_dir: str, logger: ETLLogger) -> Tuple[Optional[str], List[str]]:
    if dump_file.endswith(".sql"):
        return None, [dump_file]

    extract_dir = os.path.join(temp_dir, f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(extract_dir, exist_ok=True)
    try:
        with tarfile.open(dump_file, "r:gz") as archive:
            archive.extractall(path=extract_dir)
    except Exception as exc:
        logger.error(f"Ошибка распаковки архива: {exc}")
        return None, []

    sql_files: List[str] = []
    for root, _, files in os.walk(extract_dir):
        for file_name in files:
            if file_name.endswith(".sql"):
                sql_files.append(os.path.join(root, file_name))
    sql_files.sort()
    return extract_dir, sql_files


def build_snapshot_exports(
    db_ops: DatabaseOperations,
    temp_db: str,
    snapshot_tables: Dict[str, Sequence[str] | str],
    export_dir: str,
) -> List[Dict[str, object]]:
    os.makedirs(export_dir, exist_ok=True)
    modifications: List[Dict[str, object]] = []
    for table_name, primary_key in snapshot_tables.items():
        if not db_ops.table_exists(table_name, temp_db):
            continue
        csv_file = os.path.join(export_dir, f"{table_name}.csv")
        row_count = db_ops.export_table_to_csv(table_name, csv_file, temp_db)
        if row_count == 0 and not os.path.exists(csv_file):
            continue
        modifications.append(
            {
                "change_type": "snapshot",
                "table_name": table_name,
                "primary_key": primary_key,
                "csv_file": csv_file,
                "row_count": row_count,
            }
        )
    return modifications
