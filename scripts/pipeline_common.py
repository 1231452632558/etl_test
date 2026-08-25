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


MANUAL_TABLES = ("asterisk_cdr", "group_employee_count", "users_active")


def _validate_identifier(identifier: str, kind: str = "identifier") -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError(f"Некорректный {kind}: {identifier}")
    return identifier


def _quote_identifier(identifier: str) -> str:
    _validate_identifier(identifier)
    return f'"{identifier}"'


def _read_csv_header(csv_file: str, delimiter: str = ",") -> List[str]:
    with open(csv_file, "r", encoding="utf-8", newline="") as handle:
        header_line = handle.readline()
    if not header_line:
        return []
    return next(csv.reader([header_line], delimiter=delimiter), [])


def _normalize_csv_column_name(name: str) -> str:
    return _validate_identifier(name.strip().lower(), "column")


def _strip_csv_header(source_csv: str, delimiter: str = ",") -> str:
    fd, temp_csv = tempfile.mkstemp(prefix="pg_etl_upsert_", suffix=".csv")
    os.close(fd)
    os.chmod(temp_csv, 0o644)

    with open(source_csv, "rb") as source_handle, open(temp_csv, "wb") as target_handle:
        source_handle.readline()
        shutil.copyfileobj(source_handle, target_handle)

    return temp_csv


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
    old_backup_dir: str
    temp_dir: str
    log_file: str
    log_level: str
    max_backups: int
    cleanup_temp_db: bool
    weekly_enabled: bool
    weekly_days: Tuple[int, ...]
    weekly_group_name_pattern: str
    remote_user: str
    remote_host: str
    remote_path: str
    backup_tar: str
    issues_table: str
    projects_table: str
    auto_discover_tables: bool
    fail_on_unkeyed_tables: bool
    snapshot_excluded_tables: Tuple[str, ...]
    snapshot_replace_tables: Tuple[str, ...]
    snapshot_tables: Dict[str, Sequence[str] | str]
    manual_seed_files: Dict[str, str]
    asterisk_csv_file: str

    @classmethod
    def from_file(cls, config_path: str) -> "PipelineSettings":
        parser = configparser.ConfigParser(interpolation=None)
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
        configured_exclusions = cls._parse_identifier_list(
            parser.get("tables", "snapshot_excluded_tables", fallback="")
        )
        snapshot_excluded_tables = tuple(
            dict.fromkeys((*MANUAL_TABLES, *configured_exclusions))
        )
        snapshot_replace_tables = cls._parse_identifier_list(
            parser.get("tables", "snapshot_replace_tables", fallback="")
        )
        invalid_replace_tables = sorted(
            set(snapshot_replace_tables) & set(snapshot_excluded_tables)
        )
        if invalid_replace_tables:
            raise ValueError(
                "Таблицы нельзя одновременно исключать и заменять через snapshot: "
                f"{invalid_replace_tables}"
            )
        for excluded_table in snapshot_excluded_tables:
            snapshot_tables.pop(excluded_table, None)
        for replace_table in snapshot_replace_tables:
            snapshot_tables.pop(replace_table, None)
        issues_table = parser.get("tables", "issues_table", fallback="issues").strip() or "issues"
        projects_table = parser.get("tables", "projects_table", fallback="projects").strip() or "projects"
        snapshot_tables.setdefault(issues_table, "id")
        snapshot_tables.setdefault(projects_table, "id")
        target_day = parser.getint("schedule", "target_day", fallback=1)
        weekly_days = cls._parse_weekly_days(
            parser.get("schedule", "weekly_days", fallback=str(target_day))
        )

        return cls(
            config_path=config_path,
            parser=parser,
            db_name=parser.get("database", "db_name"),
            db_user=parser.get("database", "db_user"),
            db_host=parser.get("database", "db_host", fallback="localhost"),
            db_port=parser.get("database", "db_port", fallback="5432"),
            temp_db_prefix=parser.get("database", "temp_db_prefix", fallback="temp_restore_"),
            backup_storage_dir=parser.get("paths", "backup_storage_dir", fallback="/var/backups/postgres"),
            old_backup_dir=parser.get(
                "paths", "old_backup_dir", fallback="/workspace/old_backup"
            ),
            temp_dir=parser.get("paths", "temp_dir", fallback="/tmp/pg_etl_temp"),
            log_file=log_file,
            log_level=parser.get("logging", "log_level", fallback="INFO"),
            max_backups=parser.getint("retention", "max_backups", fallback=3),
            cleanup_temp_db=parser.getboolean("retention", "cleanup_temp_db", fallback=True),
            weekly_enabled=parser.getboolean("schedule", "weekly_enabled", fallback=True),
            weekly_days=weekly_days,
            weekly_group_name_pattern=parser.get("schedule", "weekly_group_name_pattern", fallback="masked"),
            remote_user=parser.get("remote", "remote_user", fallback=""),
            remote_host=parser.get("remote", "remote_host", fallback=""),
            remote_path=parser.get("remote", "remote_path", fallback=""),
            backup_tar=parser.get("remote", "backup_tar", fallback=""),
            issues_table=issues_table,
            projects_table=projects_table,
            auto_discover_tables=parser.getboolean(
                "tables", "auto_discover_tables", fallback=True
            ),
            fail_on_unkeyed_tables=parser.getboolean(
                "tables", "fail_on_unkeyed_tables", fallback=True
            ),
            snapshot_excluded_tables=snapshot_excluded_tables,
            snapshot_replace_tables=snapshot_replace_tables,
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

    @staticmethod
    def _parse_identifier_list(raw_value: str) -> Tuple[str, ...]:
        identifiers: List[str] = []
        for value in raw_value.split(","):
            value = value.strip()
            if not value:
                continue
            identifiers.append(_validate_identifier(value, "table_name"))
        return tuple(identifiers)

    @staticmethod
    def _parse_weekly_days(raw_value: str) -> Tuple[int, ...]:
        try:
            days = tuple(sorted({int(value.strip()) for value in raw_value.split(",") if value.strip()}))
        except ValueError as exc:
            raise ValueError(
                f"Некорректный weekly_days: {raw_value}. Используйте числа 1-7 через запятую."
            ) from exc
        if not days or any(day < 1 or day > 7 for day in days):
            raise ValueError(
                f"Некорректный weekly_days: {raw_value}. Допустимы дни недели 1-7."
            )
        return days


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

    def _preview_sql(self, text: str, limit: int = 220) -> str:
        compact = " ".join(text.split())
        return compact[:limit] + ("..." if len(compact) > limit else "")

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
        context_label: Optional[str] = None,
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
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(
                f"PSQL timeout{label}: db={target_db}, sql={self._preview_sql(command, 160)}"
            )
            return "" if capture_output else False
        except Exception as exc:
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(f"Ошибка запуска psql{label}: db={target_db}, error={exc}")
            return "" if capture_output else False

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            label = f" [{context_label}]" if context_label else ""
            details = f"db={target_db}, sql={self._preview_sql(command, 160)}"
            if stderr and not ignore_errors:
                self.logger.error(f"PSQL error{label}: {stderr} | {details}")
            elif stderr:
                self.logger.warning(f"PSQL warning{label}: {stderr} | {details}")
            return "" if capture_output else False

        if capture_output:
            return result.stdout
        return True

    def _run_psql_script(
        self,
        script: str,
        db_name: Optional[str] = None,
        ignore_errors: bool = False,
        context_label: Optional[str] = None,
    ) -> bool:
        target_db = db_name or self.settings.db_name
        cmd = self._psql_base_cmd(target_db)
        try:
            result = subprocess.run(
                cmd,
                input=script,
                capture_output=True,
                text=True,
                timeout=1800,
            )
        except subprocess.TimeoutExpired:
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(
                f"PSQL script timeout{label}: db={target_db}, sql={self._preview_sql(script, 180)}"
            )
            return False
        except Exception as exc:
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(f"Ошибка запуска psql script{label}: db={target_db}, error={exc}")
            return False

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            label = f" [{context_label}]" if context_label else ""
            details = f"db={target_db}, sql={self._preview_sql(script, 180)}"
            if stderr and not ignore_errors:
                self.logger.error(f"PSQL error{label}: {stderr} | {details}")
            elif stderr:
                self.logger.warning(f"PSQL warning{label}: {stderr} | {details}")
            return False

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
            self.logger.error(f"SQL file execution timed out: db={target_db}, file={sql_file}")
            return False
        except Exception as exc:
            self.logger.error(f"Error executing SQL file: db={target_db}, file={sql_file}, error={exc}")
            return False

        if result.returncode != 0:
            stderr = "\n".join(
                line for line in result.stderr.splitlines() if "already exists" not in line.lower()
            ).strip()
            if stderr:
                self.logger.error(f"SQL file error: db={target_db}, file={sql_file}, error={stderr}")
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

    def cleanup_stale_databases(
        self,
        exclude_names: Optional[Sequence[str]] = None,
    ) -> Dict[str, List[str]]:
        """Удаляет осиротевшие staging-БД предыдущих запусков."""
        prefix = _validate_identifier(self.settings.temp_db_prefix, "temp_db_prefix")
        protected = {name for name in (exclude_names or []) if name}
        generated_name_pattern = re.compile(
            rf"^{re.escape(prefix)}\d{{8}}_\d{{6}}$"
        )
        rows = self.run_rows(
            """
            SELECT
                d.datname,
                COUNT(a.pid)::text
            FROM pg_database d
            LEFT JOIN pg_stat_activity a ON a.datname = d.datname
            GROUP BY d.datname
            ORDER BY d.datname;
            """,
            db_name="postgres",
        )

        removed: List[str] = []
        skipped_active: List[str] = []
        skipped_protected: List[str] = []
        for row in rows:
            if len(row) < 2:
                continue
            db_name, active_connections_raw = row[0], row[1]
            if not generated_name_pattern.fullmatch(db_name):
                continue
            if db_name in protected:
                skipped_protected.append(db_name)
                self.logger.info(
                    f"Staging cleanup: база защищена текущим запуском, db={db_name}"
                )
                continue

            active_connections = int(active_connections_raw or 0)
            if active_connections > 0:
                skipped_active.append(db_name)
                self.logger.warning(
                    f"Staging cleanup: база пропущена из-за активных подключений, "
                    f"db={db_name}, connections={active_connections}"
                )
                continue

            self.logger.info(f"Staging cleanup: удаление осиротевшей базы {db_name}")
            if self._run_psql(
                f'DROP DATABASE IF EXISTS "{db_name}";',
                db_name="postgres",
                context_label=f"staging-cleanup:{db_name}",
            ):
                removed.append(db_name)
            else:
                self.logger.error(
                    f"Staging cleanup: не удалось безопасно удалить базу {db_name}; "
                    "возможно, появилось активное подключение"
                )

        self.logger.info(
            "Staging cleanup завершен: "
            f"removed={len(removed)}, active_skipped={len(skipped_active)}, "
            f"protected={len(skipped_protected)}"
        )
        return {
            "removed": removed,
            "skipped_active": skipped_active,
            "skipped_protected": skipped_protected,
        }

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

    def get_table_column_details(self, table_name: str, db_name: str) -> List[Tuple[str, str]]:
        table_name = _validate_identifier(table_name, "table_name")
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
        return [(row[0], row[1]) for row in self.run_rows(query, db_name=db_name) if len(row) >= 2]

    def get_column_default(self, table_name: str, column_name: str, db_name: str) -> str:
        table_name = _validate_identifier(table_name, "table_name")
        column_name = _validate_identifier(column_name, "column")
        query = f"""
            SELECT COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
              AND column_name = '{column_name}';
        """
        return self.run_scalar(query, db_name=db_name)

    def sync_sequence_with_max_id(self, table_name: str, db_name: str, column_name: str = "id") -> bool:
        table_name = _validate_identifier(table_name, "table_name")
        column_name = _validate_identifier(column_name, "column")
        default_expr = self.get_column_default(table_name, column_name, db_name)
        if "nextval(" not in default_expr:
            return False

        query = f"""
            SELECT setval(
                pg_get_serial_sequence('public.{table_name}', '{column_name}'),
                COALESCE((SELECT MAX({_quote_identifier(column_name)}) FROM {_quote_identifier(table_name)}), 0),
                true
            )::text;
        """
        result = self.run_scalar(query, db_name=db_name)
        self.logger.info(
            f"Sequence synchronized: db={db_name}, table={table_name}, column={column_name}, "
            f"default={default_expr!r}, setval_result={result}"
        )
        return True

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

    def log_table_schema(self, table_name: str, db_name: str, include_indexes: bool = True) -> None:
        if not self.table_exists(table_name, db_name):
            self.logger.warning(f"Schema snapshot: таблица отсутствует, db={db_name}, table={table_name}")
            return

        column_details = self.get_table_column_details(table_name, db_name)
        row_count = self.count_rows(table_name, db_name)
        columns_preview = ", ".join(f"{name}:{dtype}" for name, dtype in column_details[:20])
        if len(column_details) > 20:
            columns_preview += ", ..."
        self.logger.info(
            f"Schema snapshot: db={db_name}, table={table_name}, rows={row_count}, "
            f"columns=[{columns_preview}]"
        )
        if include_indexes:
            indexes = self.list_indexes(table_name, db_name)
            if indexes:
                indexes_preview = " | ".join(indexes[:5])
                if len(indexes) > 5:
                    indexes_preview += " | ..."
                self.logger.info(
                    f"Index snapshot: db={db_name}, table={table_name}, indexes={indexes_preview}"
                )
            else:
                self.logger.warning(f"Index snapshot: индексы не найдены, db={db_name}, table={table_name}")

    def log_pipeline_schema_snapshot(
        self,
        db_name: str,
        issues_table: str = "issues",
        projects_table: str = "projects",
    ) -> None:
        tables = [
            issues_table,
            projects_table,
            "custom_values",
            "custom_fields",
            "asterisk_cdr",
            "group_employee_count",
            "users_active",
        ]
        self.logger.info(f"=== Schema Snapshot: db={db_name} ===")
        for table_name in tables:
            self.log_table_schema(table_name, db_name)

    def export_query_to_csv(self, query: str, csv_file: str, db_name: str) -> int:
        count_query = f"SELECT COUNT(*) FROM ({query}) AS subq;"
        count = int(self.run_scalar(count_query, db_name=db_name) or 0)
        if count == 0:
            self.logger.info(
                f"Экспорт пропущен: db={db_name}, csv={csv_file}, rows=0, sql={self._preview_sql(query, 120)}"
            )
            return 0

        export_cmd = self._psql_base_cmd(db_name) + [
            "-c",
            f"\\copy ({query}) TO STDOUT WITH (FORMAT CSV, HEADER true)",
        ]
        with open(csv_file, "w", encoding="utf-8") as handle:
            result = subprocess.run(export_cmd, stdout=handle, stderr=subprocess.PIPE, text=True, timeout=1800)
        if result.returncode != 0:
            self.logger.error(
                f"Export error: db={db_name}, csv={csv_file}, error={result.stderr.strip()}, sql={self._preview_sql(query, 120)}"
            )
            return 0
        self.logger.info(f"Экспортирован CSV: db={db_name}, csv={csv_file}, rows={count}")
        return count

    def export_table_to_csv(
        self,
        table_name: str,
        csv_file: str,
        db_name: str,
        order_by: Optional[Sequence[str] | str] = None,
    ) -> int:
        table_name = _validate_identifier(table_name, "table_name")
        columns = self.get_table_columns(table_name, db_name)
        protected_tables = {self.settings.issues_table, self.settings.projects_table}
        blocked_columns = [
            column
            for column in columns
            if table_name in protected_tables and column.startswith("cf_")
        ]
        export_columns = [column for column in columns if column not in blocked_columns]
        if not export_columns:
            self.logger.error(
                f"Экспорт невозможен: после исключения cf_* не осталось колонок, "
                f"db={db_name}, table={table_name}"
            )
            return 0
        if blocked_columns:
            self.logger.info(
                f"Custom value columns исключены из snapshot: db={db_name}, "
                f"table={table_name}, columns={blocked_columns}"
            )
        quoted_columns = ", ".join(_quote_identifier(column) for column in export_columns)
        query = f"SELECT {quoted_columns} FROM {_quote_identifier(table_name)}"
        if order_by:
            order_columns = list(order_by) if isinstance(order_by, (list, tuple)) else [order_by]
            normalized_order_columns = [
                _validate_identifier(column, "order_column") for column in order_columns
            ]
            missing_order_columns = [
                column for column in normalized_order_columns if column not in export_columns
            ]
            if missing_order_columns:
                raise ValueError(
                    f"Snapshot key отсутствует в экспортируемых колонках: "
                    f"table={table_name}, columns={missing_order_columns}"
                )
            query += " ORDER BY " + ", ".join(
                _quote_identifier(column) for column in normalized_order_columns
            )
        return self.export_query_to_csv(query, csv_file, db_name)

    def upsert_from_csv(self, table_name: str, primary_key: Sequence[str] | str, csv_file: str, db_name: str) -> bool:
        if not os.path.exists(csv_file):
            self.logger.warning(f"CSV файл не найден для UPSERT: table={table_name}, csv={csv_file}")
            return False

        table_name = _validate_identifier(table_name, "table_name")
        pk_cols = list(primary_key) if isinstance(primary_key, (list, tuple)) else [primary_key]
        pk_cols = [_validate_identifier(pk, "primary_key") for pk in pk_cols]

        csv_columns = [_validate_identifier(col, "column") for col in _read_csv_header(csv_file)]
        protected_tables = {self.settings.issues_table, self.settings.projects_table}
        blocked_columns = [
            column
            for column in csv_columns
            if table_name in protected_tables and column.startswith("cf_")
        ]
        if blocked_columns:
            self.logger.error(
                f"UPSERT остановлен: snapshot для {table_name} содержит запрещенные "
                f"custom value columns: {blocked_columns}"
            )
            return False
        table_columns = set(self.get_table_columns(table_name, db_name))
        columns = [col for col in csv_columns if col in table_columns]
        skipped_columns = [col for col in csv_columns if col not in table_columns]
        if not columns:
            self.logger.error(
                f"В CSV нет совпадающих колонок для UPSERT: table={table_name}, csv={csv_file}, "
                f"csv_columns={csv_columns}, table_columns={sorted(table_columns)}"
            )
            return False
        if skipped_columns:
            self.logger.error(
                f"UPSERT остановлен из-за расхождения схемы: в main отсутствуют колонки "
                f"из snapshot, db={db_name}, table={table_name}, "
                f"missing_target_columns={skipped_columns}"
            )
            return False
        missing_pk_columns = [pk for pk in pk_cols if pk not in columns]
        if missing_pk_columns:
            self.logger.error(
                f"UPSERT остановлен: ключевые колонки отсутствуют в snapshot, "
                f"db={db_name}, table={table_name}, missing_pk={missing_pk_columns}"
            )
            return False

        quoted_columns = ", ".join(_quote_identifier(col) for col in columns)
        update_columns = [col for col in columns if col not in pk_cols]
        conflict_columns = ", ".join(_quote_identifier(pk) for pk in pk_cols)
        update_clause = ", ".join(
            f'{_quote_identifier(col)} = EXCLUDED.{_quote_identifier(col)}' for col in update_columns
        )
        change_predicate = " OR ".join(
            f'{_quote_identifier(table_name)}.{_quote_identifier(col)} '
            f'IS DISTINCT FROM EXCLUDED.{_quote_identifier(col)}'
            for col in update_columns
        )
        conflict_action = (
            f"DO UPDATE SET {update_clause} WHERE {change_predicate}"
            if update_clause
            else "DO NOTHING"
        )
        data_csv_file = _strip_csv_header(csv_file, delimiter=",")

        copy_command = (
            f"\\copy temp_upsert ({quoted_columns}) FROM '{data_csv_file}' "
            f"WITH (FORMAT csv, HEADER false, DELIMITER ',', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');"
        )
        script = f"""
CREATE TEMP TABLE temp_upsert AS
SELECT {quoted_columns}
FROM {_quote_identifier(table_name)}
LIMIT 0;

{copy_command}

INSERT INTO {_quote_identifier(table_name)} ({quoted_columns})
SELECT {quoted_columns}
FROM temp_upsert
ON CONFLICT ({conflict_columns})
{conflict_action};

DROP TABLE temp_upsert;
"""
        self.logger.info(
            f"UPSERT подготовлен: db={db_name}, table={table_name}, pk={pk_cols}, "
            f"csv={csv_file}, data_csv={data_csv_file}, header=true->stripped, delimiter=',', columns={columns}"
        )
        try:
            success = self._run_psql_script(
                script,
                db_name=db_name,
                context_label=f"upsert:{table_name}",
            )
            if success and len(pk_cols) == 1:
                self.sync_sequence_with_max_id(table_name, db_name, column_name=pk_cols[0])
            return success
        finally:
            try:
                os.remove(data_csv_file)
            except OSError:
                pass

    def replace_from_csv(self, table_name: str, csv_file: str, db_name: str) -> bool:
        """Атомарно заменяет keyless-таблицу точным snapshot из CSV."""
        if not os.path.exists(csv_file):
            self.logger.warning(
                f"CSV файл не найден для REPLACE: table={table_name}, csv={csv_file}"
            )
            return False

        table_name = _validate_identifier(table_name, "table_name")
        csv_columns = [
            _validate_identifier(column, "column")
            for column in _read_csv_header(csv_file)
        ]
        table_columns = self.get_table_columns(table_name, db_name)
        if not csv_columns:
            self.logger.error(
                f"CSV не содержит заголовок для REPLACE: table={table_name}, csv={csv_file}"
            )
            return False
        if set(csv_columns) != set(table_columns):
            self.logger.error(
                f"REPLACE остановлен из-за расхождения схемы: db={db_name}, "
                f"table={table_name}, csv_columns={csv_columns}, "
                f"table_columns={table_columns}"
            )
            return False

        quoted_columns = ", ".join(
            _quote_identifier(column) for column in csv_columns
        )
        data_csv_file = _strip_csv_header(csv_file, delimiter=",")
        copy_command = (
            f"\\copy temp_snapshot_replace ({quoted_columns}) FROM '{data_csv_file}' "
            f"WITH (FORMAT csv, HEADER false, DELIMITER ',', QUOTE '\"', "
            f"ESCAPE '\"', ENCODING 'UTF8');"
        )
        script = f"""
CREATE TEMP TABLE temp_snapshot_replace AS
SELECT {quoted_columns}
FROM {_quote_identifier(table_name)}
LIMIT 0;

{copy_command}

BEGIN;
LOCK TABLE {_quote_identifier(table_name)} IN ACCESS EXCLUSIVE MODE;
DELETE FROM {_quote_identifier(table_name)};
INSERT INTO {_quote_identifier(table_name)} ({quoted_columns})
SELECT {quoted_columns}
FROM temp_snapshot_replace;
COMMIT;

DROP TABLE temp_snapshot_replace;
"""
        self.logger.info(
            f"REPLACE snapshot подготовлен: db={db_name}, table={table_name}, "
            f"csv={csv_file}, data_csv={data_csv_file}, columns={csv_columns}"
        )
        try:
            return self._run_psql_script(
                script,
                db_name=db_name,
                context_label=f"replace:{table_name}",
            )
        finally:
            try:
                os.remove(data_csv_file)
            except OSError:
                pass

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
            self.logger.warning(f"Seed CSV не найден: table={table_name}, csv={csv_file}")
            return False
        if not self.table_exists(table_name, db_name):
            self.logger.warning(f"Seed import пропущен: таблица отсутствует, table={table_name}, db={db_name}")
            return False
        if self.count_rows(table_name, db_name) > 0:
            self.logger.info(f"Seed import пропущен: таблица не пуста, table={table_name}, db={db_name}")
            return False

        table_name = _validate_identifier(table_name, "table_name")
        table_columns = self.get_table_columns(table_name, db_name)
        table_columns_by_lower = {column.lower(): column for column in table_columns}
        csv_columns = [_normalize_csv_column_name(col) for col in _read_csv_header(csv_file, delimiter=delimiter)]
        self.logger.info(
            f"Seed import старт: db={db_name}, table={table_name}, csv={csv_file}, delimiter='{delimiter}', "
            f"csv_columns={csv_columns}"
        )
        mapped_columns = []
        for csv_column in csv_columns:
            matched_column = table_columns_by_lower.get(csv_column.lower())
            if not matched_column:
                self.logger.warning(
                    f"Колонка {csv_column} из {csv_file} отсутствует в таблице {table_name}, импорт пропущен. "
                    f"table_columns={table_columns}"
                )
                return False
            mapped_columns.append(matched_column)

        quoted_columns = ", ".join(_quote_identifier(col) for col in mapped_columns)
        command = (
            f"\\copy {_quote_identifier(table_name)} ({quoted_columns}) "
            f"FROM '{csv_file}' "
            f"WITH (FORMAT csv, HEADER true, DELIMITER '{delimiter}', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8');"
        )
        result = bool(
            self._run_psql(
                command,
                db_name=db_name,
                ignore_errors=True,
                context_label=f"seed:{table_name}",
            )
        )
        if result:
            self.sync_sequence_with_max_id(table_name, db_name)
            self.logger.info(f"Seed import выполнен: db={db_name}, table={table_name}, csv={csv_file}")
        return result

    def seed_manual_tables_if_needed(self, db_name: str, seed_files: Dict[str, str]) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for table_name, csv_file in seed_files.items():
            imported = self.import_csv_if_table_empty(table_name, csv_file, db_name)
            results[table_name] = imported
            if imported:
                self.logger.info(f"Импортирован seed для {table_name}: {csv_file}")
        return results

    def append_asterisk_cdr_from_csv(
        self,
        db_name: str,
        csv_file: Optional[str] = None,
    ) -> Dict[str, object]:
        csv_file = csv_file or self.settings.asterisk_csv_file
        table_name = "asterisk_cdr"
        if not os.path.exists(csv_file):
            self.logger.warning(f"Asterisk CSV не найден: {csv_file}")
            return {
                "success": True,
                "skipped": True,
                "reason": "csv_not_found",
                "source_rows": 0,
                "inserted_rows": 0,
            }
        if not self.table_exists(table_name, db_name):
            self.logger.error(f"Таблица {table_name} отсутствует в db={db_name}")
            return {
                "success": False,
                "skipped": False,
                "reason": "table_not_found",
                "source_rows": 0,
                "inserted_rows": 0,
            }

        table_columns = self.get_table_columns(table_name, db_name)
        table_columns_by_lower = {column.lower(): column for column in table_columns}
        csv_columns = [
            _normalize_csv_column_name(column)
            for column in _read_csv_header(csv_file, delimiter=";")
        ]
        mapped_columns: List[str] = []
        for csv_column in csv_columns:
            matched_column = table_columns_by_lower.get(csv_column)
            if not matched_column:
                self.logger.error(
                    f"Колонка {csv_column} из {csv_file} отсутствует в {table_name}; "
                    f"table_columns={table_columns}"
                )
                return {
                    "success": False,
                    "skipped": False,
                    "reason": f"missing_column:{csv_column}",
                    "source_rows": 0,
                    "inserted_rows": 0,
                }
            mapped_columns.append(matched_column)
        if "id" not in {column.lower() for column in mapped_columns}:
            self.logger.error(f"Asterisk CSV не содержит обязательную колонку id: {csv_file}")
            return {
                "success": False,
                "skipped": False,
                "reason": "missing_primary_key",
                "source_rows": 0,
                "inserted_rows": 0,
            }

        with open(csv_file, "r", encoding="utf-8", newline="") as handle:
            source_rows = max(sum(1 for _ in csv.reader(handle, delimiter=";")) - 1, 0)
        rows_before = self.count_rows(table_name, db_name)
        quoted_columns = ", ".join(_quote_identifier(column) for column in mapped_columns)
        escaped_csv_file = csv_file.replace("'", "''")
        copy_command = (
            f"\\copy temp_asterisk_import ({quoted_columns}) FROM '{escaped_csv_file}' "
            "WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', "
            "ESCAPE '\"', ENCODING 'UTF8');"
        )
        script = f"""
CREATE TEMP TABLE temp_asterisk_import AS
SELECT {quoted_columns}
FROM {_quote_identifier(table_name)}
LIMIT 0;

{copy_command}

INSERT INTO {_quote_identifier(table_name)} ({quoted_columns})
SELECT {quoted_columns}
FROM temp_asterisk_import
ON CONFLICT ({_quote_identifier("id")}) DO NOTHING;
"""
        self.logger.info(
            f"Asterisk append старт: db={db_name}, csv={csv_file}, "
            f"source_rows={source_rows}, rows_before={rows_before}, columns={mapped_columns}"
        )
        if not self._run_psql_script(
            script,
            db_name=db_name,
            context_label="asterisk:append",
        ):
            return {
                "success": False,
                "skipped": False,
                "reason": "psql_error",
                "source_rows": source_rows,
                "inserted_rows": 0,
                "rows_before": rows_before,
            }

        self.sync_sequence_with_max_id(table_name, db_name)
        self.add_project_id_column(db_name)
        rows_after = self.count_rows(table_name, db_name)
        inserted_rows = max(rows_after - rows_before, 0)
        self.logger.info(
            f"Asterisk append завершен: db={db_name}, source_rows={source_rows}, "
            f"inserted_rows={inserted_rows}, existing_rows={max(source_rows - inserted_rows, 0)}, "
            f"rows_after={rows_after}"
        )
        return {
            "success": True,
            "skipped": False,
            "reason": "",
            "source_rows": source_rows,
            "inserted_rows": inserted_rows,
            "rows_before": rows_before,
            "rows_after": rows_after,
        }

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

    def list_snapshot_candidate_tables(self, db_name: str) -> List[str]:
        """Возвращает обычные и partitioned-таблицы public без дочерних partitions."""
        rows = self.run_rows(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p')
              AND NOT c.relispartition
            ORDER BY c.relname;
            """,
            db_name=db_name,
        )
        return [
            _validate_identifier(row[0], "table_name")
            for row in rows
            if row
        ]

    def discover_unique_table_keys(
        self,
        db_name: str,
    ) -> Dict[str, Sequence[str] | str]:
        """Находит PK либо простой непредикатный UNIQUE index для каждой таблицы."""
        rows = self.run_rows(
            """
            SELECT
                tbl.relname,
                idx.relname,
                i.indisprimary::text,
                key_column.ordinality::text,
                attr.attname
            FROM pg_class tbl
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN pg_index i ON i.indrelid = tbl.oid
            JOIN pg_class idx ON idx.oid = i.indexrelid
            CROSS JOIN LATERAL unnest(i.indkey)
                WITH ORDINALITY AS key_column(attnum, ordinality)
            JOIN pg_attribute attr
              ON attr.attrelid = tbl.oid
             AND attr.attnum = key_column.attnum
            WHERE ns.nspname = 'public'
              AND tbl.relkind IN ('r', 'p')
              AND NOT tbl.relispartition
              AND i.indisunique
              AND i.indisvalid
              AND i.indisready
              AND i.indpred IS NULL
              AND i.indexprs IS NULL
              AND key_column.attnum > 0
              AND key_column.ordinality <= i.indnkeyatts
            ORDER BY
                tbl.relname,
                i.indisprimary DESC,
                i.indnkeyatts,
                idx.relname,
                key_column.ordinality;
            """,
            db_name=db_name,
        )

        candidates: Dict[Tuple[str, str], Dict[str, object]] = {}
        candidate_order: List[Tuple[str, str]] = []
        for row in rows:
            if len(row) < 5:
                continue
            table_name = _validate_identifier(row[0], "table_name")
            index_name = row[1]
            candidate_id = (table_name, index_name)
            if candidate_id not in candidates:
                candidates[candidate_id] = {
                    "primary": row[2].lower() in {"t", "true", "1"},
                    "columns": [],
                }
                candidate_order.append(candidate_id)
            columns = candidates[candidate_id]["columns"]
            if isinstance(columns, list):
                columns.append((int(row[3]), _validate_identifier(row[4], "column")))

        keys: Dict[str, Sequence[str] | str] = {}
        for table_name, index_name in candidate_order:
            if table_name in keys:
                continue
            candidate = candidates[(table_name, index_name)]
            raw_columns = candidate["columns"]
            if not isinstance(raw_columns, list) or not raw_columns:
                continue
            ordered_columns = [column for _, column in sorted(raw_columns)]
            keys[table_name] = (
                ordered_columns[0] if len(ordered_columns) == 1 else ordered_columns
            )
            key_kind = "PRIMARY KEY" if candidate["primary"] else "UNIQUE index"
            self.logger.debug(
                f"Snapshot key discovered: db={db_name}, table={table_name}, "
                f"key={ordered_columns}, source={key_kind}:{index_name}"
            )
        return keys

    def order_snapshot_tables(
        self,
        db_name: str,
        tables: Dict[str, Sequence[str] | str],
    ) -> Dict[str, Sequence[str] | str]:
        """Ставит родительские таблицы раньше дочерних для FK-safe UPSERT."""
        selected = set(tables)
        dependencies: Dict[str, set[str]] = {table_name: set() for table_name in tables}
        rows = self.run_rows(
            """
            SELECT child.relname, parent.relname
            FROM pg_constraint constraint_row
            JOIN pg_class child ON child.oid = constraint_row.conrelid
            JOIN pg_class parent ON parent.oid = constraint_row.confrelid
            JOIN pg_namespace ns ON ns.oid = child.relnamespace
            WHERE constraint_row.contype = 'f'
              AND ns.nspname = 'public'
            ORDER BY child.relname, parent.relname;
            """,
            db_name=db_name,
        )
        for row in rows:
            if len(row) < 2:
                continue
            child, parent = row[0], row[1]
            if child in selected and parent in selected and child != parent:
                dependencies[child].add(parent)

        ordered: List[str] = []
        pending = set(tables)
        while pending:
            ready = sorted(
                table_name
                for table_name in pending
                if not (dependencies[table_name] & pending)
            )
            if not ready:
                cyclic = sorted(pending)
                self.logger.warning(
                    f"Snapshot FK cycle detected: db={db_name}, tables={cyclic}; "
                    "для цикла сохраняется алфавитный порядок"
                )
                ordered.extend(cyclic)
                break
            ordered.extend(ready)
            pending.difference_update(ready)

        return {table_name: tables[table_name] for table_name in ordered}

    def resolve_snapshot_tables(
        self,
        db_name: str,
        configured_tables: Dict[str, Sequence[str] | str],
    ) -> Dict[str, Sequence[str] | str]:
        """Объединяет автообнаруженные таблицы и явные overrides из config."""
        excluded = set(self.settings.snapshot_excluded_tables)
        replace_tables = set(self.settings.snapshot_replace_tables)
        resolved: Dict[str, Sequence[str] | str] = {}
        unkeyed_tables: List[str] = []

        if self.settings.auto_discover_tables:
            candidate_tables = [
                table_name
                for table_name in self.list_snapshot_candidate_tables(db_name)
                if table_name not in excluded and table_name not in replace_tables
            ]
            discovered_keys = self.discover_unique_table_keys(db_name)
            for table_name in candidate_tables:
                key = discovered_keys.get(table_name)
                if key:
                    resolved[table_name] = key
                elif table_name not in configured_tables:
                    unkeyed_tables.append(table_name)

        for table_name, key in configured_tables.items():
            table_name = _validate_identifier(table_name, "table_name")
            if table_name not in excluded:
                resolved[table_name] = key

        if unkeyed_tables:
            message = (
                "Snapshot auto-discovery: таблицы без PRIMARY KEY/UNIQUE index: "
                f"{unkeyed_tables}. Добавьте безопасный уникальный ключ в БД либо "
                "добавьте таблицы в snapshot_replace_tables для точного зеркалирования; "
                "исключайте их через snapshot_excluded_tables только если данные не нужны."
            )
            if self.settings.fail_on_unkeyed_tables:
                raise ValueError(message)
            self.logger.warning(message)

        ordered = self.order_snapshot_tables(db_name, resolved)
        self.logger.info(
            f"Snapshot tables resolved: db={db_name}, auto={self.settings.auto_discover_tables}, "
            f"tables={len(ordered)}, excluded={sorted(excluded)}, "
            f"replace={sorted(replace_tables)}, unkeyed={len(unkeyed_tables)}"
        )
        return ordered

    def add_weekly_records(
        self,
        db_name: str,
        target_days: Sequence[int] | int,
        force: bool = False,
    ) -> Dict[str, object]:
        today = date.today()
        current_day = today.isocalendar()[2]
        normalized_days = (
            (target_days,)
            if isinstance(target_days, int)
            else tuple(sorted(set(target_days)))
        )
        if not normalized_days or any(day < 1 or day > 7 for day in normalized_days):
            raise ValueError(f"Некорректные дни weekly: {normalized_days}")
        if not force and current_day not in normalized_days:
            return {
                "skipped": True,
                "current_day": current_day,
                "target_days": list(normalized_days),
                "force": force,
            }

        raw_pattern = self.settings.weekly_group_name_pattern.strip()
        normalized_pattern = raw_pattern.strip("\"'").strip()
        pattern_sql = normalized_pattern.replace("'", "''")
        group_filter = f"AND g.lastname ILIKE '{pattern_sql}'" if normalized_pattern else ""
        snapshot_date = str(today)
        db_current_date = self.run_scalar("SELECT CURRENT_DATE::text;", db_name=db_name)
        db_current_timestamp = self.run_scalar("SELECT CURRENT_TIMESTAMP::text;", db_name=db_name)
        self.logger.info(
            f"Weekly debug: db={db_name}, python_snapshot_date={snapshot_date}, "
            f"db_current_date={db_current_date}, db_current_timestamp={db_current_timestamp}, "
            f"target_days={list(normalized_days)}, force={force}, "
            f"group_name_pattern={normalized_pattern}"
        )
        self.log_table_schema("group_employee_count", db_name, include_indexes=False)
        self.log_table_schema("users_active", db_name, include_indexes=False)
        group_id_default = self.get_column_default("group_employee_count", "id", db_name)
        users_id_default = self.get_column_default("users_active", "id", db_name)
        self.sync_sequence_with_max_id("group_employee_count", db_name)
        self.sync_sequence_with_max_id("users_active", db_name)
        group_id_default = self.get_column_default("group_employee_count", "id", db_name)
        users_id_default = self.get_column_default("users_active", "id", db_name)
        self.logger.info(
            f"Weekly debug defaults: db={db_name}, "
            f"group_employee_count.id_default={group_id_default!r}, "
            f"users_active.id_default={users_id_default!r}"
        )

        group_source_count = int(
            self.run_scalar(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT g.id
                    FROM users u
                    JOIN groups_users gu ON u.id = gu.user_id
                    JOIN users g ON gu.group_id = g.id
                    WHERE u.status = 1
                    {group_filter}
                    GROUP BY g.id, g.lastname
                ) src;
                """,
                db_name=db_name,
            )
            or 0
        )
        group_source_sample = self.run_rows(
            f"""
            SELECT
                g.id::text,
                g.lastname,
                COUNT(DISTINCT u.id)::text
            FROM users u
            JOIN groups_users gu ON u.id = gu.user_id
            JOIN users g ON gu.group_id = g.id
            WHERE u.status = 1
            {group_filter}
            GROUP BY g.id, g.lastname
            ORDER BY g.id
            LIMIT 5;
            """,
            db_name=db_name,
        )
        if group_source_sample:
            self.logger.info(
                f"Weekly debug source sample: db={db_name}, sample={group_source_sample}"
            )
        active_users_count = int(
            self.run_scalar(
                """
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                WHERE u.status = 1;
                """,
                db_name=db_name,
            )
            or 0
        )

        existing_group_rows = int(
            self.run_scalar(
                """
                SELECT COUNT(*)
                FROM group_employee_count
                WHERE snapshot_date = CURRENT_DATE;
                """,
                db_name=db_name,
            )
            or 0
        )
        existing_users_row = int(
            self.run_scalar(
                """
                SELECT COUNT(*)
                FROM users_active
                WHERE snapshot_date = CURRENT_DATE;
                """,
                db_name=db_name,
            )
            or 0
        )

        group_rows = self.run_rows(
            f"""
            SELECT
                g.id::text,
                g.lastname,
                COUNT(DISTINCT u.id)::text
            FROM users u
            JOIN groups_users gu ON u.id = gu.user_id
            JOIN users g ON gu.group_id = g.id
            WHERE u.status = 1
            {group_filter}
            GROUP BY g.id, g.lastname
            ORDER BY g.id;
            """,
            db_name=db_name,
        )
        group_upserted_rows: List[List[str]] = []
        group_failures: List[str] = []
        next_group_id = self.count_rows("group_employee_count", db_name) + 1
        for group_id, group_name, user_count in group_rows:
            exists_query = f"""
                SELECT COUNT(*)
                FROM group_employee_count
                WHERE group_id = {int(group_id)}
                  AND snapshot_date = CURRENT_DATE;
            """
            exists = int(self.run_scalar(exists_query, db_name=db_name) or 0) > 0
            if exists:
                update_query = f"""
                    UPDATE group_employee_count
                    SET group_name = '{group_name.replace("'", "''")}',
                        user_count = {int(user_count)}
                    WHERE group_id = {int(group_id)}
                      AND snapshot_date = CURRENT_DATE;
                """
                if self._run_psql(
                    update_query,
                    db_name=db_name,
                    context_label=f"weekly:group_update:{group_id}",
                ):
                    group_upserted_rows.append([group_id, group_name, snapshot_date, user_count, "f"])
                else:
                    group_failures.append(f"update group_id={group_id}")
            else:
                if group_id_default:
                    insert_query = f"""
                        INSERT INTO group_employee_count (group_id, group_name, snapshot_date, user_count)
                        VALUES ({int(group_id)}, '{group_name.replace("'", "''")}', CURRENT_DATE, {int(user_count)});
                    """
                else:
                    insert_query = f"""
                        INSERT INTO group_employee_count (id, group_id, group_name, snapshot_date, user_count)
                        VALUES ({next_group_id}, {int(group_id)}, '{group_name.replace("'", "''")}', CURRENT_DATE, {int(user_count)});
                    """
                    next_group_id += 1
                if self._run_psql(
                    insert_query,
                    db_name=db_name,
                    context_label=f"weekly:group_insert:{group_id}",
                ):
                    group_upserted_rows.append([group_id, group_name, snapshot_date, user_count, "t"])
                else:
                    group_failures.append(f"insert group_id={group_id}")

        users_upserted_rows: List[List[str]] = []
        users_failures: List[str] = []
        users_exists = int(
            self.run_scalar(
                """
                SELECT COUNT(*)
                FROM users_active
                WHERE snapshot_date = CURRENT_DATE;
                """,
                db_name=db_name,
            )
            or 0
        ) > 0
        if users_exists:
            users_query = f"""
                UPDATE users_active
                SET user_count = {active_users_count}
                WHERE snapshot_date = CURRENT_DATE;
            """
            if self._run_psql(users_query, db_name=db_name, context_label="weekly:users_update"):
                users_upserted_rows.append([snapshot_date, str(active_users_count), "f"])
            else:
                users_failures.append("update users_active")
        else:
            if users_id_default:
                users_query = f"""
                    INSERT INTO users_active (snapshot_date, user_count)
                    VALUES (CURRENT_DATE, {active_users_count});
                """
            else:
                next_users_id = self.count_rows("users_active", db_name) + 1
                users_query = f"""
                    INSERT INTO users_active (id, snapshot_date, user_count)
                    VALUES ({next_users_id}, CURRENT_DATE, {active_users_count});
                """
            if self._run_psql(users_query, db_name=db_name, context_label="weekly:users_insert"):
                users_upserted_rows.append([snapshot_date, str(active_users_count), "t"])
            else:
                users_failures.append("insert users_active")

        final_group_rows = int(
            self.run_scalar(
                """
                SELECT COUNT(*)
                FROM group_employee_count
                WHERE snapshot_date = CURRENT_DATE;
                """,
                db_name=db_name,
            )
            or 0
        )
        final_users_row = int(
            self.run_scalar(
                """
                SELECT COUNT(*)
                FROM users_active
                WHERE snapshot_date = CURRENT_DATE;
                """,
                db_name=db_name,
            )
            or 0
        )

        group_upserted = len(group_upserted_rows)
        group_inserted = sum(1 for row in group_upserted_rows if len(row) >= 5 and row[4].lower() == "t")
        group_updated = max(group_upserted - group_inserted, 0)
        users_upserted = len(users_upserted_rows)
        users_inserted = sum(1 for row in users_upserted_rows if len(row) >= 3 and row[2].lower() == "t")
        users_updated = max(users_upserted - users_inserted, 0)
        if group_upserted_rows:
            self.logger.info(
                f"Weekly debug upserted groups sample: db={db_name}, sample={group_upserted_rows[:5]}"
            )
        if users_upserted_rows:
            self.logger.info(
                f"Weekly debug upserted users_active rows: db={db_name}, rows={users_upserted_rows}"
            )
        self.logger.info(
            f"Weekly debug after upsert: db={db_name}, group_rows_before={existing_group_rows}, "
            f"group_rows_after={final_group_rows}, users_rows_before={existing_users_row}, "
            f"users_rows_after={final_users_row}"
        )
        if group_failures:
            self.logger.error(f"Weekly group failures: db={db_name}, failures={group_failures}")
        if users_failures:
            self.logger.error(f"Weekly users failures: db={db_name}, failures={users_failures}")

        return {
            "skipped": False,
            "current_day": current_day,
            "target_days": list(normalized_days),
            "force": force,
            "snapshot_date": snapshot_date,
            "group_source_count": group_source_count,
            "group_upserted": group_upserted,
            "group_inserted": group_inserted,
            "group_updated": group_updated,
            "users_active_count": active_users_count,
            "users_upserted": users_upserted,
            "users_inserted": users_inserted,
            "users_updated": users_updated,
            "weekly_group_name_pattern": normalized_pattern,
            "group_failures": group_failures,
            "users_failures": users_failures,
        }


def remote_backup_configured(settings: PipelineSettings) -> bool:
    return bool(
        settings.remote_user
        and settings.remote_host
        and settings.remote_path
        and settings.backup_tar
    )


def should_download_remote_backup(
    settings: PipelineSettings,
    dump_file: Optional[str],
) -> bool:
    return remote_backup_configured(settings) and not (
        dump_file and os.path.exists(dump_file)
    )


def copy_from_remote_or_local(
    settings: PipelineSettings,
    logger: ETLLogger,
    dump_file: Optional[str],
) -> Optional[str]:
    if dump_file and os.path.exists(dump_file):
        logger.info(f"Использую локальный дамп: {dump_file}")
        return dump_file

    if remote_backup_configured(settings):
        logger.info("Шаг 1: Скачивание нового архива во временный файл...")
        source = f"{settings.remote_user}@{settings.remote_host}:{settings.remote_path}{settings.backup_tar}"
        os.makedirs(settings.temp_dir, exist_ok=True)
        backup_name = os.path.basename(settings.backup_tar)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        destination = os.path.join(
            settings.temp_dir,
            f"incoming_{timestamp}_{backup_name}",
        )
        result = subprocess.run(
            ["scp", source, destination],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if (
            result.returncode == 0
            and os.path.isfile(destination)
            and os.path.getsize(destination) > 0
        ):
            logger.info(
                f"Новый архив скачан во временный файл: {destination}, "
                f"size={os.path.getsize(destination)}"
            )
            return destination
        logger.error(f"Ошибка копирования: {(result.stderr or '').strip()}")
        try:
            os.remove(destination)
        except OSError:
            pass

    if dump_file and os.path.exists(dump_file):
        return dump_file

    logger.error(
        f"Файл не найден и удаленный источник не настроен: {dump_file or '<не указан>'}"
    )
    return None


def _migrate_legacy_backup_dirs(settings: PipelineSettings, logger: ETLLogger) -> bool:
    """Переносит old_* из старого backup_storage_dir в новый old_backup_dir."""
    os.makedirs(settings.backup_storage_dir, exist_ok=True)
    os.makedirs(settings.old_backup_dir, exist_ok=True)
    success = True
    for item in sorted(os.listdir(settings.backup_storage_dir)):
        if not item.startswith("old_"):
            continue
        source = os.path.join(settings.backup_storage_dir, item)
        if not os.path.isdir(source):
            continue
        target = os.path.join(settings.old_backup_dir, item)
        if os.path.exists(target):
            target = os.path.join(
                settings.old_backup_dir,
                f"{item}_legacy_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
            )
        try:
            shutil.move(source, target)
            logger.info(f"Старая резервная копия перенесена: {source} -> {target}")
        except OSError as exc:
            logger.error(f"Не удалось перенести старую резервную копию {source}: {exc}")
            success = False
    return success


def rotate_backups(settings: PipelineSettings, logger: ETLLogger) -> bool:
    """Оставляет max_backups - 1 исторических каталогов плюс текущий архив."""
    logger.info(
        f"Ротация бэкапов: current=1, old_limit={max(settings.max_backups - 1, 0)}, "
        f"old_dir={settings.old_backup_dir}"
    )
    os.makedirs(settings.old_backup_dir, exist_ok=True)
    old_dirs: List[Tuple[float, str]] = []
    for item in os.listdir(settings.old_backup_dir):
        if not item.startswith("old_"):
            continue
        full_path = os.path.join(settings.old_backup_dir, item)
        if os.path.isdir(full_path):
            old_dirs.append((os.path.getmtime(full_path), full_path))

    old_dirs.sort(reverse=True)
    keep_old = max(settings.max_backups - 1, 0)
    success = True
    for _, dir_path in old_dirs[keep_old:]:
        try:
            shutil.rmtree(dir_path)
            logger.info(f"Удалена устаревшая резервная копия: {dir_path}")
        except OSError as exc:
            logger.error(f"Не удалось удалить резервную копию {dir_path}: {exc}")
            success = False
    return success


def finalize_remote_backup(
    settings: PipelineSettings,
    logger: ETLLogger,
    incoming_file: str,
) -> bool:
    """После успешного ETL публикует новый архив и оставляет всего max_backups копий."""
    if not os.path.isfile(incoming_file) or os.path.getsize(incoming_file) == 0:
        logger.error(
            f"Нельзя завершить ротацию: временный архив отсутствует или пуст: {incoming_file}"
        )
        return False

    if settings.max_backups < 1:
        logger.error(
            f"Некорректный max_backups={settings.max_backups}; должно быть не меньше 1"
        )
        return False

    os.makedirs(settings.backup_storage_dir, exist_ok=True)
    os.makedirs(settings.old_backup_dir, exist_ok=True)
    if not _migrate_legacy_backup_dirs(settings, logger):
        return False

    backup_name = os.path.basename(settings.backup_tar)
    current_tar = os.path.join(settings.backup_storage_dir, backup_name)
    archived_current = ""
    archive_dir = ""

    try:
        if os.path.exists(current_tar):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            archive_dir = os.path.join(settings.old_backup_dir, f"old_{timestamp}")
            os.makedirs(archive_dir, exist_ok=False)
            archived_current = os.path.join(archive_dir, backup_name)
            shutil.move(current_tar, archived_current)
            logger.info(f"Предыдущий текущий архив перенесен: {archived_current}")

        shutil.move(incoming_file, current_tar)
        logger.info(f"Новый проверенный архив установлен как текущий: {current_tar}")
    except OSError as exc:
        logger.error(f"Ошибка публикации нового архива: {exc}")
        if (
            archived_current
            and os.path.exists(archived_current)
            and not os.path.exists(current_tar)
        ):
            try:
                shutil.move(archived_current, current_tar)
                logger.warning(f"Предыдущий текущий архив восстановлен: {current_tar}")
            except OSError as rollback_exc:
                logger.error(f"Не удалось восстановить предыдущий архив: {rollback_exc}")
        if archive_dir and os.path.isdir(archive_dir) and not os.listdir(archive_dir):
            shutil.rmtree(archive_dir, ignore_errors=True)
        return False

    if not rotate_backups(settings, logger):
        return False

    old_count = sum(
        1
        for item in os.listdir(settings.old_backup_dir)
        if item.startswith("old_")
        and os.path.isdir(os.path.join(settings.old_backup_dir, item))
    )
    logger.info(
        f"Ротация завершена: current=1, old={old_count}, total={1 + old_count}, "
        f"max_backups={settings.max_backups}"
    )
    return True


def cleanup_temp_artifacts(temp_dir: str, logger: ETLLogger, keep_latest: int = 0) -> None:
    os.makedirs(temp_dir, exist_ok=True)
    prefixes = ("extract_", "snapshot_", "pg_etl_upsert_", "incoming_")
    candidates: List[Tuple[float, str]] = []
    for item in os.listdir(temp_dir):
        if not item.startswith(prefixes):
            continue
        full_path = os.path.join(temp_dir, item)
        try:
            mtime = os.path.getmtime(full_path)
        except OSError:
            continue
        candidates.append((mtime, full_path))

    candidates.sort(reverse=True)
    for _, path in candidates[keep_latest:]:
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)
            logger.info(f"Удален временный артефакт: {path}")
        except OSError as exc:
            logger.warning(f"Не удалось удалить временный артефакт {path}: {exc}")


def log_git_revision(logger: ETLLogger) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    try:
        commit = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        branch = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        dirty = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--short"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if commit.returncode == 0 and branch.returncode == 0:
            dirty_flag = "dirty" if (dirty.stdout or "").strip() else "clean"
            logger.info(
                f"Версия репозитория: branch={branch.stdout.strip()}, commit={commit.stdout.strip()}, state={dirty_flag}"
            )
            if dirty_flag == "dirty":
                logger.warning("В рабочем дереве есть незакоммиченные изменения")
    except Exception as exc:
        logger.warning(f"Не удалось определить git-версию репозитория: {exc}")


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
    resolved_tables = db_ops.resolve_snapshot_tables(temp_db, snapshot_tables)
    for table_name, primary_key in resolved_tables.items():
        if not db_ops.table_exists(table_name, temp_db):
            db_ops.logger.warning(
                f"Snapshot table отсутствует в staging: db={temp_db}, table={table_name}"
            )
            continue
        csv_file = os.path.join(export_dir, f"{table_name}.csv")
        row_count = db_ops.export_table_to_csv(
            table_name,
            csv_file,
            temp_db,
            order_by=primary_key,
        )
        if row_count == 0 and not os.path.exists(csv_file):
            continue
        modifications.append(
            {
                "change_type": "snapshot",
                "load_mode": "upsert",
                "table_name": table_name,
                "primary_key": primary_key,
                "csv_file": csv_file,
                "row_count": row_count,
            }
        )

    replace_tables = db_ops.order_snapshot_tables(
        temp_db,
        {
            table_name: "__replace__"
            for table_name in db_ops.settings.snapshot_replace_tables
        },
    )
    for table_name in replace_tables:
        if not db_ops.table_exists(table_name, temp_db):
            db_ops.logger.warning(
                f"REPLACE snapshot table отсутствует в staging: "
                f"db={temp_db}, table={table_name}"
            )
            continue
        csv_file = os.path.join(export_dir, f"{table_name}.csv")
        row_count = db_ops.export_table_to_csv(
            table_name,
            csv_file,
            temp_db,
        )
        if row_count == 0 and not os.path.exists(csv_file):
            columns = db_ops.get_table_columns(table_name, temp_db)
            if not columns:
                raise ValueError(
                    f"REPLACE snapshot table не содержит колонок: "
                    f"db={temp_db}, table={table_name}"
                )
            with open(csv_file, "w", encoding="utf-8", newline="") as handle:
                csv.writer(handle).writerow(columns)
            os.chmod(csv_file, 0o644)
        modifications.append(
            {
                "change_type": "snapshot_replace",
                "load_mode": "replace",
                "table_name": table_name,
                "primary_key": None,
                "csv_file": csv_file,
                "row_count": row_count,
            }
        )
    return modifications
