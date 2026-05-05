#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Монолитная версия ETL pipeline для nightly-инкремента.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline_common import (  # noqa: E402
    DatabaseOperations,
    ETLLogger,
    PipelineSettings,
    build_snapshot_exports,
    copy_from_remote_or_local,
    extract_dump,
    rotate_backups,
)


class ETLPipeline:
    """Монолитный nightly pipeline без ежедневного удаления основной базы."""

    def __init__(self, config_path: str):
        self.settings = PipelineSettings.from_file(config_path)
        self.logger = ETLLogger(self.settings.log_file, self.settings.log_level)
        self.db_ops = DatabaseOperations(self.settings, self.logger)

    def _build_temp_db_name(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.settings.temp_db_prefix}{timestamp}"

    def _cleanup(self, temp_db: str | None, extract_dir: str | None, export_dir: str | None, cleanup_temp_db: bool) -> None:
        if cleanup_temp_db and temp_db:
            self.logger.info(f"Очистка временной базы {temp_db}...")
            self.db_ops.drop_database(temp_db)
        for path in (extract_dir, export_dir):
            if path:
                shutil.rmtree(path, ignore_errors=True)

    def run_init(self, dump_file: str) -> bool:
        self.logger.info("=== ЗАПУСК ИНИЦИАЛИЗАЦИИ ===")
        actual_file = copy_from_remote_or_local(self.settings, self.logger, dump_file)
        if not actual_file:
            return False

        extract_dir, sql_files = extract_dump(actual_file, self.settings.temp_dir, self.logger)
        if not sql_files:
            self.logger.error("SQL файлы не найдены")
            return False

        main_db = self.settings.db_name
        try:
            if self.db_ops.database_exists(main_db):
                self.logger.error(
                    f"База {main_db} уже существует. Для инициализации нужна пустая цель или отдельная база."
                )
                return False

            if not self.db_ops.create_database(main_db):
                return False
            if not self.db_ops.restore_dump(main_db, sql_files):
                return False

            self.db_ops.create_required_tables(main_db)
            self.db_ops.seed_asterisk_cdr_if_needed(main_db)
            self.db_ops.add_project_id_column(main_db)
            self.db_ops.seed_manual_tables_if_needed(main_db, self.settings.manual_seed_files)
            self.db_ops.log_pipeline_schema_snapshot(main_db, self.settings.issues_table)

            transform_result = self.db_ops.transform_custom_values(main_db, self.settings.issues_table)
            self.logger.info(
                f"Custom transform: fields={transform_result['custom_fields_count']}, "
                f"issues={transform_result['processed_count']}"
            )

            self.db_ops.grant_privileges(main_db)
            self.logger.info("=== ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО ===")
            return True
        finally:
            if extract_dir:
                shutil.rmtree(extract_dir, ignore_errors=True)

    def run_nightly(self, dump_file: str, cleanup: bool = True) -> bool:
        self.logger.info("=== ЗАПУСК НОЧНОЙ ОБРАБОТКИ ===")
        self.logger.info("Основная БД не пересоздается; nightly-дамп разворачивается только во временную staging БД.")

        actual_file = copy_from_remote_or_local(self.settings, self.logger, dump_file)
        if not actual_file:
            return False

        rotate_backups(self.settings, self.logger)

        extract_dir, sql_files = extract_dump(actual_file, self.settings.temp_dir, self.logger)
        if not sql_files:
            self.logger.error("SQL файлы не найдены")
            return False

        main_db = self.settings.db_name
        temp_db = self._build_temp_db_name()
        export_dir = os.path.join(self.settings.temp_dir, f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

        try:
            if not self.db_ops.database_exists(main_db):
                self.logger.error(f"Основная база {main_db} не существует. Сначала выполните --init.")
                return False

            if not self.db_ops.create_database(temp_db, recreate=True):
                return False
            if not self.db_ops.restore_dump(temp_db, sql_files):
                return False

            self.db_ops.create_required_tables(temp_db)
            self.db_ops.seed_asterisk_cdr_if_needed(temp_db)
            self.db_ops.add_project_id_column(temp_db)
            self.db_ops.log_pipeline_schema_snapshot(temp_db, self.settings.issues_table)

            transform_result = self.db_ops.transform_custom_values(temp_db, self.settings.issues_table)
            self.logger.info(
                f"Custom transform в staging: fields={transform_result['custom_fields_count']}, "
                f"issues={transform_result['processed_count']}, indexes={transform_result['index_count']}"
            )

            modifications = build_snapshot_exports(
                self.db_ops,
                temp_db,
                self.settings.snapshot_tables,
                export_dir,
            )

            for mod in modifications:
                self.logger.info(
                    f"UPSERT snapshot {mod['table_name']} ({mod['row_count']} rows) -> {main_db}"
                )
                if not self.db_ops.upsert_from_csv(
                    str(mod["table_name"]),
                    mod["primary_key"],
                    str(mod["csv_file"]),
                    main_db,
                ):
                    return False

            self.db_ops.create_required_tables(main_db)
            weekly_result = self.db_ops.add_weekly_records(main_db, self.settings.target_day)
            if weekly_result["skipped"]:
                self.logger.info(
                    f"Weekly insert пропущен: сегодня {weekly_result['current_day']}, целевой день {weekly_result['target_day']}"
                )
            else:
                self.logger.info("Weekly insert выполнен с upsert-поведением для ручных таблиц")

            self.db_ops.grant_privileges(main_db)
            self.logger.info("=== НОЧНАЯ ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО ===")
            return True
        finally:
            self._cleanup(temp_db, extract_dir, export_dir, cleanup)

    def run(self, dump_file: str, init_mode: bool = False, cleanup: bool = True) -> bool:
        self.logger.info(f"Дата запуска: {datetime.now()}")
        self.logger.info(f"Пользователь запуска: {os.getenv('USER', 'unknown')}")
        self.logger.info(f"Хост: {os.uname().nodename}")
        if init_mode:
            return self.run_init(dump_file)
        return self.run_nightly(dump_file, cleanup)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline для nightly SQL-дампов PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("dump_file", help="Путь к .tar.gz или .sql дампу")
    parser.add_argument("--init", action="store_true", help="Первичная инициализация основной БД")
    parser.add_argument("--cleanup", action="store_true", help="Удалять staging БД после завершения")
    parser.add_argument(
        "--config",
        default="/workspace/config/etl_config.ini",
        help="Путь к конфигурации",
    )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Ошибка: Конфигурационный файл не найден: {args.config}")
        sys.exit(1)

    pipeline = ETLPipeline(args.config)
    success = pipeline.run(args.dump_file, init_mode=args.init, cleanup=args.cleanup)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
