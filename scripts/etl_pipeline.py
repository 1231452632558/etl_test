#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline - task-ориентированная версия с тем же nightly-потоком, что и монолит.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline_common import (  # noqa: E402
    DatabaseOperations,
    ETLLogger,
    PipelineSettings,
    archive_existing_backup,
    cleanup_temp_artifacts,
    copy_from_remote_or_local,
    log_git_revision,
    rotate_backups,
)
from tasks import (  # noqa: E402
    CleanupTask,
    CompareTask,
    ExtractTask,
    LoadTask,
    RestoreTask,
    TaskRunner,
    TransformCustomValuesTask,
    WeeklyTask,
)


def build_temp_db_name(settings: PipelineSettings) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{settings.temp_db_prefix}{timestamp}"


def build_runner(settings: PipelineSettings, logger: ETLLogger, db_ops: DatabaseOperations, init_mode: bool) -> TaskRunner:
    runner = TaskRunner(settings, logger, db_ops)
    runner.add_task(ExtractTask(settings, logger, db_ops))

    if init_mode:
        runner.add_task(RestoreTask(settings, logger, db_ops, target="main"))
        runner.add_task(TransformCustomValuesTask(settings, logger, db_ops, db_key="main_db"))
    else:
        runner.add_task(RestoreTask(settings, logger, db_ops, target="temp"))
        runner.add_task(TransformCustomValuesTask(settings, logger, db_ops, db_key="temp_db"))
        runner.add_task(CompareTask(settings, logger, db_ops))
        runner.add_task(LoadTask(settings, logger, db_ops))
        runner.add_task(WeeklyTask(settings, logger, db_ops))

    runner.add_task(CleanupTask(settings, logger, db_ops))
    return runner


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Pipeline для PostgreSQL (task-версия)")
    parser.add_argument("dump_file", help="Путь к .tar.gz или .sql дампу")
    parser.add_argument("--init", action="store_true", help="Первичная инициализация основной БД")
    parser.add_argument("--cleanup", action="store_true", help="Удалять staging БД после завершения")
    parser.add_argument(
        "--config",
        default="/workspace/config/etl_config.ini",
        help="Путь к конфигурационному файлу",
    )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Ошибка: Конфигурационный файл не найден: {args.config}")
        sys.exit(1)

    settings = PipelineSettings.from_file(args.config)
    logger = ETLLogger(settings.log_file, settings.log_level)
    log_git_revision(logger)
    db_ops = DatabaseOperations(settings, logger)

    cleanup_temp_artifacts(settings.temp_dir, logger)

    if (
        not args.init
        and not os.path.exists(args.dump_file)
        and settings.remote_user
        and settings.remote_host
        and settings.remote_path
        and settings.backup_tar
    ):
        archive_existing_backup(settings, logger)
        rotate_backups(settings, logger)

    actual_file = copy_from_remote_or_local(settings, logger, args.dump_file)
    if not actual_file:
        sys.exit(1)

    temp_db = None if args.init else build_temp_db_name(settings)
    runner = build_runner(settings, logger, db_ops, args.init)
    runner.update_context(
        {
            "dump_file": actual_file,
            "temp_dir": settings.temp_dir,
            "main_db": settings.db_name,
            "db_name": settings.db_name,
            "temp_db": temp_db,
            "tables": settings.snapshot_tables,
            "cleanup_temp_db": args.cleanup,
            "target_day": settings.target_day,
            "create_database": args.init,
        }
    )

    if args.init and db_ops.database_exists(settings.db_name):
        logger.error(f"База {settings.db_name} уже существует. Для --init нужна пустая целевая БД.")
        sys.exit(1)

    success = runner.run()
    cleanup_temp_artifacts(settings.temp_dir, logger)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
