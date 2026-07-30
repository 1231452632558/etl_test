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
    SeedAsteriskTask,
    TaskRunner,
    WeeklyTask,
)


STAGE_NAMES = (
    "extract",
    "restore",
    "seed_asterisk",
    "compare",
    "load",
    "weekly",
    "cleanup",
)


def build_temp_db_name(settings: PipelineSettings) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{settings.temp_db_prefix}{timestamp}"


def parse_stage_selection(raw_value: str | None, init_mode: bool) -> list[str]:
    if not raw_value:
        return []

    requested = [stage.strip() for stage in raw_value.split(",") if stage.strip()]
    invalid = [stage for stage in requested if stage not in STAGE_NAMES]
    if invalid:
        raise ValueError(
            f"Неизвестные стадии: {', '.join(invalid)}. Допустимо: {', '.join(STAGE_NAMES)}"
        )

    planned = list(requested)
    if "restore" in planned and "extract" not in planned:
        planned.insert(0, "extract")
    if "load" in planned and "compare" not in planned:
        compare_index = planned.index("load")
        planned.insert(compare_index, "compare")

    seen: set[str] = set()
    ordered: list[str] = []
    for stage in planned:
        if stage in seen:
            continue
        seen.add(stage)
        ordered.append(stage)
    return ordered


def default_stages(init_mode: bool, skip_weekly: bool) -> list[str]:
    stages = ["extract", "restore"]
    if init_mode:
        stages.append("seed_asterisk")
    else:
        stages.extend(["compare", "load", "seed_asterisk"])
        if not skip_weekly:
            stages.append("weekly")
    stages.append("cleanup")
    return stages


def resolve_db_scope(init_mode: bool, db_scope: str) -> str:
    if db_scope in {"main", "temp"}:
        return db_scope
    return "main" if init_mode else "temp"


def build_runner(
    settings: PipelineSettings,
    logger: ETLLogger,
    db_ops: DatabaseOperations,
    init_mode: bool,
    stages: list[str],
    db_scope: str,
    skip_weekly: bool = False,
) -> TaskRunner:
    runner = TaskRunner(settings, logger, db_ops)
    effective_scope = resolve_db_scope(init_mode, db_scope)

    for stage in stages:
        if stage == "extract":
            runner.add_task(ExtractTask(settings, logger, db_ops))
        elif stage == "restore":
            runner.add_task(RestoreTask(settings, logger, db_ops, target=effective_scope))
        elif stage == "seed_asterisk":
            runner.add_task(
                SeedAsteriskTask(
                    settings,
                    logger,
                    db_ops,
                    db_key="main_db",
                )
            )
        elif stage == "compare":
            runner.add_task(CompareTask(settings, logger, db_ops))
        elif stage == "load":
            runner.add_task(LoadTask(settings, logger, db_ops))
        elif stage == "weekly":
            if skip_weekly:
                logger.info("Weekly task исключен из запуска по флагу --skip-weekly")
            else:
                runner.add_task(WeeklyTask(settings, logger, db_ops))
        elif stage == "cleanup":
            runner.add_task(CleanupTask(settings, logger, db_ops))
    return runner


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Pipeline для PostgreSQL (task-версия)")
    parser.add_argument("dump_file", nargs="?", help="Путь к .tar.gz или .sql дампу")
    parser.add_argument("--init", action="store_true", help="Первичная инициализация основной БД")
    parser.add_argument("--cleanup", action="store_true", help="Удалять staging БД после завершения")
    parser.add_argument("--skip-weekly", action="store_true", help="Не выполнять weekly-пополнение ручных таблиц")
    parser.add_argument(
        "--force-weekly",
        action="store_true",
        help="Выполнить weekly независимо от weekly_days и weekly_enabled",
    )
    parser.add_argument(
        "--tasks",
        help=(
            "Список стадий через запятую: "
            "extract,restore,seed_asterisk,compare,load,weekly,cleanup"
        ),
    )
    parser.add_argument(
        "--db-scope",
        choices=("auto", "main", "temp"),
        default="auto",
        help="Целевая БД для restore; seed_asterisk всегда дополняет main",
    )
    parser.add_argument(
        "--temp-db-name",
        help="Имя существующей staging БД для частичного запуска стадий без restore",
    )
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
    try:
        selected_stages = parse_stage_selection(args.tasks, args.init)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    planned_stages = selected_stages or default_stages(args.init, args.skip_weekly)
    effective_scope = resolve_db_scope(args.init, args.db_scope)
    logger.info(
        f"План стадий task-pipeline: stages={planned_stages}, db_scope={effective_scope}, init={args.init}"
    )
    requires_dump = any(stage in {"extract", "restore"} for stage in planned_stages)
    if requires_dump and not args.dump_file:
        logger.error("Для стадий extract/restore необходимо передать dump_file")
        sys.exit(1)
    if not requires_dump and not args.dump_file:
        logger.info("Запуск без dump_file: будут выполнены только стадии, не требующие дампа")
    if "compare" in planned_stages and effective_scope != "temp":
        logger.error("Стадия compare поддерживается только для temp scope")
        sys.exit(1)
    if "load" in planned_stages and effective_scope != "temp":
        logger.error("Стадия load предполагает snapshot из temp scope")
        sys.exit(1)

    db_ops.cleanup_stale_databases(exclude_names=[args.temp_db_name] if args.temp_db_name else [])
    cleanup_temp_artifacts(settings.temp_dir, logger)

    if (
        requires_dump
        and not args.init
        and args.dump_file
        and not os.path.exists(args.dump_file)
        and settings.remote_user
        and settings.remote_host
        and settings.remote_path
        and settings.backup_tar
    ):
        archive_existing_backup(settings, logger)
        rotate_backups(settings, logger)

    actual_file = None
    if requires_dump:
        actual_file = copy_from_remote_or_local(settings, logger, args.dump_file)
        if not actual_file:
            sys.exit(1)

    temp_db = None
    if effective_scope == "temp":
        temp_db = build_temp_db_name(settings) if "restore" in planned_stages else args.temp_db_name
        if not temp_db and any(stage in {"compare", "load", "cleanup"} for stage in planned_stages):
            logger.error("Для temp-стадий без restore нужно указать --temp-db-name")
            sys.exit(1)

    runner = build_runner(
        settings,
        logger,
        db_ops,
        args.init,
        planned_stages,
        effective_scope,
        skip_weekly=args.skip_weekly,
    )
    runner.update_context(
        {
            "dump_file": actual_file,
            "temp_dir": settings.temp_dir,
            "main_db": settings.db_name,
            "db_name": settings.db_name,
            "temp_db": temp_db,
            "tables": settings.snapshot_tables,
            "cleanup_temp_db": args.cleanup,
            "weekly_enabled": settings.weekly_enabled,
            "weekly_days": settings.weekly_days,
            "force_weekly": args.force_weekly,
            "create_database": args.init or ("restore" in planned_stages and effective_scope == "main"),
        }
    )

    if args.init and "restore" in planned_stages and db_ops.database_exists(settings.db_name):
        logger.error(f"База {settings.db_name} уже существует. Для --init нужна пустая целевая БД.")
        sys.exit(1)

    success = runner.run()
    cleanup_temp_artifacts(settings.temp_dir, logger)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
