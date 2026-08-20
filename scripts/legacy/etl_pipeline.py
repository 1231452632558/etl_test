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
    archive_existing_backup,
    build_snapshot_exports,
    cleanup_temp_artifacts,
    copy_from_remote_or_local,
    extract_dump,
    log_git_revision,
    rotate_backups,
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


def parse_stage_selection(raw_value: str | None) -> list[str]:
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
        planned.insert(planned.index("load"), "compare")

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


class ETLPipeline:
    """Монолитный nightly pipeline без ежедневного удаления основной базы."""

    def __init__(self, config_path: str):
        self.settings = PipelineSettings.from_file(config_path)
        self.logger = ETLLogger(self.settings.log_file, self.settings.log_level)
        log_git_revision(self.logger)
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

    def _seed_asterisk(self, db_name: str) -> bool:
        self.db_ops.create_required_tables(db_name)
        result = self.db_ops.append_asterisk_cdr_from_csv(db_name)
        self.db_ops.add_project_id_column(db_name)
        self.db_ops.log_table_schema("asterisk_cdr", db_name, include_indexes=True)
        if not result["success"]:
            self.logger.error(
                f"Ошибка append asterisk_cdr в db={db_name}: {result.get('reason', 'unknown')}"
            )
            return False
        self.logger.info(
            f"asterisk_cdr дополнена в db={db_name}: "
            f"source_rows={result.get('source_rows', 0)}, "
            f"inserted_rows={result.get('inserted_rows', 0)}, "
            f"skipped={result.get('skipped', False)}"
        )
        return True

    def _run_weekly(self, db_name: str, force_weekly: bool = False) -> bool:
        if not self.settings.weekly_enabled and not force_weekly:
            self.logger.info("Weekly insert отключен параметром weekly_enabled=false")
            return True

        result = self.db_ops.add_weekly_records(
            db_name,
            self.settings.weekly_days,
            force=force_weekly,
        )
        if result["skipped"]:
            self.logger.info(
                f"Weekly insert пропущен: сегодня {result['current_day']}, "
                f"дни запуска {result['target_days']}"
            )
            return True

        group_failures = result.get("group_failures", [])
        users_failures = result.get("users_failures", [])
        self.logger.info(
            "Weekly insert обработан: "
            f"snapshot_date={result.get('snapshot_date', '')}, "
            f"target_days={result.get('target_days', [])}, "
            f"force={result.get('force', False)}, "
            f"group_upserted={result.get('group_upserted', 0)}, "
            f"group_inserted={result.get('group_inserted', 0)}, "
            f"users_upserted={result.get('users_upserted', 0)}, "
            f"users_inserted={result.get('users_inserted', 0)}"
        )
        if group_failures or users_failures:
            self.logger.error(
                f"Weekly завершился с ошибками: group_failures={group_failures}, "
                f"users_failures={users_failures}"
            )
            return False
        return True

    def run_init(self, dump_file: str) -> bool:
        self.logger.info("=== ЗАПУСК ИНИЦИАЛИЗАЦИИ ===")
        cleanup_temp_artifacts(self.settings.temp_dir, self.logger)
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
            self.db_ops.seed_manual_tables_if_needed(main_db, self.settings.manual_seed_files)
            if not self._seed_asterisk(main_db):
                return False

            self.db_ops.grant_privileges(main_db)
            self.logger.info("=== ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО ===")
            return True
        finally:
            if extract_dir:
                shutil.rmtree(extract_dir, ignore_errors=True)

    def run_nightly(
        self,
        dump_file: str,
        cleanup: bool = True,
        skip_weekly: bool = False,
        force_weekly: bool = False,
    ) -> bool:
        self.logger.info("=== ЗАПУСК НОЧНОЙ ОБРАБОТКИ ===")
        self.logger.info("Основная БД не пересоздается; nightly-дамп разворачивается только во временную staging БД.")
        cleanup_temp_artifacts(self.settings.temp_dir, self.logger)

        if (
            not os.path.exists(dump_file)
            and self.settings.remote_user
            and self.settings.remote_host
            and self.settings.remote_path
            and self.settings.backup_tar
        ):
            archive_existing_backup(self.settings, self.logger)
            rotate_backups(self.settings, self.logger)

        actual_file = copy_from_remote_or_local(self.settings, self.logger, dump_file)
        if not actual_file:
            return False

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
            if not self._seed_asterisk(main_db):
                return False
            if skip_weekly:
                self.logger.info("Weekly insert пропущен по флагу --skip-weekly")
            elif not self._run_weekly(main_db, force_weekly=force_weekly):
                return False

            self.db_ops.grant_privileges(main_db)
            self.logger.info("=== НОЧНАЯ ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО ===")
            return True
        finally:
            self._cleanup(temp_db, extract_dir, export_dir, cleanup)
            cleanup_temp_artifacts(self.settings.temp_dir, self.logger)

    def run(
        self,
        dump_file: str,
        init_mode: bool = False,
        cleanup: bool = True,
        skip_weekly: bool = False,
        force_weekly: bool = False,
    ) -> bool:
        self.logger.info(f"Дата запуска: {datetime.now()}")
        self.logger.info(f"Пользователь запуска: {os.getenv('USER', 'unknown')}")
        self.logger.info(f"Хост: {os.uname().nodename}")
        if init_mode:
            return self.run_init(dump_file)
        return self.run_nightly(
            dump_file,
            cleanup,
            skip_weekly=skip_weekly,
            force_weekly=force_weekly,
        )

    def run_selected(
        self,
        stages: list[str],
        dump_file: str | None,
        init_mode: bool = False,
        cleanup: bool = True,
        skip_weekly: bool = False,
        force_weekly: bool = False,
        db_scope: str = "auto",
        temp_db_name: str | None = None,
    ) -> bool:
        effective_scope = resolve_db_scope(init_mode, db_scope)
        requires_dump = any(stage in {"extract", "restore"} for stage in stages)
        if requires_dump and not dump_file:
            self.logger.error("Для стадий extract/restore необходимо передать dump_file")
            return False
        if "compare" in stages and effective_scope != "temp":
            self.logger.error("Стадия compare поддерживается только для temp scope")
            return False
        if "load" in stages and effective_scope != "temp":
            self.logger.error("Стадия load предполагает snapshot из temp scope")
            return False

        cleanup_temp_artifacts(self.settings.temp_dir, self.logger)
        if (
            requires_dump
            and not init_mode
            and dump_file
            and not os.path.exists(dump_file)
            and self.settings.remote_user
            and self.settings.remote_host
            and self.settings.remote_path
            and self.settings.backup_tar
        ):
            archive_existing_backup(self.settings, self.logger)
            rotate_backups(self.settings, self.logger)

        actual_file = None
        extract_dir = None
        export_dir = None
        modifications: list[dict] = []
        sql_files: list[str] = []
        temp_db = temp_db_name
        main_db = self.settings.db_name

        self.logger.info(
            f"План стадий monolith-pipeline: stages={stages}, db_scope={effective_scope}, init={init_mode}"
        )

        try:
            if requires_dump:
                actual_file = copy_from_remote_or_local(self.settings, self.logger, dump_file)
                if not actual_file:
                    return False

            if "extract" in stages:
                extract_dir, sql_files = extract_dump(actual_file, self.settings.temp_dir, self.logger)
                if not sql_files:
                    self.logger.error("SQL файлы не найдены")
                    return False

            if effective_scope == "temp" and "restore" in stages:
                temp_db = self._build_temp_db_name()
            if effective_scope == "temp" and not temp_db and any(stage in {"compare", "load", "cleanup"} for stage in stages):
                self.logger.error("Для temp-стадий без restore нужно указать --temp-db-name")
                return False

            for stage in stages:
                if stage == "extract":
                    continue
                if stage == "restore":
                    target_db = main_db if effective_scope == "main" else temp_db
                    if init_mode and effective_scope == "main" and self.db_ops.database_exists(main_db):
                        self.logger.error(
                            f"База {main_db} уже существует. Для инициализации нужна пустая цель."
                        )
                        return False
                    recreate = effective_scope == "temp"
                    if not self.db_ops.create_database(target_db, recreate=recreate):
                        return False
                    if sql_files and not self.db_ops.restore_dump(target_db, sql_files):
                        return False
                    self.db_ops.create_required_tables(target_db)
                    if effective_scope == "main":
                        self.db_ops.seed_manual_tables_if_needed(target_db, self.settings.manual_seed_files)
                elif stage == "seed_asterisk":
                    if not self._seed_asterisk(main_db):
                        return False
                elif stage == "compare":
                    export_dir = os.path.join(
                        self.settings.temp_dir,
                        f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    )
                    modifications = build_snapshot_exports(
                        self.db_ops,
                        temp_db,
                        self.settings.snapshot_tables,
                        export_dir,
                    )
                    for mod in modifications:
                        self.logger.info(
                            f"Подготовлен snapshot {mod['table_name']}: {mod['csv_file']} ({mod['row_count']} rows)"
                        )
                elif stage == "load":
                    if not export_dir:
                        export_dir = os.path.join(
                            self.settings.temp_dir,
                            f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
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
                elif stage == "weekly":
                    if skip_weekly:
                        self.logger.info("Weekly insert пропущен по флагу --skip-weekly")
                        continue
                    if not self._run_weekly(main_db, force_weekly=force_weekly):
                        return False
                elif stage == "cleanup":
                    self._cleanup(temp_db if effective_scope == "temp" else None, extract_dir, export_dir, cleanup)
                    extract_dir = None
                    export_dir = None
                    temp_db = None if effective_scope == "temp" else temp_db

            if self.db_ops.database_exists(main_db):
                self.db_ops.grant_privileges(main_db)
            cleanup_temp_artifacts(self.settings.temp_dir, self.logger)
            return True
        finally:
            if "cleanup" not in stages:
                self._cleanup(temp_db if effective_scope == "temp" else None, extract_dir, export_dir, cleanup)
                cleanup_temp_artifacts(self.settings.temp_dir, self.logger)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline для nightly SQL-дампов PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
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
        help="Путь к конфигурации",
    )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Ошибка: Конфигурационный файл не найден: {args.config}")
        sys.exit(1)

    pipeline = ETLPipeline(args.config)
    try:
        selected_stages = parse_stage_selection(args.tasks)
    except ValueError as exc:
        print(f"Ошибка: {exc}")
        sys.exit(1)

    pipeline.db_ops.cleanup_stale_databases(
        exclude_names=[args.temp_db_name] if args.temp_db_name else []
    )

    if selected_stages:
        success = pipeline.run_selected(
            selected_stages,
            args.dump_file,
            init_mode=args.init,
            cleanup=args.cleanup,
            skip_weekly=args.skip_weekly,
            force_weekly=args.force_weekly,
            db_scope=args.db_scope,
            temp_db_name=args.temp_db_name,
        )
    else:
        success = pipeline.run(
            args.dump_file,
            init_mode=args.init,
            cleanup=args.cleanup,
            skip_weekly=args.skip_weekly,
            force_weekly=args.force_weekly,
        )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
