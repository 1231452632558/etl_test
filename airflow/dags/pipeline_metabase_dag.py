"""Ночной Pipeline Metabase, реализованный как нативный Airflow DAG.

Каждая стадия ETL является отдельной Airflow task. Текущий CLI-пайплайн из
``scripts/etl_pipeline.py`` намеренно не вызывается и не меняется. DAG повторно
использует общий PostgreSQL-слой из ``scripts/pipeline_common.py``.
"""

from __future__ import annotations

import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "pipeline_metabase_nightly"
PROJECT_DIR = Path(
    os.environ.get("PIPELINE_METABASE_PROJECT_DIR", "/opt/airflow/pipeline-metabase")
)
CONFIG_FILE = os.environ.get(
    "PIPELINE_METABASE_CONFIG_FILE",
    "/opt/airflow/runtime/etl_config.ini",
)


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Отдельное получение CSV не равно загрузке строк в main БД. По умолчанию
# сохраняем append-only загрузку, пока внешний DAG не отвечает за CSV -> main.
LOAD_ASTERISK_CDR = _env_flag("PIPELINE_METABASE_LOAD_ASTERISK_CDR", True)


def _runtime() -> tuple[Any, Any, Any]:
    """Создать независимое runtime-окружение для одного Airflow task."""

    scripts_dir = str(PROJECT_DIR / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from pipeline_common import DatabaseOperations, ETLLogger, PipelineSettings

    if not os.path.exists(CONFIG_FILE):
        raise AirflowFailException(f"Конфигурационный файл не найден: {CONFIG_FILE}")
    settings = PipelineSettings.from_file(CONFIG_FILE)
    logger = ETLLogger(settings.log_file, settings.log_level)
    return settings, logger, DatabaseOperations(settings, logger)


def _task_error(task_name: str, message: str, errors: list[str] | None = None) -> None:
    details = "; ".join(errors or [])
    suffix = f"; детали: {details}" if details else ""
    raise AirflowFailException(f"{task_name}: {message}{suffix}")


with DAG(
    dag_id=DAG_ID,
    description="Ночная staging -> main синхронизация PostgreSQL для Metabase",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["etl", "postgresql", "metabase"],
) as dag:

    @task
    def prepare_run_context(**context: Any) -> dict[str, str]:
        """Очистить старые staging-ресурсы и выбрать БД для текущего DagRun."""

        settings, logger, db_ops = _runtime()
        from pipeline_common import cleanup_temp_artifacts, log_git_revision

        logical_date = context["logical_date"]
        temp_db = f"{settings.temp_db_prefix}{logical_date.strftime('%Y%m%d_%H%M%S')}"
        try:
            db_ops.cleanup_stale_databases(exclude_names=[temp_db])
            cleanup_temp_artifacts(settings.temp_dir, logger)
            log_git_revision(logger)
        except Exception as exc:
            _task_error("prepare_run_context", str(exc))
        return {"main_db": settings.db_name, "temp_db": temp_db}

    @task
    def download_remote_dump(run_context: dict[str, str]) -> dict[str, str]:
        """Ротировать прежний архив и скачать новый dump из [remote]."""

        settings, logger, _ = _runtime()
        from pipeline_common import archive_existing_backup, copy_from_remote_or_local, rotate_backups

        if not all(
            (settings.remote_user, settings.remote_host, settings.remote_path, settings.backup_tar)
        ):
            _task_error(
                "download_remote_dump",
                "для Airflow DAG должна быть заполнена секция [remote] в etl_config.ini",
            )
        archive_existing_backup(settings, logger)
        rotate_backups(settings, logger)
        dump_file = copy_from_remote_or_local(settings, logger, "")
        if not dump_file:
            _task_error("download_remote_dump", "не удалось скачать свежий dump")
        return {**run_context, "dump_file": dump_file}

    @task
    def extract_dump_files(run_context: dict[str, str]) -> dict[str, Any]:
        """Извлечь SQL-файлы из полученного dump."""

        settings, logger, _ = _runtime()
        from pipeline_common import extract_dump

        extract_dir, sql_files = extract_dump(
            run_context["dump_file"], settings.temp_dir, logger
        )
        if not sql_files:
            _task_error("extract_dump_files", "в dump не найдены SQL-файлы")
        return {**run_context, "extract_dir": extract_dir or "", "sql_files": list(sql_files)}

    @task
    def restore_staging(run_context: dict[str, Any]) -> dict[str, Any]:
        """Восстановить dump только в новую staging-БД."""

        settings, _, db_ops = _runtime()
        temp_db = run_context["temp_db"]
        if not db_ops.create_database(temp_db, recreate=True):
            _task_error("restore_staging", f"не удалось создать staging БД {temp_db}")
        if not db_ops.restore_dump(temp_db, run_context["sql_files"]):
            _task_error("restore_staging", f"не удалось восстановить dump в {temp_db}")
        db_ops.create_required_tables(temp_db)
        db_ops.log_pipeline_schema_snapshot(temp_db, settings.issues_table, settings.projects_table)
        db_ops.grant_privileges(temp_db)
        return run_context

    @task
    def export_snapshot_csv(run_context: dict[str, Any]) -> dict[str, Any]:
        """Экспортировать staging snapshot в CSV для идемпотентного UPSERT."""

        settings, logger, db_ops = _runtime()
        from pipeline_common import build_snapshot_exports

        export_dir = os.path.join(
            settings.temp_dir,
            f"snapshot_{run_context['temp_db'].removeprefix(settings.temp_db_prefix)}",
        )
        try:
            modifications = build_snapshot_exports(
                db_ops, run_context["temp_db"], settings.snapshot_tables, export_dir
            )
        except Exception as exc:
            _task_error("export_snapshot_csv", str(exc))
        if not modifications:
            _task_error("export_snapshot_csv", "snapshot не содержит таблиц для загрузки")
        for item in modifications:
            logger.info(
                "Snapshot export: table=%s, csv=%s, rows=%s"
                % (item["table_name"], item["csv_file"], item["row_count"])
            )
        return {**run_context, "export_dir": export_dir, "modifications": modifications}

    @task
    def load_snapshot_to_main(run_context: dict[str, Any]) -> dict[str, int]:
        """Выполнить UPSERT CSV snapshot в стабильную main-БД."""

        _, logger, db_ops = _runtime()
        applied_count = 0
        errors: list[str] = []
        for item in run_context["modifications"]:
            table_name = item["table_name"]
            csv_file = item["csv_file"]
            if not os.path.exists(csv_file):
                errors.append(f"файл не найден: {csv_file}")
                continue
            if db_ops.upsert_from_csv(
                table_name, item["primary_key"], csv_file, run_context["main_db"]
            ):
                applied_count += 1
                logger.info(f"Snapshot UPSERT выполнен: table={table_name}")
            else:
                errors.append(f"ошибка UPSERT: {table_name}")
        if errors:
            _task_error("load_snapshot_to_main", "часть snapshot не загружена", errors)
        return {"applied_count": applied_count}

    @task
    def load_asterisk_cdr(
        run_context: dict[str, Any], load_summary: dict[str, int]
    ) -> dict[str, Any]:
        """Догрузить CSV только когда внешний DAG не берёт это на себя."""

        del load_summary
        if not LOAD_ASTERISK_CDR:
            return {"skipped": True, "reason": "managed_by_external_asterisk_dag"}
        _, _, db_ops = _runtime()
        db_name = run_context["main_db"]
        db_ops.create_required_tables(db_name)
        result = db_ops.append_asterisk_cdr_from_csv(db_name)
        db_ops.add_project_id_column(db_name)
        db_ops.log_table_schema("asterisk_cdr", db_name, include_indexes=True)
        if not result["success"]:
            _task_error("load_asterisk_cdr", str(result.get("reason", "unknown")))
        return result

    @task
    def update_weekly_tables(
        run_context: dict[str, Any], asterisk_summary: dict[str, Any]
    ) -> dict[str, Any]:
        """Идемпотентно обновить group_employee_count и users_active."""

        del asterisk_summary
        settings, logger, db_ops = _runtime()
        if not settings.weekly_enabled:
            return {"skipped": True, "reason": "disabled"}
        result = db_ops.add_weekly_records(
            run_context["main_db"], settings.weekly_days, force=False
        )
        errors = list(result.get("group_failures", [])) + list(
            result.get("users_failures", [])
        )
        if errors:
            _task_error("update_weekly_tables", "weekly upsert завершился ошибкой", errors)
        logger.info("Weekly result: %s" % result)
        return result

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def cleanup_successful_run(
        run_context: dict[str, Any], weekly_summary: dict[str, Any]
    ) -> dict[str, Any]:
        """Удалить staging и временные файлы только после полного успеха DAG."""

        del weekly_summary
        settings, logger, db_ops = _runtime()
        from pipeline_common import cleanup_temp_artifacts

        cleaned: list[str] = []
        if db_ops.drop_database(run_context["temp_db"]):
            cleaned.append(f"database:{run_context['temp_db']}")
        for directory in (run_context.get("extract_dir", ""), run_context.get("export_dir", "")):
            if not directory:
                continue
            try:
                shutil.rmtree(directory)
                cleaned.append(f"directory:{directory}")
            except FileNotFoundError:
                pass
            except OSError as exc:
                _task_error("cleanup_successful_run", f"не удалось удалить {directory}: {exc}")
        cleanup_temp_artifacts(settings.temp_dir, logger)
        return {"cleaned": cleaned}

    prepared = prepare_run_context()
    downloaded = download_remote_dump(prepared)
    extracted = extract_dump_files(downloaded)
    restored = restore_staging(extracted)
    exported = export_snapshot_csv(restored)
    loaded = load_snapshot_to_main(exported)
    asterisk = load_asterisk_cdr(exported, loaded)
    weekly = update_weekly_tables(exported, asterisk)
    cleanup_successful_run(exported, weekly)
