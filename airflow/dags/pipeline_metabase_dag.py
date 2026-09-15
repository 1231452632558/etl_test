"""Ночной Pipeline Metabase, реализованный как нативный Airflow DAG.

Каждая стадия ETL является отдельной Airflow task. Текущий CLI-пайплайн из
``scripts/etl_pipeline.py`` намеренно не вызывается и не меняется. DAG повторно
использует только общий слой PostgreSQL/FDW из ``scripts/pipeline_common.py``.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import Connection, DAG, task
from airflow.sdk.exceptions import AirflowFailException


DAG_ID = "pipeline_metabase_nightly"
PROJECT_DIR = Path(
    os.environ.get("PIPELINE_METABASE_PROJECT_DIR", "/opt/airflow/pipeline-metabase")
)
CONFIG_FILE = os.environ.get(
    "PIPELINE_METABASE_CONFIG_FILE",
    "/opt/airflow/runtime/etl_config.ini",
)
DB_HOST_CONNECTION_ID = os.environ.get(
    "PIPELINE_METABASE_DB_HOST_CONN_ID",
    "pipeline_metabase_db_host",
)


def _runtime() -> tuple[Any, Any, Any]:
    """Создать независимое runtime-окружение для одного Airflow task."""

    import_paths = [PROJECT_DIR / "scripts", PROJECT_DIR / "airflow" / "runtime"]
    for import_path in import_paths:
        path_text = str(import_path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)
    from pipeline_common import ETLLogger, PipelineSettings
    from remote_postgres import SshPostgresDatabaseOperations, SshPostgresTarget

    if not os.path.exists(CONFIG_FILE):
        raise AirflowFailException(f"Конфигурационный файл не найден: {CONFIG_FILE}")
    settings = PipelineSettings.from_file(CONFIG_FILE)
    logger = ETLLogger(settings.log_file, settings.log_level)
    try:
        connection = Connection.get(DB_HOST_CONNECTION_ID)
        target = SshPostgresTarget.from_airflow_connection(
            connection,
            default_db_socket=settings.db_host,
            default_db_port=settings.db_port,
        )
    except Exception as exc:
        raise AirflowFailException(
            f"Не удалось загрузить SSH Connection {DB_HOST_CONNECTION_ID}: {exc}"
        ) from exc
    # В Airflow db_host — это socket уже на целевом сервере. Сам worker до БД
    # не подключается: все psql-команды выполняются через SSH adapter.
    settings.db_host = target.db_socket
    settings.db_port = target.db_port
    return settings, logger, SshPostgresDatabaseOperations(settings, logger, target)


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
        """Проверить доступ к БД и подготовить изолированную staging-БД."""

        settings, logger, db_ops = _runtime()
        from pipeline_common import cleanup_temp_artifacts, log_git_revision

        log_git_revision(logger)
        # Имя соответствует шаблону основного pipeline и стабильно при retry
        # одного DagRun, поэтому штатная очистка умеет найти его позднее.
        dag_run = context["dag_run"]
        logical_date = dag_run.logical_date
        if logical_date is not None:
            timestamp = logical_date.strftime("%Y%m%d_%H%M%S")
        else:
            # В Airflow 3 ручной/API-запуск может не иметь logical_date.
            # Детерминированные 14 цифр сохраняют имя при retry и соответствуют
            # шаблону очистки staging-БД в pipeline_common.py.
            digest = int(hashlib.sha256(dag_run.run_id.encode("utf-8")).hexdigest(), 16)
            compact = f"{digest % (10**14):014d}"
            timestamp = f"{compact[:8]}_{compact[8:]}"
        temp_db = f"{settings.temp_db_prefix}{timestamp}"
        try:
            db_ops.cleanup_stale_databases(exclude_names=[temp_db])
            cleanup_temp_artifacts(settings.temp_dir, logger)
        except Exception as exc:
            _task_error("prepare_run_context", str(exc))
        return {"main_db": settings.db_name, "temp_db": temp_db}

    @task
    def download_and_extract(run_context: dict[str, str]) -> dict[str, Any]:
        """Скачать свежий dump и извлечь SQL-файлы в рабочий каталог запуска."""

        settings, logger, _ = _runtime()
        from pipeline_common import (
            copy_from_remote_or_local,
            extract_dump,
            remote_backup_configured,
        )
        if not remote_backup_configured(settings):
            _task_error(
                "download_and_extract",
                "для Airflow DAG должна быть заполнена секция [remote] в etl_config.ini",
            )
        dump_file = copy_from_remote_or_local(settings, logger, None, force_local=False)
        if not dump_file:
            _task_error("download_and_extract", "не удалось скачать свежий dump")
        extract_dir, sql_files = extract_dump(dump_file, settings.temp_dir, logger)
        if not sql_files:
            _task_error("download_and_extract", "в dump не найдены SQL-файлы")
        return {
            "dump_file": dump_file,
            "extract_dir": extract_dir or "",
            "sql_files": list(sql_files),
            "temp_db": run_context["temp_db"],
        }

    @task
    def restore_staging(
        run_context: dict[str, str], extracted: dict[str, Any]
    ) -> dict[str, str]:
        """Восстановить dump только в новую staging-БД."""

        settings, logger, db_ops = _runtime()
        temp_db = run_context["temp_db"]
        if not db_ops.create_database(temp_db, recreate=True):
            _task_error("restore_staging", f"не удалось создать staging БД {temp_db}")
        if not db_ops.restore_dump(temp_db, extracted["sql_files"]):
            _task_error("restore_staging", f"не удалось восстановить dump в {temp_db}")
        if not db_ops.create_required_tables(temp_db):
            _task_error("restore_staging", f"не удалось создать служебные таблицы в {temp_db}")
        db_ops.log_pipeline_schema_snapshot(temp_db, settings.issues_table, settings.projects_table)
        if not db_ops.grant_privileges(temp_db):
            _task_error("restore_staging", f"не удалось выдать права в {temp_db}")
        return {**run_context, "extract_dir": extracted["extract_dir"]}

    @task
    def build_fdw_snapshot_plan(run_context: dict[str, str]) -> list[dict[str, Any]]:
        """Определить таблицы, ключи и режимы FDW load для текущего staging."""

        settings, logger, db_ops = _runtime()
        from pipeline_common import build_snapshot_plan

        try:
            plan = build_snapshot_plan(db_ops, run_context["temp_db"], settings.snapshot_tables)
        except Exception as exc:
            _task_error("build_fdw_snapshot_plan", str(exc))
        for item in plan:
            logger.info(
                "FDW plan: table=%s, mode=%s, key=%s"
                % (item["table_name"], item["load_mode"], item["primary_key"])
            )
        return plan

    @task
    def load_snapshot_to_main(
        run_context: dict[str, str], plan: list[dict[str, Any]]
    ) -> dict[str, int]:
        """Выполнить идемпотентный staging -> main load через postgres_fdw."""

        _, _, db_ops = _runtime()
        success, applied_count, errors = db_ops.apply_snapshot_via_fdw(
            plan,
            run_context["temp_db"],
            run_context["main_db"],
        )
        if not success:
            _task_error("load_snapshot_to_main", "FDW load завершился ошибкой", errors)
        return {"applied_count": applied_count}

    @task
    def update_weekly_tables(
        run_context: dict[str, str], load_summary: dict[str, int]
    ) -> dict[str, Any]:
        """Идемпотентно обновить group_employee_count и users_active."""

        del load_summary
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

    @task
    def publish_verified_backup(
        extracted: dict[str, Any], weekly_summary: dict[str, Any]
    ) -> dict[str, bool]:
        """Опубликовать dump в архив только после успешной загрузки main."""

        del weekly_summary
        settings, logger, _ = _runtime()
        from pipeline_common import finalize_remote_backup

        if not finalize_remote_backup(settings, logger, extracted["dump_file"]):
            _task_error("publish_verified_backup", "не удалось ротировать проверенный backup")
        return {"published": True}

    @task
    def cleanup_successful_run(
        run_context: dict[str, str], publish_summary: dict[str, bool]
    ) -> dict[str, Any]:
        """Удалить staging и временные файлы только после полного успеха DAG."""

        del publish_summary
        settings, logger, db_ops = _runtime()
        from pipeline_common import cleanup_temp_artifacts

        cleaned: list[str] = []
        if db_ops.drop_database(run_context["temp_db"]):
            cleaned.append(f"database:{run_context['temp_db']}")
        extract_dir = run_context.get("extract_dir", "")
        if extract_dir:
            try:
                shutil.rmtree(extract_dir)
                cleaned.append(f"directory:{extract_dir}")
            except FileNotFoundError:
                pass
            except OSError as exc:
                _task_error("cleanup_successful_run", f"не удалось удалить {extract_dir}: {exc}")
        cleanup_temp_artifacts(settings.temp_dir, logger)
        return {"cleaned": cleaned}

    run_context = prepare_run_context()
    extracted = download_and_extract(run_context)
    restored = restore_staging(run_context, extracted)
    plan = build_fdw_snapshot_plan(restored)
    loaded = load_snapshot_to_main(restored, plan)
    weekly = update_weekly_tables(restored, loaded)
    published = publish_verified_backup(extracted, weekly)
    cleanup_successful_run(restored, published)
