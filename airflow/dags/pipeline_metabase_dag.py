"""Airflow DAG для ежедневного запуска Pipeline Metabase.

Бизнес-логика остается в scripts/etl_pipeline.py. DAG отвечает только за
расписание, взаимное исключение запусков, повторную попытку и журнал Airflow.
"""

from __future__ import annotations

import os
import shlex
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


DAG_ID = "pipeline_metabase_nightly"
PROJECT_DIR = os.environ.get(
    "PIPELINE_METABASE_PROJECT_DIR",
    "/opt/airflow/pipeline-metabase",
)
CONFIG_FILE = os.environ.get(
    "PIPELINE_METABASE_CONFIG_FILE",
    "/opt/airflow/runtime/etl_config.ini",
)
PYTHON_BIN = os.environ.get("PIPELINE_METABASE_PYTHON", "python3")


def shell_join(parts: list[str]) -> str:
    """Собрать команду без небезопасной интерполяции значений окружения."""

    return " ".join(shlex.quote(part) for part in parts)


pipeline_command = shell_join(
    [
        PYTHON_BIN,
        os.path.join(PROJECT_DIR, "scripts", "etl_pipeline.py"),
        "--config",
        CONFIG_FILE,
        "--cleanup",
    ]
)


with DAG(
    dag_id=DAG_ID,
    description="Ночная синхронизация PostgreSQL staging -> stable main для Metabase",
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
    run_pipeline = BashOperator(
        task_id="run_incremental_pipeline",
        bash_command=f"set -euo pipefail; {pipeline_command}",
        cwd=PROJECT_DIR,
        append_env=True,
        execution_timeout=timedelta(hours=6),
    )
