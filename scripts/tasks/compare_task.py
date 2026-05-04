#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Подготовка snapshot-экспортов из staging БД для последующего UPSERT в main.
"""

import os
from datetime import datetime
from typing import Any, Dict

from .base import BaseTask, TaskResult
from pipeline_common import build_snapshot_exports


class CompareTask(BaseTask):
    """Формирует набор CSV-снимков таблиц из staging БД."""

    @property
    def name(self) -> str:
        return "compare"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started_at = datetime.now()
        self.logger.info("=== Задача: Подготовка snapshot-экспортов ===")

        try:
            temp_db = context.get('temp_db')
            tables = context.get('tables', {})
            if not temp_db:
                return self._create_result(
                    success=False,
                    message="Не указана временная база данных",
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            export_dir = os.path.join(
                self.config.temp_dir,
                f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            )
            modifications = build_snapshot_exports(self.db_ops, temp_db, tables, export_dir)

            for mod in modifications:
                self.logger.info(
                    f"Подготовлен snapshot {mod['table_name']}: {mod['csv_file']} ({mod['row_count']} rows)"
                )

            return self._create_result(
                success=True,
                message=f"Подготовлено snapshot-файлов: {len(modifications)}",
                data={
                    'modifications': modifications,
                    'export_dir': export_dir,
                },
                started_at=started_at,
                completed_at=datetime.now(),
            )
        except Exception as exc:
            self.logger.error(f"Ошибка подготовки snapshot-экспортов: {exc}")
            return self._create_result(
                success=False,
                message=f"Ошибка compare stage: {exc}",
                errors=[str(exc)],
                started_at=started_at,
                completed_at=datetime.now(),
            )
