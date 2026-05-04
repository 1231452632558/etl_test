#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Таск материализации custom_values в колонки таблицы issues.
"""

from datetime import datetime
from typing import Any, Dict

from .base import BaseTask, TaskResult


class TransformCustomValuesTask(BaseTask):
    """Трансформация custom values в staging или main БД."""

    def __init__(self, config, logger, db_ops, db_key: str):
        super().__init__(config, logger, db_ops)
        self.db_key = db_key

    @property
    def name(self) -> str:
        return "transform_custom_values"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started_at = datetime.now()
        self.logger.info("=== Задача: Трансформация Custom Values ===")

        try:
            db_name = context.get(self.db_key)
            if not db_name:
                return self._create_result(
                    success=False,
                    message=f"Имя базы данных не указано в контексте: {self.db_key}",
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            result = self.db_ops.transform_custom_values(db_name, self.config.issues_table)
            self.logger.info(
                f"Custom transform завершен: fields={result['custom_fields_count']}, "
                f"issues={result['processed_count']}, added_columns={len(result['columns_added'])}"
            )

            return self._create_result(
                success=True,
                message=f"Трансформация завершена, обработано issues: {result['processed_count']}",
                data={
                    'custom_transform': result,
                    'db_name': db_name,
                },
                started_at=started_at,
                completed_at=datetime.now(),
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при трансформации custom values: {exc}")
            return self._create_result(
                success=False,
                message=f"Ошибка трансформации: {exc}",
                errors=[str(exc)],
                started_at=started_at,
                completed_at=datetime.now(),
            )
