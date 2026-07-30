#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Отдельная задача ручной загрузки asterisk_cdr из CSV.
"""

from datetime import datetime
from typing import Any, Dict

from .base import BaseTask, TaskResult


class SeedAsteriskTask(BaseTask):
    """Добавляет отсутствующие строки asterisk_cdr напрямую в main БД."""

    def __init__(self, config, logger, db_ops, db_key: str):
        super().__init__(config, logger, db_ops)
        self.db_key = db_key

    @property
    def name(self) -> str:
        return "seed_asterisk"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started_at = datetime.now()
        self.logger.info("=== Задача: Загрузка asterisk_cdr ===")

        try:
            db_name = context.get(self.db_key)
            if not db_name:
                return self._create_result(
                    success=False,
                    message=f"Имя базы данных не указано в контексте: {self.db_key}",
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            self.db_ops.create_required_tables(db_name)
            append_result = self.db_ops.append_asterisk_cdr_from_csv(db_name)
            self.db_ops.add_project_id_column(db_name)
            self.db_ops.log_table_schema("asterisk_cdr", db_name, include_indexes=True)
            if not append_result["success"]:
                reason = str(append_result.get("reason", "unknown"))
                return self._create_result(
                    success=False,
                    message=f"Ошибка append asterisk_cdr в db={db_name}: {reason}",
                    data={"db_name": db_name, **append_result},
                    errors=[reason],
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            return self._create_result(
                success=True,
                message=(
                    f"asterisk_cdr дополнена в db={db_name}: "
                    f"source_rows={append_result.get('source_rows', 0)}, "
                    f"inserted_rows={append_result.get('inserted_rows', 0)}, "
                    f"skipped={append_result.get('skipped', False)}"
                ),
                data={
                    "db_name": db_name,
                    **append_result,
                },
                started_at=started_at,
                completed_at=datetime.now(),
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при загрузке asterisk_cdr: {exc}")
            return self._create_result(
                success=False,
                message=f"Ошибка seed asterisk: {exc}",
                errors=[str(exc)],
                started_at=started_at,
                completed_at=datetime.now(),
            )
