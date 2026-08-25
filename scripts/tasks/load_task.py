#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача загрузки изменений в основную базу данных
Применяет UPSERT для keyed-таблиц и атомарный REPLACE для явно заданных keyless-таблиц
"""

from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult


class LoadTask(BaseTask):
    """Задача применения snapshot-изменений в основную базу."""
    
    @property
    def name(self) -> str:
        return "load"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Применение изменений (UPSERT/REPLACE) в основную базу
        
        Args:
            context: Контекст с ключами:
                - 'db_name': имя основной базы
                - 'modifications': список модификаций от CompareTask
                
        Returns:
            TaskResult с результатом применения изменений
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Загрузка изменений ===")
        
        try:
            db_name = context.get('main_db') or context.get('db_name')
            modifications = context.get('modifications', [])
            
            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            if not modifications:
                return self._create_result(
                    success=False,
                    message="Список snapshot-модификаций пуст",
                    errors=["Нет данных для синхронизации main БД"],
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            self.logger.info(
                f"Атомарная синхронизация snapshot: "
                f"db={db_name}, tables={len(modifications)}"
            )
            success = self.db_ops.apply_snapshot_batch(modifications, db_name)
            errors = [] if success else [
                "Ошибка атомарной snapshot-синхронизации; изменения main БД откатаны"
            ]
            applied_count = len(modifications) if success else 0

            return self._create_result(
                    success=success,
                    message=f"Применено {applied_count} модификаций",
                data={
                    'applied_count': applied_count,
                    'errors_count': len(errors)
                },
                errors=errors,
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке изменений: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка загрузки: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
