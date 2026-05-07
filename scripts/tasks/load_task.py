#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача загрузки изменений в основную базу данных
Применяет UPSERT для новых и измененных строк
"""

import os
from datetime import datetime
from typing import Dict, Any, List

from .base import BaseTask, TaskResult


class LoadTask(BaseTask):
    """Задача применения изменений в основную базу"""
    
    @property
    def name(self) -> str:
        return "load"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Применение изменений (UPSERT) в основную базу
        
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
            
            applied_count = 0
            errors = []
            
            # Применяем каждую модификацию
            for mod in modifications:
                change_type = mod.get('change_type')
                table_name = mod.get('table_name')
                primary_key = mod.get('primary_key')
                csv_file = mod.get('csv_file')
                
                if not csv_file or not os.path.exists(csv_file):
                    self.logger.warning(f"CSV файл не найден: {csv_file}")
                    errors.append(f"Файл не найден: {csv_file}")
                    continue
                
                self.logger.info(
                    f"Применение {change_type} строк в {table_name}..."
                )
                
                if self.db_ops.upsert_from_csv(
                    table_name, primary_key, csv_file, db_name
                ):
                    applied_count += 1
                    self.logger.info(
                        f"Успешно применено: {change_type} строки в {table_name}"
                    )
                else:
                    error_msg = f"Ошибка UPSERT в {table_name}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
            
            success = len(errors) == 0

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
