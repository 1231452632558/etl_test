#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача очистки временных ресурсов
Удаляет временную базу данных и временные файлы
"""

import shutil
from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult


class CleanupTask(BaseTask):
    """Задача очистки временных ресурсов"""
    
    @property
    def name(self) -> str:
        return "cleanup"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Очистка временной базы данных и файлов
        
        Args:
            context: Контекст с ключами:
                - 'temp_db': имя временной базы (опционально)
                - 'extract_dir': путь к директории распаковки (опционально)
                - 'cleanup_temp_db': флаг очистки БД (по умолчанию True)
                
        Returns:
            TaskResult с результатом очистки
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Очистка временных ресурсов ===")
        
        try:
            temp_db = context.get('temp_db')
            extract_dir = context.get('extract_dir')
            export_dir = context.get('export_dir')
            cleanup_temp_db = context.get('cleanup_temp_db', True)
            
            cleaned_items = []
            warnings = []
            
            # Очищаем временную базу данных
            if cleanup_temp_db and temp_db:
                self.logger.info(f"Удаление временной базы {temp_db}...")
                if self.db_ops.drop_database(temp_db):
                    cleaned_items.append(f"database:{temp_db}")
                    self.logger.info(f"База {temp_db} удалена")
                else:
                    warnings.append(f"Не удалось удалить базу {temp_db}")
            
            # Очищаем временные директории
            for directory in (extract_dir, export_dir):
                if not directory:
                    continue
                self.logger.info(f"Удаление директории {directory}...")
                try:
                    shutil.rmtree(directory)
                    cleaned_items.append(f"directory:{directory}")
                    self.logger.info(f"Директория {directory} удалена")
                except Exception as e:
                    warnings.append(f"Ошибка удаления директории: {str(e)}")
                    self.logger.warning(f"Ошибка удаления {directory}: {e}")
            
            return self._create_result(
                success=True,
                message=f"Очистка завершена: {len(cleaned_items)} объектов",
                data={
                    'cleaned_items': cleaned_items,
                    'warnings_count': len(warnings)
                },
                warnings=warnings,
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при очистке: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка очистки: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
