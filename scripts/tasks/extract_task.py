#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача извлечения данных из архива дампа
"""

import os
from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult
from pipeline_common import extract_dump


class ExtractTask(BaseTask):
    """Задача распаковки SQL-дампа из архива"""
    
    @property
    def name(self) -> str:
        return "extract"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Распаковка tar.gz архива с дампом
        
        Args:
            context: Контекст с ключом 'dump_file' - путь к архиву
            
        Returns:
            TaskResult с путями к извлеченным SQL файлам
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Извлечение данных из архива ===")
        
        try:
            dump_file = context.get('dump_file')
            if not dump_file:
                return self._create_result(
                    success=False,
                    message="Путь к файлу дампа не указан",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            if not os.path.exists(dump_file):
                return self._create_result(
                    success=False,
                    message=f"Файл дампа не найден: {dump_file}",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            temp_dir = context.get('temp_dir', self.config.temp_dir)
            self.logger.info(f"Подготовка дампа: {dump_file}")
            extract_dir, sql_files = extract_dump(dump_file, temp_dir, self.logger)
            if not sql_files:
                self.logger.warning("SQL файлы не найдены в дампе")
            
            self.logger.info(f"Архив успешно распакован в {extract_dir}")
            self.logger.info(f"Найдено SQL файлов: {len(sql_files)}")
            
            result_data = {
                'extract_dir': extract_dir,
                'sql_files': sql_files
            }
            
            return self._create_result(
                success=True,
                message=f"Подготовлено SQL файлов: {len(sql_files)}",
                data=result_data,
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при распаковке архива: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка распаковки: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
