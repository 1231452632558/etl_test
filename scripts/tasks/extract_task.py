#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача извлечения данных из архива дампа
"""

import os
import shutil
import tarfile
from datetime import datetime
from typing import Dict, Any, List, Tuple

from .base import BaseTask, TaskResult


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
            
            # Создаем директорию для распаковки
            temp_dir = context.get('temp_dir', '/tmp/pg_etl_temp')
            extract_dir = os.path.join(
                temp_dir, 
                f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            os.makedirs(extract_dir, exist_ok=True)
            
            # Распаковываем архив
            self.logger.info(f"Распаковка архива: {dump_file}")
            with tarfile.open(dump_file, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            
            # Находим все SQL файлы
            sql_files = []
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    if file.endswith('.sql'):
                        sql_files.append(os.path.join(root, file))
            
            if not sql_files:
                self.logger.warning("SQL файлы не найдены в архиве")
            
            self.logger.info(f"Архив успешно распакован в {extract_dir}")
            self.logger.info(f"Найдено SQL файлов: {len(sql_files)}")
            
            result_data = {
                'extract_dir': extract_dir,
                'sql_files': sql_files
            }
            
            return self._create_result(
                success=True,
                message=f"Извлечено {len(sql_files)} SQL файлов",
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
