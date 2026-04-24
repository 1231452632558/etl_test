#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача сравнения данных между временной и основной базой
Поиск новых и измененных строк
"""

import os
import shutil
import tempfile
from datetime import datetime
from typing import Dict, Any, List, Tuple

from .base import BaseTask, TaskResult


class CompareTask(BaseTask):
    """Задача сравнения данных и поиска изменений"""
    
    @property
    def name(self) -> str:
        return "compare"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Сравнение данных между временной и основной базой
        
        Args:
            context: Контекст с ключами:
                - 'main_db': имя основной базы
                - 'temp_db': имя временной базы
                - 'tables': словарь {table_name: primary_key}
                
        Returns:
            TaskResult со списком изменений для каждой таблицы
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Сравнение данных ===")
        
        try:
            main_db = context.get('main_db')
            temp_db = context.get('temp_db')
            tables = context.get('tables', {})
            
            if not main_db or not temp_db:
                return self._create_result(
                    success=False,
                    message="Не указаны имена баз данных",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            all_modifications = []
            
            # Для каждой таблицы находим новые и измененные строки
            for table_name, primary_key in tables.items():
                self.logger.info(f"Обработка таблицы {table_name}...")
                
                modified_tables = self.db_ops.find_new_and_changed_rows(
                    table_name, primary_key, main_db, temp_db
                )
                
                for change_type, tbl_name, pk, csv_file in modified_tables:
                    self.logger.info(
                        f"Найдены {change_type} строки в {tbl_name}: {csv_file}"
                    )
                    all_modifications.append({
                        'change_type': change_type,
                        'table_name': tbl_name,
                        'primary_key': pk,
                        'csv_file': csv_file
                    })
            
            self.logger.info(
                f"Всего найдено модификаций: {len(all_modifications)}"
            )
            
            return self._create_result(
                success=True,
                message=f"Найдено {len(all_modifications)} модификаций",
                data={
                    'modifications': all_modifications,
                    'main_db': main_db,
                    'temp_db': temp_db
                },
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при сравнении данных: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка сравнения: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
