#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача восстановления дампа в базу данных
"""

import os
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from .base import BaseTask, TaskResult


class RestoreTask(BaseTask):
    """Задача восстановления SQL-дампа в базу данных"""

    def __init__(self, config, logger, db_ops, target: str):
        super().__init__(config, logger, db_ops)
        self.target = target
    
    @property
    def name(self) -> str:
        return f"restore_{self.target}"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Восстановление дампа из SQL файлов в базу данных
        
        Args:
            context: Контекст с ключами:
                - 'db_name': имя базы данных
                - 'sql_files': список SQL файлов
                - 'create_tables': создавать ли требуемые таблицы
                
        Returns:
            TaskResult с результатом восстановления
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Восстановление дампа ===")
        
        try:
            db_name = context.get('main_db') if self.target == 'main' else context.get('temp_db')
            sql_files = context.get('sql_files', [])
            
            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            create_database = self.target == 'temp' or context.get('create_database', False)
            if create_database:
                self.logger.info(f"Создание базы данных {db_name}...")
                recreate = self.target == 'temp'
                if not self.db_ops.create_database(db_name, recreate=recreate):
                    return self._create_result(
                        success=False,
                        message=f"Ошибка создания базы данных {db_name}",
                        started_at=started_at,
                        completed_at=datetime.now()
                    )
            
            # Восстанавливаем дамп из SQL файлов
            if sql_files:
                if not self.db_ops.restore_dump(db_name, sql_files):
                    return self._create_result(
                        success=False,
                        message="Ошибка восстановления дампа",
                        started_at=started_at,
                        completed_at=datetime.now()
                    )
            
            self.db_ops.create_required_tables(db_name)
            asterisk_seeded = self.db_ops.seed_asterisk_cdr_if_needed(db_name)
            self.db_ops.add_project_id_column(db_name)
            if self.target == 'main':
                seed_results = self.db_ops.seed_manual_tables_if_needed(db_name, self.config.manual_seed_files)
            else:
                seed_results = {}
            self.db_ops.log_pipeline_schema_snapshot(db_name, self.config.issues_table)
            self.db_ops.grant_privileges(db_name)
            
            self.logger.info(f"Восстановление в базу {db_name} завершено успешно")
            
            return self._create_result(
                success=True,
                message=f"База {db_name} успешно восстановлена",
                data={
                    'db_name': db_name,
                    'seed_results': seed_results,
                    'asterisk_seeded': asterisk_seeded,
                    'main_db': db_name if self.target == 'main' else context.get('main_db'),
                    'temp_db': db_name if self.target == 'temp' else context.get('temp_db'),
                },
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при восстановлении дампа: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка восстановления: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
