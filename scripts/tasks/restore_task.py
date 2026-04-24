#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача восстановления дампа в базу данных
"""

import os
from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult


class RestoreTask(BaseTask):
    """Задача восстановления SQL-дампа в базу данных"""
    
    @property
    def name(self) -> str:
        return "restore"
    
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
            db_name = context.get('db_name')
            sql_files = context.get('sql_files', [])
            create_tables = context.get('create_tables', False)
            
            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            # Создаем базу данных если нужно (для новой БД)
            if context.get('create_database', False):
                self.logger.info(f"Создание базы данных {db_name}...")
                if not self.db_ops.create_database(db_name):
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
            
            # Создаем необходимые таблицы если требуется
            if create_tables:
                self.logger.info("Создание необходимых таблиц...")
                self.db_ops.create_required_tables(db_name)
                
                # Добавляем project_id в asterisk_cdr
                self.db_ops.add_project_id_column(db_name)
            
            # Предоставляем права
            self.db_ops.grant_privileges(db_name)
            
            self.logger.info(f"Восстановление в базу {db_name} завершено успешно")
            
            return self._create_result(
                success=True,
                message=f"База {db_name} успешно восстановлена",
                data={'db_name': db_name},
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
