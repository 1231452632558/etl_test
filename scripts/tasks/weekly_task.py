#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача добавления еженедельных записей
Добавляет строки в group_employee_count и users_active
"""

from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult


class WeeklyTask(BaseTask):
    """Задача добавления еженедельных записей"""
    
    @property
    def name(self) -> str:
        return "weekly"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Добавление еженедельных записей
        
        Args:
            context: Контекст с ключами:
                - 'db_name': имя базы данных
                - 'target_day': целевой день недели (1-7, где 1=понедельник)
                
        Returns:
            TaskResult с результатом выполнения
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Еженедельные записи ===")
        
        try:
            db_name = context.get('db_name')
            target_day = context.get('target_day', 1)
            
            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            result = self.db_ops.add_weekly_records(db_name, target_day)
            if result['skipped']:
                current_day = result['current_day']
                return self._create_result(
                    success=True,
                    message=f"Пропущено: сегодня день {current_day}, целевой {target_day}",
                    data={
                        'skipped': True,
                        'current_day': current_day,
                        'target_day': target_day
                    },
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            current_day = result['current_day']
            self.logger.info(
                f"Еженедельные ручные таблицы обработаны для дня {current_day}: "
                f"group_source_count={result.get('group_source_count', 0)}, "
                f"group_upserted={result.get('group_upserted', 0)}, "
                f"users_active_count={result.get('users_active_count', 0)}, "
                f"users_upserted={result.get('users_upserted', 0)}, "
                f"group_name_pattern={result.get('weekly_group_name_pattern', '')}"
            )
            if result.get('group_source_count', 0) == 0:
                self.logger.warning(
                    "Еженедельный источник для group_employee_count вернул 0 групп; "
                    "проверьте weekly_group_name_pattern и данные users/groups_users"
                )
            
            return self._create_result(
                success=True,
                message=(
                    "Еженедельные записи обработаны: "
                    f"group_upserted={result.get('group_upserted', 0)}, "
                    f"users_upserted={result.get('users_upserted', 0)}"
                ),
                data={
                    'skipped': False,
                    'current_day': current_day,
                    'target_day': target_day,
                    'group_source_count': result.get('group_source_count', 0),
                    'group_upserted': result.get('group_upserted', 0),
                    'users_active_count': result.get('users_active_count', 0),
                    'users_upserted': result.get('users_upserted', 0),
                    'weekly_group_name_pattern': result.get('weekly_group_name_pattern', ''),
                },
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении еженедельных записей: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
