#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача добавления еженедельных записей
Добавляет строки в group_employee_count и users_active
"""

from datetime import datetime, date
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
            
            # Проверяем текущий день недели
            today = date.today()
            current_day = today.isocalendar()[2]  # 1-7 (понедельник-воскресенье)
            
            if current_day != target_day:
                self.logger.info(
                    f"Сегодня не целевой день ({current_day} != {target_day}), "
                    f"пропускаем добавление недельных записей"
                )
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
            
            self.logger.info(f"Еженедельный день ({target_day}). Добавление строк...")
            
            # Добавляем запись в group_employee_count
            command = """
            WITH max_id AS (
                SELECT COALESCE(MAX(id), 0) AS val FROM group_employee_count
            ),
            new_rows AS (
                SELECT
                    m.val + ROW_NUMBER() OVER() AS new_id,
                    g.id as gid, 
                    g.lastname as gname, 
                    COUNT(DISTINCT u.id) as u_count
                FROM users u, max_id m
                JOIN groups_users gu ON u.id = gu.user_id
                JOIN users g ON gu.group_id = g.id
                WHERE u.status = 1 and g.lastname ILIKE 'masked'
                GROUP BY gid, gname
            )
            INSERT INTO group_employee_count (id, group_id, group_name, snapshot_date, user_count)
            SELECT new_id, gid, gname, CURRENT_DATE, u_count
            FROM new_rows
            WHERE NOT EXISTS (
                SELECT 1 FROM group_employee_count gec 
                WHERE gec.group_id = new_rows.gid 
                AND gec.snapshot_date = CURRENT_DATE
            );
            """
            
            self.db_ops._run_psql(command, db_name=db_name, ignore_errors=True)
            self.logger.info("Добавлена запись в group_employee_count")
            
            # Добавляем запись в users_active
            command = """
            INSERT INTO users_active (snapshot_date, user_count) 
            SELECT CURRENT_DATE, COUNT(DISTINCT u.id) 
            FROM users u 
            WHERE u.status = 1
            AND NOT EXISTS (
                SELECT 1 FROM users_active ua 
                WHERE ua.snapshot_date = CURRENT_DATE
            );
            """
            
            self.db_ops._run_psql(command, db_name=db_name, ignore_errors=True)
            self.logger.info("Добавлена запись в users_active")
            
            return self._create_result(
                success=True,
                message="Еженедельные записи добавлены успешно",
                data={
                    'skipped': False,
                    'current_day': current_day,
                    'target_day': target_day
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
