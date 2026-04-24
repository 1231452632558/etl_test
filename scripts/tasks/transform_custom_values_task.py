#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Задача трансформации custom values для таблицы issues
Берет каждую задачу из таблицы issues, сопоставляет строки из custom_values
(идентифицируются по customized_id и customized_type) и транспонирует их в столбцы
таблицы issues (названия берутся из поля name в таблице custom_fields по custom_field_id)
"""

import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from .base import BaseTask, TaskResult


class TransformCustomValuesTask(BaseTask):
    """
    Задача трансформации custom values
    
    Логика:
    1. Для каждой записи в таблице issues находим соответствующие записи 
       в custom_values (где customized_id = issues.id и customized_type = 'Issue')
    2. Для каждой custom_value получаем название поля из custom_fields по custom_field_id
    3. Транспонируем строки custom_values в столбцы и обновляем issues
    """
    
    @property
    def name(self) -> str:
        return "transform_custom_values"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Трансформация custom values в столбцы таблицы issues
        
        Args:
            context: Контекст с ключами:
                - 'db_name': имя базы данных
                
        Returns:
            TaskResult с результатом трансформации
        """
        started_at = datetime.now()
        self.logger.info("=== Задача: Трансформация Custom Values ===")
        
        try:
            db_name = context.get('db_name')
            
            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            # Шаг 1: Получаем все уникальные custom_field_id для Issue
            self.logger.info("Получение списка custom fields для Issue...")
            custom_fields = self._get_custom_fields(db_name)
            
            if not custom_fields:
                self.logger.warning("Custom fields не найдены")
                return self._create_result(
                    success=True,
                    message="Custom fields не найдены, трансформация не требуется",
                    data={'processed_count': 0},
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            self.logger.info(f"Найдено custom fields: {len(custom_fields)}")
            
            # Шаг 2: Добавляем новые колонки в таблицу issues если их нет
            self.logger.info("Добавление колонок в таблицу issues...")
            added_columns = self._add_columns_to_issues(db_name, custom_fields)
            self.logger.info(f"Добавлено колонок: {len(added_columns)}")
            
            # Шаг 3: Транспонируем custom_values и обновляем issues
            self.logger.info("Транспонирование custom values...")
            processed_count = self._transpose_and_update(db_name, custom_fields)
            
            self.logger.info(
                f"Обновлено записей issues с custom values: {processed_count}"
            )
            
            return self._create_result(
                success=True,
                message=f"Трансформация завершена, обработано записей: {processed_count}",
                data={
                    'processed_count': processed_count,
                    'custom_fields_count': len(custom_fields),
                    'columns_added': added_columns
                },
                started_at=started_at,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при трансформации custom values: {e}")
            return self._create_result(
                success=False,
                message=f"Ошибка трансформации: {str(e)}",
                errors=[str(e)],
                started_at=started_at,
                completed_at=datetime.now()
            )
    
    def _get_custom_fields(self, db_name: str) -> List[Dict[str, Any]]:
        """
        Получение списка custom fields для Issue
        
        Returns:
            Список словарей {custom_field_id, field_name}
        """
        query = """
            SELECT id, name
            FROM custom_fields
            WHERE type = 1  -- Type 1 = Issue custom field
            ORDER BY id;
        """
        
        result = self.db_ops._run_psql(query, db_name=db_name, capture_output=True)
        
        if not result:
            return []
        
        fields = []
        lines = result.strip().split('\n')
        
        # Пропускаем заголовок и разделители
        for line in lines:
            line = line.strip()
            if not line or line.startswith('-') or 'id' in line.lower():
                continue
            
            parts = line.split('|')
            if len(parts) >= 2:
                fields.append({
                    'custom_field_id': parts[0].strip(),
                    'field_name': parts[1].strip()
                })
        
        return fields
    
    def _sanitize_column_name(self, name: str) -> str:
        """Преобразование имени поля в корректное имя колонки SQL"""
        # Заменяем пробелы и спецсимволы на подчеркивания
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Убираем множественные подчеркивания
        sanitized = re.sub(r'_+', '_', sanitized)
        # Убираем подчеркивания в начале и конце
        sanitized = sanitized.strip('_')
        # Ограничиваем длину
        return f"cf_{sanitized[:50]}"
    
    def _add_columns_to_issues(self, db_name: str, 
                                custom_fields: List[Dict[str, Any]]) -> List[str]:
        """
        Добавление колонок в таблицу issues для custom fields
        
        Returns:
            Список добавленных колонок
        """
        added_columns = []
        
        # Сначала получаем существующие колонки
        existing_columns = self.db_ops.get_table_columns('issues', db_name)
        
        for field in custom_fields:
            column_name = self._sanitize_column_name(field['field_name'])
            
            if column_name not in existing_columns:
                # Добавляем колонку TEXT типа (универсально для любых значений)
                alter_query = f"""
                    ALTER TABLE issues 
                    ADD COLUMN IF NOT EXISTS {column_name} TEXT;
                """
                
                if self.db_ops._run_psql(alter_query, db_name=db_name, ignore_errors=True):
                    added_columns.append(column_name)
                    self.logger.info(f"Добавлена колонка: {column_name}")
                else:
                    self.logger.warning(f"Не удалось добавить колонку: {column_name}")
        
        return added_columns
    
    def _transpose_and_update(self, db_name: str, 
                               custom_fields: List[Dict[str, Any]]) -> int:
        """
        Транспонирование custom_values и обновление issues
        
        Логика:
        1. Для каждого custom_field создаем UPDATE запрос который:
           - JOIN issues с custom_values по customized_id = issues.id
           - Фильтр: customized_type = 'Issue' AND custom_field_id = X
           - Устанавливает значение в соответствующую колонку
        
        Returns:
            Количество обработанных записей
        """
        total_updated = 0
        
        for field in custom_fields:
            column_name = self._sanitize_column_name(field['field_name'])
            custom_field_id = field['custom_field_id']
            
            # Создаем UPDATE запрос для каждого custom field
            update_query = f"""
                UPDATE issues
                SET {column_name} = cv.value
                FROM custom_values cv
                WHERE cv.customized_id = issues.id
                  AND cv.customized_type = 'Issue'
                  AND cv.custom_field_id = {custom_field_id};
            """
            
            # Выполняем обновление
            result = self.db_ops._run_psql(update_query, db_name=db_name, 
                                           capture_output=True, ignore_errors=True)
            
            if result:
                # Пытаемся получить количество обновленных строк
                count_query = f"""
                    SELECT COUNT(DISTINCT issues.id)
                    FROM issues
                    JOIN custom_values cv ON cv.customized_id = issues.id
                    WHERE cv.customized_type = 'Issue'
                      AND cv.custom_field_id = {custom_field_id}
                      AND cv.value IS NOT NULL;
                """
                count_result = self.db_ops._run_psql(
                    count_query, db_name=db_name, capture_output=True
                )
                if count_result and count_result.strip().isdigit():
                    count = int(count_result.strip())
                    total_updated += count
                    self.logger.info(
                        f"Обновлено записей для {column_name}: {count}"
                    )
        
        return total_updated
