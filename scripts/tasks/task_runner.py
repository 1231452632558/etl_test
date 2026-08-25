#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task Runner - Оркестратор задач ETL pipeline
Управляет последовательным выполнением тасок
"""

from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field

from .base import BaseTask, TaskResult


@dataclass
class TaskExecutionRecord:
    """Запись о выполнении задачи"""
    task_name: str
    result: TaskResult
    executed_at: datetime = field(default_factory=datetime.now)


class TaskRunner:
    """
    Оркестратор для управления выполнением задач ETL
    
    Поддерживает:
    - Последовательное выполнение задач
    - Прерывание при ошибке (fail-fast)
    - Контекст между задачами
    - Логирование выполнения
    """
    
    def __init__(self, config, logger, db_ops):
        self.config = config
        self.logger = logger
        self.db_ops = db_ops
        self.tasks: List[BaseTask] = []
        self.execution_history: List[TaskExecutionRecord] = []
        self.context: Dict[str, Any] = {}
    
    def add_task(self, task: BaseTask) -> 'TaskRunner':
        """
        Добавление задачи в пайплайн
        
        Args:
            task: Экземпляр задачи
            
        Returns:
            Self для цепочки вызовов
        """
        self.tasks.append(task)
        self.logger.debug(f"Добавлена задача: {task.name}")
        return self
    
    def set_context(self, key: str, value: Any) -> 'TaskRunner':
        """
        Установка значения в контекст выполнения
        
        Args:
            key: Ключ контекста
            value: Значение
            
        Returns:
            Self для цепочки вызовов
        """
        self.context[key] = value
        return self
    
    def update_context(self, data: Dict[str, Any]) -> 'TaskRunner':
        """
        Обновление контекста данными из словаря
        
        Args:
            data: Словарь данных для добавления в контекст
            
        Returns:
            Self для цепочки вызовов
        """
        self.context.update(data)
        return self
    
    def run(self, fail_fast: bool = True) -> bool:
        """
        Выполнение всех задач
        
        Args:
            fail_fast: Если True, останавливаемся при первой ошибке
            
        Returns:
            True если все задачи выполнены успешно, иначе False
        """
        self.logger.info("=" * 60)
        self.logger.info("Запуск Task Runner")
        self.logger.info(f"Количество задач: {len(self.tasks)}")
        self.logger.info("=" * 60)
        
        all_success = True
        failed_index: Optional[int] = None
        
        for idx, task in enumerate(self.tasks, 1):
            self.logger.info(f"\n[{idx}/{len(self.tasks)}] Выполнение задачи: {task.name}")
            self.logger.info("-" * 40)
            
            try:
                # Выполняем задачу
                result = task.execute(self.context)
                
                # Сохраняем историю
                self.execution_history.append(TaskExecutionRecord(
                    task_name=task.name,
                    result=result
                ))
                
                # Логируем результат
                duration = result.duration or 0
                status = "✓ УСПЕШНО" if result.success else "✗ ОШИБКА"
                self.logger.info(
                    f"Задача {task.name}: {status} "
                    f"(время: {duration:.2f}с)"
                )
                self.logger.info(f"Сообщение: {result.message}")
                
                if result.warnings:
                    for warning in result.warnings:
                        self.logger.warning(f"Предупреждение: {warning}")
                
                if result.errors:
                    for error in result.errors:
                        self.logger.error(f"Ошибка: {error}")
                
                # Обновляем контекст данными от задачи
                if result.data:
                    if isinstance(result.data, dict):
                        self.update_context(result.data)
                
                # Проверяем успешность
                if not result.success:
                    all_success = False
                    if fail_fast:
                        failed_index = idx - 1
                        self.logger.error(
                            f"Прерывание выполнения из-за ошибки в задаче {task.name}"
                        )
                        break
                        
            except Exception as e:
                self.logger.error(f"Исключение при выполнении задачи {task.name}: {e}")
                all_success = False
                failed_index = idx - 1
                
                self.execution_history.append(TaskExecutionRecord(
                    task_name=task.name,
                    result=TaskResult(
                        success=False,
                        task_name=task.name,
                        message=f"Исключение: {str(e)}",
                        errors=[str(e)],
                        started_at=datetime.now(),
                        completed_at=datetime.now()
                    )
                ))
                
                if fail_fast:
                    break

        if not all_success and failed_index is not None:
            self.context["preserve_on_failure"] = True
            self.logger.warning(
                "Pipeline завершился с ошибкой: staging БД и временные "
                "артефакты будут сохранены для диагностики"
            )
            self._run_cleanup_tasks_from(failed_index + 1)
        
        # Итоговый отчет
        self._log_summary(all_success)
        
        return all_success

    def _run_cleanup_tasks_from(self, start_index: int) -> None:
        for task in self.tasks[start_index:]:
            if task.name != "cleanup":
                continue
            self.logger.info("\n[cleanup] Выполнение обязательной очистки после ошибки")
            self.logger.info("-" * 40)
            try:
                result = task.execute(self.context)
                self.execution_history.append(TaskExecutionRecord(task_name=task.name, result=result))
                duration = result.duration or 0
                status = "✓ УСПЕШНО" if result.success else "✗ ОШИБКА"
                self.logger.info(
                    f"Задача {task.name}: {status} "
                    f"(время: {duration:.2f}с)"
                )
                self.logger.info(f"Сообщение: {result.message}")
                if result.warnings:
                    for warning in result.warnings:
                        self.logger.warning(f"Предупреждение: {warning}")
                if result.errors:
                    for error in result.errors:
                        self.logger.error(f"Ошибка: {error}")
            except Exception as exc:
                self.logger.error(f"Исключение при выполнении cleanup: {exc}")
            break
    
    def _log_summary(self, all_success: bool):
        """Логирование итогового отчета"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("ИТОГОВЫЙ ОТЧЕТ")
        self.logger.info("=" * 60)
        
        total_tasks = len(self.execution_history)
        successful_tasks = sum(1 for r in self.execution_history if r.result.success)
        failed_tasks = total_tasks - successful_tasks
        total_duration = sum(
            (r.result.duration or 0) for r in self.execution_history
        )
        
        self.logger.info(f"Всего задач выполнено: {total_tasks}")
        self.logger.info(f"Успешно: {successful_tasks}")
        self.logger.info(f"С ошибками: {failed_tasks}")
        self.logger.info(f"Общее время выполнения: {total_duration:.2f}с")
        self.logger.info(f"Статус: {'УСПЕШНО' if all_success else 'С ОШИБКАМИ'}")
        self.logger.info("=" * 60)
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Получение сводки выполнения
        
        Returns:
            Словарь со статистикой выполнения
        """
        total_tasks = len(self.execution_history)
        successful_tasks = sum(1 for r in self.execution_history if r.result.success)
        total_duration = sum(
            (r.result.duration or 0) for r in self.execution_history
        )
        
        return {
            'total_tasks': total_tasks,
            'successful_tasks': successful_tasks,
            'failed_tasks': total_tasks - successful_tasks,
            'total_duration': total_duration,
            'all_success': all(r.result.success for r in self.execution_history),
            'history': [
                {
                    'task_name': r.task_name,
                    'success': r.result.success,
                    'message': r.result.message,
                    'duration': r.result.duration
                }
                for r in self.execution_history
            ]
        }
