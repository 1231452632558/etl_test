#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Базовый класс для всех задач ETL pipeline
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime


@dataclass
class TaskResult:
    """Результат выполнения задачи"""
    success: bool
    task_name: str
    message: str = ""
    data: Any = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Длительность выполнения в секундах"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class BaseTask(ABC):
    """Базовый класс для всех задач ETL"""
    
    def __init__(self, config, logger, db_ops):
        self.config = config
        self.logger = logger
        self.db_ops = db_ops
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Имя задачи"""
        pass
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """
        Выполнение задачи
        
        Args:
            context: Контекст выполнения, содержащий данные от предыдущих задач
            
        Returns:
            TaskResult: Результат выполнения задачи
        """
        pass
    
    def _create_result(self, success: bool, message: str = "", 
                       data: Any = None, errors: List[str] = None,
                       warnings: List[str] = None) -> TaskResult:
        """Создание результата выполнения задачи"""
        return TaskResult(
            success=success,
            task_name=self.name,
            message=message,
            data=data,
            errors=errors or [],
            warnings=warnings or []
        )
