# -*- coding: utf-8 -*-
"""
ETL Pipeline Tasks - Модульная система задач
Разбивает монолитный ETL pipeline на отдельные таски
"""

from .base import BaseTask, TaskResult
from .extract_task import ExtractTask
from .restore_task import RestoreTask
from .seed_asterisk_task import SeedAsteriskTask
from .compare_task import CompareTask
from .load_task import LoadTask
from .weekly_task import WeeklyTask
from .cleanup_task import CleanupTask
from .task_runner import TaskRunner

__all__ = [
    'BaseTask',
    'TaskResult',
    'ExtractTask',
    'RestoreTask',
    'SeedAsteriskTask',
    'CompareTask',
    'LoadTask',
    'WeeklyTask',
    'CleanupTask',
    'TaskRunner',
]
