#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Задачи и оркестратор task-ориентированного ETL pipeline."""

from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline_common import build_snapshot_plan, extract_dump


@dataclass
class TaskResult:
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
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class BaseTask(ABC):
    def __init__(self, config, logger, db_ops, task_label: Optional[str] = None):
        self.config = config
        self.logger = logger
        self.db_ops = db_ops
        self.task_label = task_label or self.__class__.__name__

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        raise NotImplementedError

    def _result(
        self,
        success: bool,
        started_at: datetime,
        message: str = "",
        data: Any = None,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
    ) -> TaskResult:
        return TaskResult(
            success=success,
            task_name=self.name,
            message=message,
            data=data,
            errors=errors or [],
            warnings=warnings or [],
            started_at=started_at,
            completed_at=datetime.now(),
        )


class ExtractTask(BaseTask):
    @property
    def name(self) -> str:
        return "extract"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Извлечение данных из архива ===")
        try:
            dump_file = context.get("dump_file")
            if not dump_file:
                return self._result(False, started, "Путь к файлу дампа не указан")
            if not os.path.exists(dump_file):
                return self._result(False, started, f"Файл дампа не найден: {dump_file}")
            temp_dir = context.get("temp_dir", self.config.temp_dir)
            self.logger.info(f"Подготовка дампа: {dump_file}")
            extract_dir, sql_files = extract_dump(dump_file, temp_dir, self.logger)
            if not sql_files:
                return self._result(
                    False,
                    started,
                    "SQL файлы не найдены в дампе",
                    data={"extract_dir": extract_dir, "sql_files": []},
                    errors=["Архив не содержит пригодных SQL-файлов"],
                )
            self.logger.info(f"Архив успешно распакован в {extract_dir}")
            self.logger.info(f"Найдено SQL файлов: {len(sql_files)}")
            return self._result(
                True,
                started,
                f"Подготовлено SQL файлов: {len(sql_files)}",
                data={"extract_dir": extract_dir, "sql_files": sql_files},
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при распаковке архива: {exc}")
            return self._result(False, started, f"Ошибка распаковки: {exc}", errors=[str(exc)])


class RestoreTask(BaseTask):
    def __init__(self, config, logger, db_ops, target: str):
        super().__init__(config, logger, db_ops)
        self.target = target

    @property
    def name(self) -> str:
        return f"restore_{self.target}"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Восстановление дампа ===")
        try:
            db_name = (
                context.get("main_db")
                if self.target == "main"
                else context.get("temp_db")
            )
            sql_files = context.get("sql_files", [])
            if not db_name:
                return self._result(False, started, "Имя базы данных не указано")
            if not sql_files:
                return self._result(
                    False,
                    started,
                    "Не переданы SQL-файлы для восстановления",
                    errors=["Пустой список sql_files"],
                )
            create_database = self.target == "temp" or context.get("create_database", False)
            if create_database:
                self.logger.info(f"Создание базы данных {db_name}...")
                if not self.db_ops.create_database(db_name, recreate=self.target == "temp"):
                    return self._result(False, started, f"Ошибка создания базы данных {db_name}")
            if not self.db_ops.restore_dump(db_name, sql_files):
                return self._result(False, started, "Ошибка восстановления дампа")
            if not self.db_ops.create_required_tables(db_name):
                return self._result(False, started, "Ошибка создания служебных таблиц")
            if self.target == "main":
                seed_results = self.db_ops.seed_manual_tables_if_needed(
                    db_name, self.config.manual_seed_files
                )
            else:
                seed_results = {}
                self.logger.info(
                    "Seed для group_employee_count/users_active в nightly staging не выполняется; "
                    "эти таблицы обновляются только weekly-шагом в main БД"
                )
            self.db_ops.log_pipeline_schema_snapshot(
                db_name, self.config.issues_table, self.config.projects_table
            )
            if not self.db_ops.grant_privileges(db_name):
                return self._result(False, started, "Ошибка выдачи прав на восстановленную базу")
            self.logger.info(f"Восстановление в базу {db_name} завершено успешно")
            return self._result(
                True,
                started,
                f"База {db_name} успешно восстановлена",
                data={
                    "db_name": db_name,
                    "seed_results": seed_results,
                    "main_db": db_name if self.target == "main" else context.get("main_db"),
                    "temp_db": db_name if self.target == "temp" else context.get("temp_db"),
                },
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при восстановлении дампа: {exc}")
            return self._result(False, started, f"Ошибка восстановления: {exc}", errors=[str(exc)])


class CompareTask(BaseTask):
    @property
    def name(self) -> str:
        return "compare"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Подготовка FDW snapshot-плана ===")
        try:
            temp_db = context.get("temp_db")
            if not temp_db:
                return self._result(False, started, "Не указана временная база данных")
            modifications = build_snapshot_plan(
                self.db_ops,
                temp_db,
                context.get("tables", {}),
            )
            for modification in modifications:
                self.logger.info(
                    f"Подготовлен FDW snapshot: table={modification['table_name']}, "
                    f"mode={modification['load_mode']}, key={modification['primary_key']}"
                )
            return self._result(
                True,
                started,
                f"Подготовлен FDW snapshot-план: {len(modifications)} таблиц",
                data={"modifications": modifications},
            )
        except Exception as exc:
            self.logger.error(f"Ошибка подготовки FDW snapshot-плана: {exc}")
            return self._result(False, started, f"Ошибка compare stage: {exc}", errors=[str(exc)])


class LoadTask(BaseTask):
    @property
    def name(self) -> str:
        return "load"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Загрузка изменений через postgres_fdw ===")
        try:
            target_db = context.get("main_db") or context.get("db_name")
            source_db = context.get("temp_db")
            modifications = context.get("modifications", [])
            if not target_db or not source_db:
                return self._result(
                    False,
                    started,
                    "Не указана main или staging БД",
                    errors=["Для FDW load требуются main_db и temp_db"],
                )
            if not modifications:
                return self._result(
                    False,
                    started,
                    "Список snapshot-модификаций пуст",
                    errors=["Нет данных для синхронизации main БД"],
                )
            self.logger.info(
                f"FDW snapshot sync: source_db={source_db}, "
                f"target_db={target_db}, tables={len(modifications)}"
            )
            success, applied_count, errors = self.db_ops.apply_snapshot_via_fdw(
                modifications,
                source_db,
                target_db,
            )
            return self._result(
                success,
                started,
                f"Применено {applied_count} модификаций",
                data={"applied_count": applied_count, "errors_count": len(errors)},
                errors=errors,
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при загрузке изменений: {exc}")
            return self._result(False, started, f"Ошибка загрузки: {exc}", errors=[str(exc)])


class SeedAsteriskTask(BaseTask):
    def __init__(self, config, logger, db_ops, db_key: str):
        super().__init__(config, logger, db_ops)
        self.db_key = db_key

    @property
    def name(self) -> str:
        return "seed_asterisk"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Загрузка asterisk_cdr ===")
        try:
            db_name = context.get(self.db_key)
            if not db_name:
                return self._result(
                    False,
                    started,
                    f"Имя базы данных не указано в контексте: {self.db_key}",
                )
            if not self.db_ops.create_required_tables(db_name):
                return self._result(
                    False,
                    started,
                    f"Ошибка создания служебных таблиц в db={db_name}",
                    errors=["create_required_tables failed"],
                )
            result = self.db_ops.append_asterisk_cdr_from_csv(db_name)
            self.db_ops.add_project_id_column(db_name)
            self.db_ops.log_table_schema("asterisk_cdr", db_name, include_indexes=True)
            if not result["success"]:
                reason = str(result.get("reason", "unknown"))
                return self._result(
                    False,
                    started,
                    f"Ошибка append asterisk_cdr в db={db_name}: {reason}",
                    data={"db_name": db_name, **result},
                    errors=[reason],
                )
            return self._result(
                True,
                started,
                f"asterisk_cdr дополнена в db={db_name}: "
                f"source_rows={result.get('source_rows', 0)}, "
                f"inserted_rows={result.get('inserted_rows', 0)}, "
                f"skipped={result.get('skipped', False)}",
                data={"db_name": db_name, **result},
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при загрузке asterisk_cdr: {exc}")
            return self._result(False, started, f"Ошибка seed asterisk: {exc}", errors=[str(exc)])


class WeeklyTask(BaseTask):
    @property
    def name(self) -> str:
        return "weekly"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Еженедельные записи ===")
        try:
            db_name = context.get("main_db") or context.get("db_name")
            enabled = context.get("weekly_enabled", self.config.weekly_enabled)
            days = tuple(context.get("weekly_days", self.config.weekly_days))
            force = bool(context.get("force_weekly", False))
            if not db_name:
                return self._result(False, started, "Имя базы данных не указано")
            if not enabled and not force:
                return self._result(
                    True,
                    started,
                    f"Пропущено: weekly отключен в конфигурации для db={db_name}",
                    data={"skipped": True, "reason": "disabled", "db_name": db_name},
                )
            result = self.db_ops.add_weekly_records(db_name, days, force=force)
            if result["skipped"]:
                return self._result(
                    True,
                    started,
                    f"Пропущено: db={db_name}, сегодня день {result['current_day']}, "
                    f"дни запуска {list(days)}",
                    data={"db_name": db_name, **result},
                )
            group_failures = result.get("group_failures", [])
            users_failures = result.get("users_failures", [])
            self.logger.info(
                f"Еженедельные ручные таблицы обработаны: db={db_name}, "
                f"snapshot_date={result.get('snapshot_date', '')}, "
                f"group_source_count={result.get('group_source_count', 0)}, "
                f"group_upserted={result.get('group_upserted', 0)}, "
                f"users_upserted={result.get('users_upserted', 0)}"
            )
            if result.get("group_source_count", 0) == 0:
                self.logger.warning(
                    "Еженедельный источник для group_employee_count вернул 0 групп; "
                    "проверьте weekly_group_name_pattern и данные users/groups_users"
                )
            errors = group_failures + users_failures
            return self._result(
                not errors,
                started,
                (
                    f"Weekly завершился с ошибками: {len(errors)}"
                    if errors
                    else f"Еженедельные записи обработаны в db={db_name}: "
                    f"group_upserted={result.get('group_upserted', 0)}, "
                    f"users_upserted={result.get('users_upserted', 0)}, "
                    f"snapshot_date={result.get('snapshot_date', '')}"
                ),
                data={"db_name": db_name, **result},
                errors=errors,
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при добавлении еженедельных записей: {exc}")
            return self._result(False, started, f"Ошибка: {exc}", errors=[str(exc)])


class CleanupTask(BaseTask):
    @property
    def name(self) -> str:
        return "cleanup"

    def execute(self, context: Dict[str, Any]) -> TaskResult:
        started = datetime.now()
        self.logger.info("=== Задача: Очистка временных ресурсов ===")
        try:
            temp_db = context.get("temp_db")
            extract_dir = context.get("extract_dir")
            cleanup_temp_db = context.get("cleanup_temp_db", True)
            if context.get("preserve_on_failure", False):
                preserved = [value for value in (temp_db, extract_dir) if value]
                self.logger.warning(
                    f"Очистка пропущена после ошибки; сохранено для диагностики: {preserved}"
                )
                return self._result(
                    True,
                    started,
                    f"Диагностические ресурсы сохранены: {len(preserved)}",
                    data={"preserved_items": preserved},
                )
            cleaned: List[str] = []
            warnings: List[str] = []
            if cleanup_temp_db and temp_db:
                self.logger.info(f"Удаление временной базы {temp_db}...")
                if self.db_ops.drop_database(temp_db):
                    cleaned.append(f"database:{temp_db}")
                else:
                    warnings.append(f"Не удалось удалить базу {temp_db}")
            if extract_dir:
                self.logger.info(f"Удаление директории {extract_dir}...")
                try:
                    shutil.rmtree(extract_dir)
                    cleaned.append(f"directory:{extract_dir}")
                except Exception as exc:
                    warnings.append(f"Ошибка удаления директории: {exc}")
            return self._result(
                True,
                started,
                f"Очистка завершена: {len(cleaned)} объектов",
                data={"cleaned_items": cleaned, "warnings_count": len(warnings)},
                warnings=warnings,
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при очистке: {exc}")
            return self._result(False, started, f"Ошибка очистки: {exc}", errors=[str(exc)])


@dataclass
class TaskExecutionRecord:
    task_name: str
    result: TaskResult
    executed_at: datetime = field(default_factory=datetime.now)


class TaskRunner:
    def __init__(self, config, logger, db_ops):
        self.config = config
        self.logger = logger
        self.db_ops = db_ops
        self.tasks: List[BaseTask] = []
        self.execution_history: List[TaskExecutionRecord] = []
        self.context: Dict[str, Any] = {}

    def add_task(self, task: BaseTask) -> "TaskRunner":
        self.tasks.append(task)
        self.logger.debug(f"Добавлена задача: {task.name}")
        return self

    def set_context(self, key: str, value: Any) -> "TaskRunner":
        self.context[key] = value
        return self

    def update_context(self, data: Dict[str, Any]) -> "TaskRunner":
        self.context.update(data)
        return self

    def _record_and_log(self, task: BaseTask, result: TaskResult) -> None:
        self.execution_history.append(TaskExecutionRecord(task.name, result))
        status = "✓ УСПЕШНО" if result.success else "✗ ОШИБКА"
        self.logger.info(
            f"Задача {task.name}: {status} (время: {(result.duration or 0):.2f}с)"
        )
        self.logger.info(f"Сообщение: {result.message}")
        for warning in result.warnings:
            self.logger.warning(f"Предупреждение: {warning}")
        for error in result.errors:
            self.logger.error(f"Ошибка: {error}")
        if isinstance(result.data, dict):
            self.update_context(result.data)

    def run(self, fail_fast: bool = True) -> bool:
        self.logger.info("=" * 60)
        self.logger.info("Запуск Task Runner")
        self.logger.info(f"Количество задач: {len(self.tasks)}")
        self.logger.info("=" * 60)
        all_success = True
        failed_index: Optional[int] = None
        for index, task in enumerate(self.tasks, 1):
            self.logger.info(f"\n[{index}/{len(self.tasks)}] Выполнение задачи: {task.name}")
            self.logger.info("-" * 40)
            try:
                result = task.execute(self.context)
            except Exception as exc:
                result = TaskResult(
                    success=False,
                    task_name=task.name,
                    message=f"Исключение: {exc}",
                    errors=[str(exc)],
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                )
            self._record_and_log(task, result)
            if not result.success:
                all_success = False
                failed_index = index - 1
                if fail_fast:
                    self.logger.error(
                        f"Прерывание выполнения из-за ошибки в задаче {task.name}"
                    )
                    break
        if not all_success and failed_index is not None:
            self.context["preserve_on_failure"] = True
            self.logger.warning(
                "Pipeline завершился с ошибкой: staging БД и временные "
                "артефакты будут сохранены для диагностики"
            )
            self._run_cleanup_tasks_from(failed_index + 1)
        self._log_summary(all_success)
        return all_success

    def _run_cleanup_tasks_from(self, start_index: int) -> None:
        for task in self.tasks[start_index:]:
            if task.name != "cleanup":
                continue
            self.logger.info("\n[cleanup] Выполнение обязательной очистки после ошибки")
            self.logger.info("-" * 40)
            try:
                self._record_and_log(task, task.execute(self.context))
            except Exception as exc:
                self.logger.error(f"Исключение при выполнении cleanup: {exc}")
            break

    def _log_summary(self, all_success: bool) -> None:
        total = len(self.execution_history)
        successful = sum(record.result.success for record in self.execution_history)
        duration = sum((record.result.duration or 0) for record in self.execution_history)
        self.logger.info("\n" + "=" * 60)
        self.logger.info("ИТОГОВЫЙ ОТЧЕТ")
        self.logger.info("=" * 60)
        self.logger.info(f"Всего задач выполнено: {total}")
        self.logger.info(f"Успешно: {successful}")
        self.logger.info(f"С ошибками: {total - successful}")
        self.logger.info(f"Общее время выполнения: {duration:.2f}с")
        self.logger.info(f"Статус: {'УСПЕШНО' if all_success else 'С ОШИБКАМИ'}")
        self.logger.info("=" * 60)

    def get_execution_summary(self) -> Dict[str, Any]:
        total = len(self.execution_history)
        successful = sum(record.result.success for record in self.execution_history)
        return {
            "total_tasks": total,
            "successful_tasks": successful,
            "failed_tasks": total - successful,
            "total_duration": sum(
                (record.result.duration or 0) for record in self.execution_history
            ),
            "all_success": all(record.result.success for record in self.execution_history),
            "history": [
                {
                    "task_name": record.task_name,
                    "success": record.result.success,
                    "message": record.result.message,
                    "duration": record.result.duration,
                }
                for record in self.execution_history
            ],
        }


__all__ = [
    "BaseTask",
    "TaskResult",
    "ExtractTask",
    "RestoreTask",
    "CompareTask",
    "LoadTask",
    "SeedAsteriskTask",
    "WeeklyTask",
    "CleanupTask",
    "TaskRunner",
]
