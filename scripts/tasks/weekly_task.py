#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Добавление исторических срезов в group_employee_count и users_active."""

from datetime import datetime
from typing import Dict, Any

from .base import BaseTask, TaskResult


class WeeklyTask(BaseTask):
    """Добавляет или обновляет срез на текущую дату в стабильной main БД."""

    @property
    def name(self) -> str:
        return "weekly"
    
    def execute(self, context: Dict[str, Any]) -> TaskResult:
        """Выполняет weekly по расписанию либо принудительно."""
        started_at = datetime.now()
        self.logger.info("=== Задача: Еженедельные записи ===")

        try:
            db_name = context.get("main_db") or context.get("db_name")
            weekly_enabled = context.get("weekly_enabled", self.config.weekly_enabled)
            weekly_days = tuple(context.get("weekly_days", self.config.weekly_days))
            force_weekly = bool(context.get("force_weekly", False))

            if not db_name:
                return self._create_result(
                    success=False,
                    message="Имя базы данных не указано",
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            if not weekly_enabled and not force_weekly:
                return self._create_result(
                    success=True,
                    message=f"Пропущено: weekly отключен в конфигурации для db={db_name}",
                    data={
                        "skipped": True,
                        "reason": "disabled",
                        "db_name": db_name,
                        "weekly_days": list(weekly_days),
                        "force_weekly": force_weekly,
                    },
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            result = self.db_ops.add_weekly_records(
                db_name,
                weekly_days,
                force=force_weekly,
            )
            if result["skipped"]:
                current_day = result["current_day"]
                return self._create_result(
                    success=True,
                    message=(
                        f"Пропущено: db={db_name}, сегодня день {current_day}, "
                        f"дни запуска {list(weekly_days)}"
                    ),
                    data={
                        "skipped": True,
                        "reason": "day_mismatch",
                        "db_name": db_name,
                        "current_day": current_day,
                        "weekly_days": list(weekly_days),
                        "force_weekly": force_weekly,
                    },
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            current_day = result["current_day"]
            group_failures = result.get("group_failures", [])
            users_failures = result.get("users_failures", [])
            self.logger.info(
                f"Еженедельные ручные таблицы обработаны: db={db_name}, day={current_day}, "
                f"weekly_days={list(weekly_days)}, force={force_weekly}, "
                f"snapshot_date={result.get('snapshot_date', '')}, "
                f"group_source_count={result.get('group_source_count', 0)}, "
                f"group_upserted={result.get('group_upserted', 0)}, "
                f"group_inserted={result.get('group_inserted', 0)}, "
                f"group_updated={result.get('group_updated', 0)}, "
                f"users_active_count={result.get('users_active_count', 0)}, "
                f"users_upserted={result.get('users_upserted', 0)}, "
                f"users_inserted={result.get('users_inserted', 0)}, "
                f"users_updated={result.get('users_updated', 0)}, "
                f"group_name_pattern={result.get('weekly_group_name_pattern', '')}"
            )
            if result.get("group_source_count", 0) == 0:
                self.logger.warning(
                    "Еженедельный источник для group_employee_count вернул 0 групп; "
                    "проверьте weekly_group_name_pattern и данные users/groups_users"
                )
            if group_failures or users_failures:
                return self._create_result(
                    success=False,
                    message=(
                        f"Weekly завершился с ошибками: "
                        f"group_failures={len(group_failures)}, users_failures={len(users_failures)}"
                    ),
                    data={
                        "skipped": False,
                        "db_name": db_name,
                        "current_day": current_day,
                        "weekly_days": list(weekly_days),
                        "force_weekly": force_weekly,
                        "snapshot_date": result.get("snapshot_date", ""),
                        "group_source_count": result.get("group_source_count", 0),
                        "group_upserted": result.get("group_upserted", 0),
                        "group_inserted": result.get("group_inserted", 0),
                        "group_updated": result.get("group_updated", 0),
                        "users_active_count": result.get("users_active_count", 0),
                        "users_upserted": result.get("users_upserted", 0),
                        "users_inserted": result.get("users_inserted", 0),
                        "users_updated": result.get("users_updated", 0),
                        "weekly_group_name_pattern": result.get("weekly_group_name_pattern", ""),
                    },
                    errors=group_failures + users_failures,
                    started_at=started_at,
                    completed_at=datetime.now()
                )
            
            return self._create_result(
                success=True,
                message=(
                    f"Еженедельные записи обработаны в db={db_name}: "
                    f"group_upserted={result.get('group_upserted', 0)}, "
                    f"users_upserted={result.get('users_upserted', 0)}, "
                    f"snapshot_date={result.get('snapshot_date', '')}"
                ),
                data={
                    "skipped": False,
                    "db_name": db_name,
                    "current_day": current_day,
                    "weekly_days": list(weekly_days),
                    "force_weekly": force_weekly,
                    "snapshot_date": result.get("snapshot_date", ""),
                    "group_source_count": result.get("group_source_count", 0),
                    "group_upserted": result.get("group_upserted", 0),
                    "group_inserted": result.get("group_inserted", 0),
                    "group_updated": result.get("group_updated", 0),
                    "users_active_count": result.get("users_active_count", 0),
                    "users_upserted": result.get("users_upserted", 0),
                    "users_inserted": result.get("users_inserted", 0),
                    "users_updated": result.get("users_updated", 0),
                    "weekly_group_name_pattern": result.get("weekly_group_name_pattern", ""),
                },
                started_at=started_at,
                completed_at=datetime.now()
            )
        except Exception as exc:
            self.logger.error(f"Ошибка при добавлении еженедельных записей: {exc}")
            return self._create_result(
                success=False,
                message=f"Ошибка: {exc}",
                errors=[str(exc)],
                started_at=started_at,
                completed_at=datetime.now(),
            )
