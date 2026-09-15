"""Безопасный запуск PostgreSQL-команд на целевом сервере по SSH.

Airflow worker не подключается к PostgreSQL напрямую. Он подключается по SSH к
целевому серверу под техническим пользователем, а psql запускается там через
существующее правило sudoers.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pipeline_common import DatabaseOperations


def _required_text(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"Не задан параметр удаленного PostgreSQL: {name}")
    return text


def _port(value: object, name: str) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Некорректный порт {name}: {value}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"Некорректный порт {name}: {port}")
    return port


def _absolute_file(value: object, name: str) -> str:
    path = Path(_required_text(value, name))
    if not path.is_absolute():
        raise ValueError(f"{name} должен быть абсолютным путем: {path}")
    if not path.is_file():
        raise ValueError(f"Файл {name} не найден: {path}")
    if not os.access(path, os.R_OK):
        raise ValueError(f"Файл {name} недоступен для чтения: {path}")
    return str(path)


@dataclass(frozen=True)
class SshPostgresTarget:
    ssh_host: str
    ssh_user: str
    ssh_port: int
    identity_file: str
    known_hosts_file: str
    db_socket: str
    db_port: str
    remote_sudo: str = "/usr/bin/sudo"
    remote_psql: str = "/usr/bin/psql"
    remote_db_os_user: str = "postgres"
    connect_timeout: int = 15
    restore_timeout_seconds: int = 14400

    @classmethod
    def from_airflow_connection(
        cls,
        connection: Any,
        default_db_socket: str,
        default_db_port: str,
    ) -> "SshPostgresTarget":
        extra = dict(connection.extra_dejson or {})
        ssh_host = _required_text(connection.host, "connection.host")
        ssh_user = _required_text(connection.login, "connection.login")
        if ssh_host.startswith("-") or any(char.isspace() for char in ssh_host):
            raise ValueError(f"Некорректный SSH host: {ssh_host}")
        if (
            ssh_user.startswith("-")
            or "@" in ssh_user
            or any(char.isspace() for char in ssh_user)
        ):
            raise ValueError(f"Некорректный SSH user: {ssh_user}")

        db_socket = _required_text(
            extra.get("db_socket", default_db_socket),
            "extra.db_socket",
        )
        if not db_socket.startswith("/"):
            raise ValueError(
                "Для удаленного запуска db_socket должен быть абсолютным путем "
                f"на целевом сервере: {db_socket}"
            )

        return cls(
            ssh_host=ssh_host,
            ssh_user=ssh_user,
            ssh_port=_port(connection.port or 22, "connection.port"),
            identity_file=_absolute_file(
                extra.get("identity_file"), "extra.identity_file"
            ),
            known_hosts_file=_absolute_file(
                extra.get("known_hosts_file"), "extra.known_hosts_file"
            ),
            db_socket=db_socket,
            db_port=str(_port(extra.get("db_port", default_db_port), "extra.db_port")),
            remote_sudo=_required_text(
                extra.get("remote_sudo", "/usr/bin/sudo"), "extra.remote_sudo"
            ),
            remote_psql=_required_text(
                extra.get("remote_psql", "/usr/bin/psql"), "extra.remote_psql"
            ),
            remote_db_os_user=_required_text(
                extra.get("remote_db_os_user", "postgres"),
                "extra.remote_db_os_user",
            ),
            connect_timeout=max(1, int(extra.get("connect_timeout", 15))),
            restore_timeout_seconds=max(
                60, int(extra.get("restore_timeout_seconds", 14400))
            ),
        )


class SshPostgresDatabaseOperations(DatabaseOperations):
    """DatabaseOperations, выполняющий psql на целевом сервере через SSH."""

    def __init__(self, settings: Any, logger: Any, target: SshPostgresTarget):
        super().__init__(settings, logger)
        self.target = target

    def _remote_psql_args(self, db_name: str, tuples_only: bool = False) -> list[str]:
        args = [
            self.target.remote_sudo,
            "-n",
            "-u",
            self.target.remote_db_os_user,
            self.target.remote_psql,
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self.target.db_socket,
            "-p",
            self.target.db_port,
            "-d",
            db_name,
        ]
        if tuples_only:
            args.extend(["-t", "-A", "-F", "\t"])
        return args

    def _ssh_command(self, remote_args: list[str]) -> list[str]:
        return [
            "ssh",
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={self.target.known_hosts_file}",
            "-o",
            f"ConnectTimeout={self.target.connect_timeout}",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=6",
            "-i",
            self.target.identity_file,
            "-p",
            str(self.target.ssh_port),
            f"{self.target.ssh_user}@{self.target.ssh_host}",
            shlex.join(remote_args),
        ]

    def _log_process_error(
        self,
        kind: str,
        target_db: str,
        stderr: str,
        context_label: Optional[str],
        ignore_errors: bool,
    ) -> None:
        label = f" [{context_label}]" if context_label else ""
        message = (
            f"{kind}{label}: ssh_host={self.target.ssh_host}, "
            f"db={target_db}, error={stderr or 'unknown error'}"
        )
        if ignore_errors:
            self.logger.warning(message)
        else:
            self.logger.error(message)

    def _run_psql(
        self,
        command: str,
        db_name: Optional[str] = None,
        capture_output: bool = False,
        ignore_errors: bool = False,
        tuples_only: bool = False,
        context_label: Optional[str] = None,
    ) -> bool | str:
        target_db = db_name or self.settings.db_name
        cmd = self._ssh_command(self._remote_psql_args(target_db, tuples_only))
        try:
            result = subprocess.run(
                cmd,
                input=command,
                capture_output=True,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            label = f" [{context_label}]" if context_label else ""
            message = (
                f"Remote PSQL timeout{label}: ssh_host={self.target.ssh_host}, "
                f"db={target_db}"
            )
            self.logger.error(message)
            if capture_output:
                raise RuntimeError(message)
            return False
        except Exception as exc:
            label = f" [{context_label}]" if context_label else ""
            message = (
                f"Ошибка запуска remote psql{label}: "
                f"ssh_host={self.target.ssh_host}, db={target_db}, error={exc}"
            )
            self.logger.error(message)
            if capture_output:
                raise RuntimeError(message) from exc
            return False

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            self._log_process_error(
                "Remote PSQL error",
                target_db,
                stderr,
                context_label,
                ignore_errors,
            )
            if capture_output:
                raise RuntimeError(
                    f"Remote PSQL query failed: ssh_host={self.target.ssh_host}, "
                    f"db={target_db}: {stderr or f'returncode={result.returncode}'}"
                )
            return False

        return result.stdout if capture_output else True

    def _run_psql_script(
        self,
        script: str,
        db_name: Optional[str] = None,
        ignore_errors: bool = False,
        context_label: Optional[str] = None,
        timeout_seconds: int = 1800,
    ) -> bool:
        target_db = db_name or self.settings.db_name
        cmd = self._ssh_command(self._remote_psql_args(target_db))
        try:
            result = subprocess.run(
                cmd,
                input=script,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(
                f"Remote PSQL script timeout{label}: "
                f"ssh_host={self.target.ssh_host}, db={target_db}, "
                f"timeout_seconds={timeout_seconds}"
            )
            return False
        except Exception as exc:
            label = f" [{context_label}]" if context_label else ""
            self.logger.error(
                f"Ошибка запуска remote psql script{label}: "
                f"ssh_host={self.target.ssh_host}, db={target_db}, error={exc}"
            )
            return False

        if result.returncode != 0:
            self._log_process_error(
                "Remote PSQL script error",
                target_db,
                (result.stderr or "").strip(),
                context_label,
                ignore_errors,
            )
            return False
        return True

    def _run_psql_file(self, sql_file: str, db_name: Optional[str] = None) -> bool:
        target_db = db_name or self.settings.db_name
        if not os.path.isfile(sql_file):
            self.logger.error(f"SQL file not found: {sql_file}")
            return False
        cmd = self._ssh_command(self._remote_psql_args(target_db))
        try:
            with open(sql_file, "rb") as sql_stream:
                result = subprocess.run(
                    cmd,
                    stdin=sql_stream,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=self.target.restore_timeout_seconds,
                )
        except subprocess.TimeoutExpired:
            self.logger.error(
                f"Remote SQL file timeout: ssh_host={self.target.ssh_host}, "
                f"db={target_db}, file={sql_file}, "
                f"timeout_seconds={self.target.restore_timeout_seconds}"
            )
            return False
        except Exception as exc:
            self.logger.error(
                f"Ошибка передачи SQL file: ssh_host={self.target.ssh_host}, "
                f"db={target_db}, file={sql_file}, error={exc}"
            )
            return False

        if result.returncode != 0:
            stderr = (result.stderr or b"").decode("utf-8", errors="replace").strip()
            self._log_process_error(
                "Remote SQL file error",
                target_db,
                stderr,
                context_label=f"file:{os.path.basename(sql_file)}",
                ignore_errors=False,
            )
            return False
        return True
