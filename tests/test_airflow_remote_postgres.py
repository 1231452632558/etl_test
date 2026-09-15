#!/usr/bin/env python3

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "scripts"))
sys.path.insert(0, str(ROOT_DIR / "airflow" / "runtime"))

from remote_postgres import (  # noqa: E402
    SshPostgresDatabaseOperations,
    SshPostgresTarget,
)


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(("INFO", message))

    def warning(self, message):
        self.messages.append(("WARNING", message))

    def error(self, message):
        self.messages.append(("ERROR", message))


class RemotePostgresTests(unittest.TestCase):
    def _target(self, temp_dir):
        identity = Path(temp_dir) / "id_ed25519"
        known_hosts = Path(temp_dir) / "known_hosts"
        identity.write_text("test-key", encoding="utf-8")
        known_hosts.write_text("db.example.test ssh-ed25519 test", encoding="utf-8")
        connection = SimpleNamespace(
            host="db.example.test",
            login="airflow-etl",
            port=2222,
            extra_dejson={
                "identity_file": str(identity),
                "known_hosts_file": str(known_hosts),
                "db_socket": "/var/run/postgresql",
                "db_port": 5433,
                "restore_timeout_seconds": 7200,
            },
        )
        return SshPostgresTarget.from_airflow_connection(
            connection,
            default_db_socket="/run/postgresql",
            default_db_port="5432",
        )

    def test_connection_builds_remote_target(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = self._target(temp_dir)
        self.assertEqual(target.ssh_host, "db.example.test")
        self.assertEqual(target.ssh_user, "airflow-etl")
        self.assertEqual(target.ssh_port, 2222)
        self.assertEqual(target.db_socket, "/var/run/postgresql")
        self.assertEqual(target.db_port, "5433")
        self.assertEqual(target.restore_timeout_seconds, 7200)

    def test_psql_runs_only_on_target_host(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = self._target(temp_dir)
            settings = SimpleNamespace(db_name="main")
            db_ops = SshPostgresDatabaseOperations(settings, FakeLogger(), target)
            completed = subprocess.CompletedProcess([], 0, stdout="postgres\n", stderr="")
            with patch("remote_postgres.subprocess.run", return_value=completed) as run:
                result = db_ops._run_psql(
                    "SELECT current_user;",
                    db_name="postgres",
                    capture_output=True,
                    tuples_only=True,
                )

        self.assertEqual(result, "postgres\n")
        command = run.call_args.args[0]
        self.assertEqual(command[0], "ssh")
        self.assertIn("airflow-etl@db.example.test", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn(
            "/usr/bin/sudo -n -u postgres /usr/bin/psql",
            command[-1],
        )
        self.assertIn("-h /var/run/postgresql", command[-1])
        self.assertEqual(run.call_args.kwargs["input"], "SELECT current_user;")

    def test_fdw_uses_target_local_socket_without_password(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = self._target(temp_dir)
            settings = SimpleNamespace(
                db_name="main",
                db_host=target.db_socket,
                db_port=target.db_port,
            )
            db_ops = SshPostgresDatabaseOperations(settings, FakeLogger(), target)
            db_ops.run_scalar = lambda query, db_name=None: "postgres"
            scripts = []
            db_ops._run_psql_script = (
                lambda script, **kwargs: scripts.append((script, kwargs)) or True
            )
            self.assertTrue(
                db_ops.setup_snapshot_fdw("staging", "main", ["issues"])
            )

        script = scripts[0][0]
        self.assertIn("host '/var/run/postgresql'", script)
        self.assertIn("port '5433'", script)
        self.assertIn("dbname 'staging'", script)
        self.assertIn("OPTIONS (user 'postgres')", script)
        self.assertNotIn("password", script.lower())

    def test_sql_file_is_streamed_to_remote_psql(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = self._target(temp_dir)
            sql_file = Path(temp_dir) / "dump.sql"
            sql_file.write_text("SELECT 1;\n", encoding="utf-8")
            settings = SimpleNamespace(db_name="staging")
            db_ops = SshPostgresDatabaseOperations(settings, FakeLogger(), target)
            completed = subprocess.CompletedProcess([], 0, stdout=b"", stderr=b"")
            with patch("remote_postgres.subprocess.run", return_value=completed) as run:
                self.assertTrue(db_ops._run_psql_file(str(sql_file)))

        command = run.call_args.args[0]
        self.assertEqual(command[0:2], ["ssh", "-T"])
        self.assertNotIn("SELECT 1", " ".join(command))
        self.assertEqual(run.call_args.kwargs["timeout"], 7200)
        self.assertEqual(run.call_args.kwargs["stdout"], subprocess.DEVNULL)

    def test_missing_ssh_files_are_rejected(self):
        connection = SimpleNamespace(
            host="db.example.test",
            login="airflow-etl",
            port=22,
            extra_dejson={
                "identity_file": "/missing/id_ed25519",
                "known_hosts_file": "/missing/known_hosts",
            },
        )
        with self.assertRaisesRegex(ValueError, "identity_file"):
            SshPostgresTarget.from_airflow_connection(
                connection,
                default_db_socket="/var/run/postgresql",
                default_db_port="5432",
            )

    def test_option_like_ssh_user_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            identity = Path(temp_dir) / "id_ed25519"
            known_hosts = Path(temp_dir) / "known_hosts"
            identity.touch()
            known_hosts.touch()
            connection = SimpleNamespace(
                host="db.example.test",
                login="-oProxyCommand=bad",
                port=22,
                extra_dejson={
                    "identity_file": str(identity),
                    "known_hosts_file": str(known_hosts),
                },
            )
            with self.assertRaisesRegex(ValueError, "SSH user"):
                SshPostgresTarget.from_airflow_connection(
                    connection,
                    default_db_socket="/var/run/postgresql",
                    default_db_port="5432",
                )


if __name__ == "__main__":
    unittest.main()
