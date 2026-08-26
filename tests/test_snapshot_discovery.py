#!/usr/bin/env python3

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_common import (  # noqa: E402
    SNAPSHOT_FDW_SCHEMA,
    DatabaseOperations,
    build_snapshot_plan,
    copy_from_remote_or_local,
    finalize_remote_backup,
    should_download_remote_backup,
)
from tasks import LoadTask  # noqa: E402


class FakeLogger:
    def __init__(self):
        self.messages = []

    def _add(self, level, message):
        self.messages.append((level, message))

    def debug(self, message):
        self._add("DEBUG", message)

    def info(self, message):
        self._add("INFO", message)

    def warning(self, message):
        self._add("WARNING", message)

    def error(self, message):
        self._add("ERROR", message)


def make_settings(**overrides):
    values = {
        "auto_discover_tables": True,
        "fail_on_unkeyed_tables": True,
        "snapshot_excluded_tables": (
            "asterisk_cdr",
            "group_employee_count",
            "users_active",
        ),
        "snapshot_replace_tables": (),
        "issues_table": "issues",
        "projects_table": "projects",
        "db_name": "main",
        "db_host": "/tmp",
        "db_port": "5432",
        "snapshot_batch_timeout_seconds": 14400,
        "snapshot_lock_timeout_seconds": 300,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class SnapshotDiscoveryTests(unittest.TestCase):
    def test_discovers_keys_excludes_manual_and_orders_parent_first(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())

        def fake_run_rows(query, db_name=None):
            if "SELECT c.relname" in query:
                return [["asterisk_cdr"], ["issues"], ["projects"], ["users"]]
            if "FROM pg_class tbl" in query:
                return [
                    ["asterisk_cdr", "asterisk_cdr_pkey", "t", "1", "id"],
                    ["issues", "issues_pkey", "t", "1", "id"],
                    ["projects", "projects_pkey", "t", "1", "id"],
                    ["users", "users_pkey", "t", "1", "id"],
                ]
            if "FROM pg_constraint constraint_row" in query:
                return [["issues", "projects"], ["issues", "users"]]
            return []

        db_ops.run_rows = fake_run_rows
        resolved = db_ops.resolve_snapshot_tables("staging", {})
        self.assertEqual(list(resolved), ["projects", "users", "issues"])
        self.assertNotIn("asterisk_cdr", resolved)
        self.assertEqual(resolved["issues"], "id")

    def test_composite_unique_index_is_used(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())

        def fake_run_rows(query, db_name=None):
            if "SELECT c.relname" in query:
                return [["groups_users"]]
            if "FROM pg_class tbl" in query:
                return [
                    ["groups_users", "groups_users_unique", "f", "1", "group_id"],
                    ["groups_users", "groups_users_unique", "f", "2", "user_id"],
                ]
            return []

        db_ops.run_rows = fake_run_rows
        self.assertEqual(
            db_ops.resolve_snapshot_tables("staging", {}),
            {"groups_users": ["group_id", "user_id"]},
        )

    def test_unkeyed_table_stops_pipeline(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        db_ops.run_rows = lambda query, db_name=None: (
            [["unsafe_table"]] if "SELECT c.relname" in query else []
        )
        with self.assertRaisesRegex(ValueError, "unsafe_table"):
            db_ops.resolve_snapshot_tables("staging", {})

    def test_explicit_replace_table_is_not_treated_as_unkeyed(self):
        db_ops = DatabaseOperations(
            make_settings(snapshot_replace_tables=("keyless_links",)), FakeLogger()
        )
        db_ops.run_rows = lambda query, db_name=None: (
            [["keyless_links"]] if "SELECT c.relname" in query else []
        )
        self.assertEqual(db_ops.resolve_snapshot_tables("staging", {}), {})

    def test_build_plan_contains_metadata_and_no_csv_path(self):
        db_ops = DatabaseOperations(
            make_settings(snapshot_replace_tables=("keyless_links",)), FakeLogger()
        )
        db_ops.resolve_snapshot_tables = lambda db_name, configured: {"issues": "id"}
        db_ops.table_exists = lambda table_name, db_name: True
        plan = build_snapshot_plan(db_ops, "staging", {})
        self.assertEqual(plan[0]["table_name"], "issues")
        self.assertEqual(plan[0]["load_mode"], "upsert")
        self.assertEqual(plan[1]["table_name"], "keyless_links")
        self.assertEqual(plan[1]["load_mode"], "replace")
        self.assertTrue(all("csv_file" not in item for item in plan))


class SnapshotFdwTests(unittest.TestCase):
    def test_setup_imports_only_selected_tables(self):
        db_ops = DatabaseOperations(make_settings(db_host="/run/postgresql"), FakeLogger())
        scripts = []
        db_ops.run_scalar = lambda query, db_name=None: "postgres"

        def fake_script(script, **kwargs):
            scripts.append((script, kwargs))
            return True

        db_ops._run_psql_script = fake_script
        self.assertTrue(db_ops.setup_snapshot_fdw("staging", "main", ["issues", "projects"]))
        sql = scripts[0][0]
        self.assertIn("CREATE EXTENSION IF NOT EXISTS postgres_fdw", sql)
        self.assertIn("host '/run/postgresql'", sql)
        self.assertIn("dbname 'staging'", sql)
        self.assertIn('LIMIT TO ("issues", "projects")', sql)
        self.assertIn(f'INTO "{SNAPSHOT_FDW_SCHEMA}"', sql)

    def test_upsert_reads_foreign_table_and_updates_only_changed_rows(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        sql = db_ops._snapshot_upsert_sql(
            "issues",
            ["id", "subject", "metadata"],
            {"id": "integer", "subject": "text", "metadata": "json"},
            ["id"],
        )
        self.assertIn(f'FROM "{SNAPSHOT_FDW_SCHEMA}"."issues"', sql)
        self.assertIn('ON CONFLICT ("id")', sql)
        self.assertIn('"issues"."subject" IS DISTINCT FROM EXCLUDED."subject"', sql)
        self.assertIn('("issues"."metadata")::jsonb IS DISTINCT FROM', sql)
        self.assertNotIn("COPY", sql.upper())

    def test_apply_runs_keyed_then_replace_group_and_cleans(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        calls = []
        db_ops.setup_snapshot_fdw = lambda source, target, tables: True
        db_ops.cleanup_snapshot_fdw = lambda target: calls.append(("cleanup", target)) or True
        db_ops.log_suspicious_transactions = lambda target: None
        db_ops._schema_column_details = lambda schema, tables, db: {
            "projects": [("id", "integer"), ("name", "text")],
            "issues": [("id", "integer"), ("subject", "text")],
            "links": [("from_id", "integer"), ("to_id", "integer")],
        }

        def fake_script(script, db_name=None, context_label="", **kwargs):
            calls.append((context_label, db_name, script))
            return True

        db_ops._run_psql_script = fake_script
        success, count, errors = db_ops.apply_snapshot_via_fdw(
            [
                {"table_name": "projects", "primary_key": "id", "load_mode": "upsert"},
                {"table_name": "issues", "primary_key": "id", "load_mode": "upsert"},
                {"table_name": "links", "primary_key": None, "load_mode": "replace"},
            ],
            "staging",
            "main",
        )
        self.assertTrue(success)
        self.assertEqual(count, 3)
        self.assertEqual(errors, [])
        labels = [call[0] for call in calls]
        self.assertEqual(
            labels[:3],
            ["fdw_table:projects", "fdw_table:issues", "fdw_replace_group"],
        )
        self.assertEqual(calls[-1], ("cleanup", "main"))

    def test_schema_drift_stops_before_writes_and_cleans(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        cleanup = []
        db_ops.setup_snapshot_fdw = lambda source, target, tables: True
        db_ops.cleanup_snapshot_fdw = lambda target: cleanup.append(target) or True
        db_ops._schema_column_details = lambda schema, tables, db: (
            {"issues": [("id", "integer"), ("new_column", "text")]}
            if schema == SNAPSHOT_FDW_SCHEMA
            else {"issues": [("id", "integer")]}
        )
        db_ops._run_psql_script = lambda *args, **kwargs: self.fail("write must not run")
        success, count, errors = db_ops.apply_snapshot_via_fdw(
            [{"table_name": "issues", "primary_key": "id", "load_mode": "upsert"}],
            "staging",
            "main",
        )
        self.assertFalse(success)
        self.assertEqual(count, 0)
        self.assertIn("new_column", errors[0])
        self.assertEqual(cleanup, ["main"])

    def test_load_task_passes_source_and_target(self):
        calls = []

        class FakeDbOps:
            def apply_snapshot_via_fdw(self, modifications, source_db, target_db):
                calls.append((modifications, source_db, target_db))
                return True, len(modifications), []

        task = LoadTask(make_settings(), FakeLogger(), FakeDbOps())
        modifications = [{"table_name": "issues", "primary_key": "id"}]
        result = task.execute(
            {"temp_db": "staging", "main_db": "main", "modifications": modifications}
        )
        self.assertTrue(result.success)
        self.assertEqual(calls, [(modifications, "staging", "main")])


class BackupTests(unittest.TestCase):
    def test_remote_download_is_always_required_when_configured(self):
        settings = SimpleNamespace(
            remote_user="backup",
            remote_host="srv",
            remote_path="/backup/",
            backup_tar="current.tgz",
        )
        self.assertTrue(should_download_remote_backup(settings, None))
        self.assertTrue(should_download_remote_backup(settings, "/tmp/old.tgz"))

    def test_local_copy_uses_incoming_file_before_publication(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "remote.tgz"
            source.write_bytes(b"new dump")
            settings = SimpleNamespace(
                remote_user="",
                remote_host="",
                remote_path="",
                backup_tar="",
            )
            selected = copy_from_remote_or_local(settings, logger, str(source))
            self.assertEqual(selected, str(source))

    def test_successful_rotation_keeps_current_and_two_old(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as directory:
            backup_dir = Path(directory) / "backup"
            old_dir = Path(directory) / "old_backup"
            backup_dir.mkdir()
            old_dir.mkdir()
            current = backup_dir / "current.tgz"
            current.write_bytes(b"previous")
            incoming = backup_dir / "incoming_new.tgz"
            incoming.write_bytes(b"new")
            for index in range(3):
                archive_dir = old_dir / f"old_2026082{index}"
                archive_dir.mkdir()
                (archive_dir / "current.tgz").write_bytes(str(index).encode())
                os.utime(archive_dir, (index + 1, index + 1))
            settings = SimpleNamespace(
                backup_storage_dir=str(backup_dir),
                old_backup_dir=str(old_dir),
                backup_tar="current.tgz",
                max_backups=3,
            )
            self.assertTrue(finalize_remote_backup(settings, logger, str(incoming)))
            self.assertEqual(current.read_bytes(), b"new")
            self.assertEqual(len(list(old_dir.iterdir())), 2)

    def test_failed_publication_does_not_replace_current(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as directory:
            current = Path(directory) / "current.tgz"
            current.write_bytes(b"previous")
            incoming = Path(directory) / "incoming_new.tgz"
            incoming.write_bytes(b"new")
            settings = SimpleNamespace(
                backup_storage_dir=directory,
                old_backup_dir=str(Path(directory) / "old"),
                backup_tar="current.tgz",
                max_backups=3,
            )
            with patch("pipeline_common.shutil.move", side_effect=OSError("boom")):
                self.assertFalse(finalize_remote_backup(settings, logger, str(incoming)))
            self.assertEqual(current.read_bytes(), b"previous")


if __name__ == "__main__":
    unittest.main()
