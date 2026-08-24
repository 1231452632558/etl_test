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
    DatabaseOperations,
    copy_from_remote_or_local,
    finalize_remote_backup,
    should_download_remote_backup,
)


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
        "issues_table": "issues",
        "projects_table": "projects",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class SnapshotDiscoveryTests(unittest.TestCase):
    def test_discovers_keys_excludes_manual_and_orders_parent_first(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())

        def fake_run_rows(query, db_name=None):
            if "SELECT c.relname" in query:
                return [
                    ["asterisk_cdr"],
                    ["issues"],
                    ["projects"],
                    ["users"],
                ]
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
            if "FROM pg_constraint constraint_row" in query:
                return []
            return []

        db_ops.run_rows = fake_run_rows
        resolved = db_ops.resolve_snapshot_tables("staging", {})

        self.assertEqual(resolved, {"groups_users": ["group_id", "user_id"]})

    def test_unkeyed_table_stops_pipeline(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())

        def fake_run_rows(query, db_name=None):
            if "SELECT c.relname" in query:
                return [["unsafe_table"]]
            return []

        db_ops.run_rows = fake_run_rows
        with self.assertRaisesRegex(ValueError, "unsafe_table"):
            db_ops.resolve_snapshot_tables("staging", {})

    def test_single_key_only_table_uses_do_nothing(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        db_ops.get_table_columns = lambda table_name, db_name: ["version"]
        captured = {}

        def fake_run_script(script, db_name=None, context_label=None):
            captured["script"] = script
            return True

        db_ops._run_psql_script = fake_run_script
        db_ops.sync_sequence_with_max_id = lambda *args, **kwargs: False

        csv_path = ""
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
                csv_path = handle.name
                handle.write("version\n202608240001\n")
            result = db_ops.upsert_from_csv(
                "schema_migrations",
                "version",
                csv_path,
                "main",
            )
        finally:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)

        self.assertTrue(result)
        self.assertIn("ON CONFLICT (\"version\")\nDO NOTHING;", captured["script"])

    def test_schema_drift_is_not_silently_ignored(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        db_ops.get_table_columns = lambda table_name, db_name: ["id"]

        csv_path = ""
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
                csv_path = handle.name
                handle.write("id,new_column\n1,value\n")
            result = db_ops.upsert_from_csv("issues", "id", csv_path, "main")
        finally:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)

        self.assertFalse(result)

    def test_upsert_updates_only_changed_rows(self):
        db_ops = DatabaseOperations(make_settings(), FakeLogger())
        db_ops.get_table_columns = lambda table_name, db_name: ["id", "subject"]
        captured = {}

        def fake_run_script(script, db_name=None, context_label=None):
            captured["script"] = script
            return True

        db_ops._run_psql_script = fake_run_script
        db_ops.sync_sequence_with_max_id = lambda *args, **kwargs: False

        csv_path = ""
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as handle:
                csv_path = handle.name
                handle.write("id,subject\n1,Updated\n")
            result = db_ops.upsert_from_csv("issues", "id", csv_path, "main")
        finally:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)

        self.assertTrue(result)
        self.assertIn(
            'WHERE "issues"."subject" IS DISTINCT FROM EXCLUDED."subject"',
            captured["script"],
        )


class BackupRotationTests(unittest.TestCase):
    def test_remote_mode_does_not_require_dump_argument(self):
        settings = make_settings(
            remote_user="backup",
            remote_host="example.internal",
            remote_path="/exports/",
            backup_tar="nightly.tar.gz",
        )

        self.assertTrue(should_download_remote_backup(settings, None))

    def test_remote_archive_is_downloaded_to_temp_before_etl(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as root_dir:
            backup_dir = os.path.join(root_dir, "current")
            temp_dir = os.path.join(root_dir, "temp")
            os.makedirs(backup_dir)
            current_file = os.path.join(backup_dir, "nightly.tar.gz")
            with open(current_file, "w", encoding="utf-8") as handle:
                handle.write("previous")

            settings = make_settings(
                remote_user="backup",
                remote_host="example.internal",
                remote_path="/exports/",
                backup_tar="nightly.tar.gz",
                backup_storage_dir=backup_dir,
                temp_dir=temp_dir,
            )

            def fake_scp(command, **_kwargs):
                destination = command[2]
                with open(destination, "w", encoding="utf-8") as handle:
                    handle.write("incoming")
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            with patch("pipeline_common.subprocess.run", side_effect=fake_scp):
                incoming_file = copy_from_remote_or_local(settings, logger, None)

            self.assertIsNotNone(incoming_file)
            self.assertTrue(incoming_file.startswith(os.path.join(temp_dir, "incoming_")))
            with open(current_file, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "previous")

    def test_invalid_incoming_archive_does_not_change_existing_backups(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as root_dir:
            backup_dir = os.path.join(root_dir, "current")
            old_backup_dir = os.path.join(root_dir, "old_backup")
            os.makedirs(backup_dir)
            os.makedirs(old_backup_dir)
            current_file = os.path.join(backup_dir, "nightly.tar.gz")
            with open(current_file, "w", encoding="utf-8") as handle:
                handle.write("previous")

            settings = make_settings(
                backup_storage_dir=backup_dir,
                old_backup_dir=old_backup_dir,
                backup_tar="nightly.tar.gz",
                max_backups=3,
            )
            missing_file = os.path.join(root_dir, "incoming_missing.tar.gz")

            self.assertFalse(finalize_remote_backup(settings, logger, missing_file))
            with open(current_file, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "previous")
            self.assertEqual(os.listdir(old_backup_dir), [])

    def test_successful_finalize_keeps_current_and_two_old_backups(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as root_dir:
            backup_dir = os.path.join(root_dir, "current")
            old_backup_dir = os.path.join(root_dir, "old_backup")
            temp_dir = os.path.join(root_dir, "temp")
            os.makedirs(backup_dir)
            os.makedirs(old_backup_dir)
            os.makedirs(temp_dir)

            current_file = os.path.join(backup_dir, "nightly.tar.gz")
            with open(current_file, "w", encoding="utf-8") as handle:
                handle.write("yesterday")

            for index, timestamp in ((1, 1000), (2, 2000)):
                old_dir = os.path.join(old_backup_dir, f"old_2026082{index}_020000")
                os.makedirs(old_dir)
                with open(
                    os.path.join(old_dir, "nightly.tar.gz"),
                    "w",
                    encoding="utf-8",
                ) as handle:
                    handle.write(f"old-{index}")
                os.utime(old_dir, (timestamp, timestamp))

            incoming_file = os.path.join(temp_dir, "incoming_nightly.tar.gz")
            with open(incoming_file, "w", encoding="utf-8") as handle:
                handle.write("today")

            settings = make_settings(
                backup_storage_dir=backup_dir,
                old_backup_dir=old_backup_dir,
                backup_tar="nightly.tar.gz",
                max_backups=3,
            )
            self.assertTrue(finalize_remote_backup(settings, logger, incoming_file))

            with open(current_file, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "today")
            old_dirs = sorted(
                item
                for item in os.listdir(old_backup_dir)
                if item.startswith("old_")
            )
            self.assertEqual(len(old_dirs), 2)
            old_contents = []
            for old_dir in old_dirs:
                archived_file = os.path.join(
                    old_backup_dir,
                    old_dir,
                    "nightly.tar.gz",
                )
                with open(archived_file, "r", encoding="utf-8") as handle:
                    old_contents.append(handle.read())
            self.assertCountEqual(old_contents, ["old-2", "yesterday"])
            self.assertFalse(os.path.exists(incoming_file))

    def test_legacy_old_directories_are_moved_to_new_location(self):
        logger = FakeLogger()
        with tempfile.TemporaryDirectory() as root_dir:
            backup_dir = os.path.join(root_dir, "current")
            old_backup_dir = os.path.join(root_dir, "old_backup")
            temp_dir = os.path.join(root_dir, "temp")
            os.makedirs(backup_dir)
            os.makedirs(temp_dir)

            legacy_dir = os.path.join(backup_dir, "old_20260820_020000")
            os.makedirs(legacy_dir)
            with open(
                os.path.join(legacy_dir, "nightly.tar.gz"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write("legacy")

            incoming_file = os.path.join(temp_dir, "incoming_nightly.tar.gz")
            with open(incoming_file, "w", encoding="utf-8") as handle:
                handle.write("today")

            settings = make_settings(
                backup_storage_dir=backup_dir,
                old_backup_dir=old_backup_dir,
                backup_tar="nightly.tar.gz",
                max_backups=3,
            )
            self.assertTrue(finalize_remote_backup(settings, logger, incoming_file))

            self.assertFalse(os.path.exists(legacy_dir))
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        old_backup_dir,
                        "old_20260820_020000",
                        "nightly.tar.gz",
                    )
                )
            )


if __name__ == "__main__":
    unittest.main()
