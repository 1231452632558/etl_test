#!/usr/bin/env python3

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_common import DatabaseOperations  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
