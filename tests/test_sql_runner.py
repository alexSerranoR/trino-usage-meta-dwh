import unittest

from metadwh.sql_runner import render, split_statements


class RenderTest(unittest.TestCase):
    def test_substitutes_parameters(self):
        sql = "SELECT * FROM t WHERE d BETWEEN DATE '${start_date}' AND DATE '${end_date}'"
        rendered = render(sql, {"start_date": "2026-10-01", "end_date": "2026-10-07"})
        self.assertEqual(rendered, "SELECT * FROM t WHERE d BETWEEN DATE '2026-10-01' AND DATE '2026-10-07'")

    def test_missing_parameter_fails_loudly(self):
        with self.assertRaises(KeyError):
            render("SELECT '${start_date}'", {})


class SplitStatementsTest(unittest.TestCase):
    def test_splits_on_terminating_semicolons(self):
        script = "-- header\nDELETE FROM t;\n\nINSERT INTO t\nSELECT 1;\n"
        self.assertEqual(split_statements(script), ["-- header\nDELETE FROM t", "INSERT INTO t\nSELECT 1"])

    def test_ignores_comment_only_chunks(self):
        self.assertEqual(split_statements("SELECT 1;\n-- trailing comment\n"), ["SELECT 1"])

    def test_keeps_semicolons_inside_a_line(self):
        self.assertEqual(split_statements("SELECT 'a;b' AS x;\n"), ["SELECT 'a;b' AS x"])


if __name__ == "__main__":
    unittest.main()
