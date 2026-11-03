import unittest
from datetime import date
from itertools import pairwise

from metadwh import pipeline


class DateChunksTest(unittest.TestCase):
    def test_covers_range_without_gaps_or_overlaps(self):
        chunks = pipeline.date_chunks(date(2026, 10, 1), date(2026, 10, 31), 7)
        self.assertEqual(chunks[0], (date(2026, 10, 1), date(2026, 10, 7)))
        self.assertEqual(chunks[-1], (date(2026, 10, 29), date(2026, 10, 31)))
        for (_, previous_end), (next_start, _) in pairwise(chunks):
            self.assertEqual((next_start - previous_end).days, 1)

    def test_single_day(self):
        self.assertEqual(pipeline.date_chunks(date(2026, 10, 5), date(2026, 10, 5), 7),
                         [(date(2026, 10, 5), date(2026, 10, 5))])

    def test_rejects_inverted_range(self):
        with self.assertRaises(ValueError):
            pipeline.date_chunks(date(2026, 10, 5), date(2026, 10, 1), 7)


class ContiguousRangesTest(unittest.TestCase):
    def test_groups_consecutive_days(self):
        days = [date(2026, 10, d) for d in (5, 1, 2, 3, 9, 10)]
        self.assertEqual(
            pipeline.contiguous_ranges(days),
            [(date(2026, 10, 1), date(2026, 10, 3)), (date(2026, 10, 5), date(2026, 10, 5)),
             (date(2026, 10, 9), date(2026, 10, 10))],
        )

    def test_empty(self):
        self.assertEqual(pipeline.contiguous_ranges([]), [])


class LogicVersionTest(unittest.TestCase):
    def test_ignores_comments_and_formatting(self):
        a = "-- old comment\nSELECT a,\n       b\nFROM t"
        b = "-- reworded comment\nSELECT a, b FROM t  "
        self.assertEqual(pipeline.normalized_sql(a), pipeline.normalized_sql(b))

    def test_detects_logic_changes(self):
        self.assertNotEqual(pipeline.normalized_sql("SELECT a FROM t"), pipeline.normalized_sql("SELECT b FROM t"))

    def test_is_stable_short_hash(self):
        self.assertEqual(pipeline.logic_version(), pipeline.logic_version())
        self.assertEqual(len(pipeline.logic_version()), 12)


class SqlLayoutTest(unittest.TestCase):
    def test_raw_is_read_from_one_snapshot_per_run(self):
        for path in [pipeline.INCREMENTAL_DIR / "10_staging_query_completed.sql",
                     pipeline.OPS_DIR / "record_builds.sql", pipeline.OPS_DIR / "plan.sql"]:
            self.assertIn("FOR VERSION AS OF ${raw_snapshot_id}", path.read_text(encoding="utf-8"), path.name)

    def test_incremental_steps_take_only_window_parameters(self):
        for path in pipeline.INCREMENTAL_DIR.glob("*.sql"):
            sql = path.read_text(encoding="utf-8")
            self.assertIn("${start_date}", sql, path.name)
            self.assertIn("DELETE FROM", sql, f"{path.name} must replace its partitions to stay idempotent")

    def test_every_check_returns_a_failures_column(self):
        for path in pipeline.CHECKS_DIR.glob("*.sql"):
            self.assertIn("AS failures", path.read_text(encoding="utf-8"), path.name)


if __name__ == "__main__":
    unittest.main()
