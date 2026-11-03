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


class SqlLayoutTest(unittest.TestCase):
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
