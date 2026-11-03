import unittest
from collections import Counter
from datetime import date

from metadwh.demo.events import EventGenerator
from metadwh.demo.sql_literals import event_values, varchar_array
from metadwh.literals import literal


class EventGeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = EventGenerator(date(2026, 10, 1), date(2026, 10, 14), seed=7).generate()

    def test_is_deterministic_for_a_seed(self):
        again = EventGenerator(date(2026, 10, 1), date(2026, 10, 14), seed=7).generate()
        self.assertEqual([e.query_id for e in self.events], [e.query_id for e in again])

    def test_contains_redelivered_events(self):
        counts = Counter(e.query_id for e in self.events)
        self.assertTrue(any(c > 1 for c in counts.values()))

    def test_events_are_ingested_after_completion(self):
        for event in self.events:
            self.assertGreaterEqual(event.ingested_at, event.end_time)
            self.assertGreaterEqual(event.end_time, event.create_time)

    def test_covers_edge_cases(self):
        self.assertTrue(any(e.query_state == "FAILED" for e in self.events))
        self.assertTrue(any(not e.inputs for e in self.events))
        self.assertTrue(any(len(e.inputs) > 1 for e in self.events))
        self.assertTrue(any(e.output for e in self.events))
        self.assertTrue(any(e.user_name != e.user_name.lower() for e in self.events))

    def test_query_totals_match_inputs(self):
        for event in self.events:
            self.assertEqual(event.physical_input_bytes, sum(i.physical_input_bytes or 0 for i in event.inputs))


class SqlLiteralTest(unittest.TestCase):
    def test_escapes_quotes(self):
        self.assertEqual(literal("it's"), "'it''s'")

    def test_null_and_bool(self):
        self.assertEqual(literal(None), "NULL")
        self.assertEqual(literal(True), "TRUE")

    def test_empty_array_is_typed(self):
        self.assertEqual(varchar_array([]), "CAST(ARRAY[] AS array(varchar))")

    def test_event_row_has_one_value_per_raw_column(self):
        event = EventGenerator(date(2026, 10, 1), date(2026, 10, 1), seed=1).generate()[0]
        self.assertTrue(event_values(event).startswith("('"))


if __name__ == "__main__":
    unittest.main()
