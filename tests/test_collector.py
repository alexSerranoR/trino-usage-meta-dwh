import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from metadwh.collector import duration_ms, instant, parse_event
from metadwh.raw_events import event_values

# Payloads captured from the Trino 476 HTTP event listener, trimmed of query plans.
FIXTURE = Path(__file__).parent / "fixtures" / "trino_476_query_completed_events.json"


class DurationTest(unittest.TestCase):
    def test_airlift_format(self):
        self.assertEqual(duration_ms("1.50s"), 1500)
        self.assertEqual(duration_ms("250.00ms"), 250)
        self.assertEqual(duration_ms("2.00m"), 120_000)

    def test_iso_format(self):
        self.assertEqual(duration_ms("PT1.5S"), 1500)
        self.assertEqual(duration_ms("PT1H2M"), 3_720_000)

    def test_decimal_seconds_text(self):
        self.assertEqual(duration_ms("0.757000000"), 757)

    def test_numeric_seconds_and_null(self):
        self.assertEqual(duration_ms(0.25), 250)
        self.assertIsNone(duration_ms(None))


class InstantTest(unittest.TestCase):
    def test_iso_text(self):
        expected = datetime(2026, 11, 3, 12, 0, 0, 500000, tzinfo=timezone.utc)
        self.assertEqual(instant("2026-11-03T12:00:00.5Z"), expected)

    def test_epoch_seconds(self):
        self.assertEqual(instant(0), datetime(1970, 1, 1, tzinfo=timezone.utc))


class ParseRealEventTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        payloads = json.loads(FIXTURE.read_text(encoding="utf-8"))
        ingested = datetime(2026, 11, 3, 12, 5, tzinfo=timezone.utc)
        cls.finished, cls.failed = (parse_event(p, ingested) for p in payloads)

    def test_finished_query(self):
        event = self.finished
        self.assertEqual(event.query_id, "20261103_120241_00004_t3v5r")
        self.assertEqual((event.user_name, event.source, event.query_type), ("anna.petrova", "trino-cli", "SELECT"))
        self.assertEqual(event.cpu_time_ms, 757)
        self.assertEqual(event.create_time, datetime(2026, 11, 3, 12, 2, 41, 729000, tzinfo=timezone.utc))
        self.assertEqual(event.physical_input_bytes, 706914)

    def test_inputs_carry_tables_columns_and_bytes(self):
        inputs = {(i.schema_name, i.table_name): i for i in self.finished.inputs}
        self.assertEqual(set(inputs), {("core", "fct_query"), ("core", "dim_team")})
        self.assertEqual(inputs[("core", "fct_query")].columns, ["team_id"])
        self.assertEqual(inputs[("core", "fct_query")].physical_input_bytes, 705695)

    def test_failed_query(self):
        self.assertEqual(self.failed.query_state, "FAILED")
        self.assertEqual(self.failed.error_code, "TABLE_NOT_FOUND")
        self.assertEqual(self.failed.inputs, [])

    def test_renders_as_raw_row(self):
        self.assertIn("'anna.petrova'", event_values(self.finished))


if __name__ == "__main__":
    unittest.main()
