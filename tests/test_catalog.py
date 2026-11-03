import unittest
from datetime import date

from metadwh.catalog import build_catalog

PROFILE_COLUMNS = [
    "table_fqn", "description", "owner_team_id", "domain", "contains_pii", "as_of_date", "last_read_date",
    "queries_30d", "distinct_users_30d", "queries_7d", "queries_prev_7d", "top_teams_30d", "usage_status",
]


class FakeExecutor:
    """Returns canned results keyed by the table each statement reads."""

    def query(self, sql):
        if "table_usage_profile" in sql:
            return PROFILE_COLUMNS, [
                ("iceberg.a.used", "Used table", "team-a", "product", True, date(2026, 10, 31), date(2026, 10, 31),
                 10, 3, 4, 3, ["team-a"], "active"),
                ("iceberg.scratch.tmp", None, None, None, None, date(2026, 10, 31), None,
                 0, 0, 0, 0, [], "never_used"),
            ]
        if "UNNEST" in sql:
            return ["table_fqn", "column_name", "reads"], [("iceberg.a.used", "event_date", 9)]
        if "table_comments" in sql:
            return ["schema_name", "table_name", "comment"], [("core", "fct_query", "Executed queries.")]
        return ["table_schema", "table_name", "column_name", "data_type", "comment"], [
            ("core", "fct_query", "query_id", "varchar", "Trino query id"),
        ]


class BuildCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = build_catalog(FakeExecutor())

    def test_datasets_combine_registry_and_usage(self):
        used = self.catalog["datasets"][0]
        self.assertEqual(used["owner_team"], "team-a")
        self.assertEqual(used["usage"]["queries_30d"], 10)
        self.assertEqual(used["columns_by_usage"], [{"name": "event_date", "reads_30d": 9}])
        self.assertEqual(self.catalog["as_of_date"], "2026-10-31")

    def test_warnings_flag_pii_and_catalog_gaps(self):
        used, unregistered = self.catalog["datasets"]
        self.assertTrue(any("personal data" in w for w in used["warnings"]))
        self.assertEqual(len(unregistered["warnings"]), 3)

    def test_documents_the_warehouse_itself(self):
        (table,) = self.catalog["usage_warehouse"]
        self.assertEqual(table["description"], "Executed queries.")
        self.assertEqual(table["columns"][0]["description"], "Trino query id")


if __name__ == "__main__":
    unittest.main()
