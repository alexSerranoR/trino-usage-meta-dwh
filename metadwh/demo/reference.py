"""Reference data for the demo: teams, users and the table registry."""

from __future__ import annotations

from dataclasses import dataclass, field

TEAMS: list[tuple[str, str, str]] = [
    ("product-analytics", "Product Analytics", "CC-110"),
    ("data-platform", "Data Platform", "CC-120"),
    ("ai-assistant", "AI Assistant", "CC-130"),
    ("marketing-analytics", "Marketing Analytics", "CC-140"),
    ("ide-growth", "IDE Growth", "CC-150"),
    ("finance", "Finance", "CC-160"),
]


@dataclass(frozen=True)
class User:
    name: str
    team_id: str
    account_type: str = "human"
    activity: float = 1.0
    sources: tuple[str, ...] = ("trino-cli", "datagrip", "jupyter")
    # Optional team change: (day offset from the start of the period, new team).
    moves_to: tuple[int, str] | None = None
    # Users absent from the company directory end up as 'unassigned'.
    in_directory: bool = True


USERS: list[User] = [
    User("anna.petrova", "product-analytics", activity=1.6),
    User("david.kim", "product-analytics", activity=1.2),
    User("lucia.romero", "product-analytics"),
    User("tomas.novak", "product-analytics", activity=0.8),
    User("svc_tableau", "product-analytics", "service", activity=6.0, sources=("tableau",)),
    User("jan.muller", "data-platform", activity=0.9),
    User("olga.ivanova", "data-platform", activity=0.7),
    User("svc_airflow", "data-platform", "service", activity=5.0, sources=("airflow",)),
    User("pierre.martin", "ai-assistant", activity=1.1),
    User("sara.conti", "ai-assistant"),
    User("svc_ai_agent", "ai-assistant", "service", activity=7.0, sources=("ai-agent",)),
    User("maria.lopez", "marketing-analytics", activity=1.3, moves_to=(14, "product-analytics")),
    User("emma.wilson", "marketing-analytics"),
    User("liam.brown", "marketing-analytics", activity=0.6),
    User("noah.fischer", "ide-growth", activity=1.2),
    User("mia.jensen", "ide-growth", activity=0.9),
    User("eva.horvat", "finance", activity=0.8),
    User("oliver.smith", "finance", activity=0.5),
    User("contractor.temp", "ide-growth", activity=0.4, in_directory=False),
]


@dataclass(frozen=True)
class Table:
    fqn: str
    owner_team_id: str | None
    domain: str
    description: str
    columns: tuple[str, ...]
    scan_bytes_median: float
    contains_pii: bool = False
    registered: bool = True
    readers: dict[str, float] = field(default_factory=dict)


_GB = 1024**3
_MB = 1024**2

TABLES: list[Table] = [
    Table(
        "iceberg.product_events.ide_sessions", "data-platform", "product",
        "One row per IDE session with product, version, OS and duration.",
        ("event_date", "product_code", "session_id", "ide_version", "os", "duration_sec", "country"),
        6 * _GB,
        readers={"product-analytics": 5, "ide-growth": 6, "ai-assistant": 2, "data-platform": 2},
    ),
    Table(
        "iceberg.product_events.feature_usage", "data-platform", "product",
        "Feature invocation events from the IDEs, one row per action.",
        ("event_date", "product_code", "feature_id", "session_id", "action_count", "plugin_id"),
        9 * _GB,
        readers={"product-analytics": 6, "ide-growth": 4, "ai-assistant": 3},
    ),
    Table(
        "iceberg.product_events.plugin_installs", "data-platform", "product",
        "Plugin install and uninstall events from the Marketplace client.",
        ("event_date", "plugin_id", "product_code", "action", "plugin_version"),
        800 * _MB,
        readers={"ide-growth": 5, "product-analytics": 2},
    ),
    Table(
        "iceberg.product_events.crash_reports", "data-platform", "product",
        "Anonymized IDE crash reports with stack signature and build.",
        ("event_date", "product_code", "build", "signature", "os"),
        1.5 * _GB,
        readers={"ide-growth": 2, "product-analytics": 1},
    ),
    Table(
        "iceberg.product_events.license_activations", "finance", "product",
        "License activations per account; links product usage to customers.",
        ("event_date", "license_id", "account_id", "product_code", "email"),
        400 * _MB, contains_pii=True,
        readers={"finance": 4, "product-analytics": 1, "marketing-analytics": 1},
    ),
    Table(
        "iceberg.analytics.daily_active_users", "product-analytics", "product",
        "Daily active users per product and country. Built by Airflow.",
        ("event_date", "product_code", "country", "dau"),
        60 * _MB,
        readers={"product-analytics": 6, "ide-growth": 3, "marketing-analytics": 2, "finance": 1},
    ),
    Table(
        "iceberg.analytics.retention_cohorts", "product-analytics", "product",
        "Weekly retention by signup cohort and product.",
        ("cohort_week", "product_code", "weeks_since_signup", "retained_users"),
        120 * _MB,
        readers={"product-analytics": 4, "ide-growth": 2},
    ),
    Table(
        "iceberg.analytics.feature_adoption", "product-analytics", "product",
        "Share of active users using each feature per week.",
        ("week", "product_code", "feature_id", "adoption_rate"),
        90 * _MB,
        readers={"product-analytics": 4, "ai-assistant": 2},
    ),
    Table(
        "iceberg.analytics.product_kpis", "product-analytics", "product",
        "Executive KPI table: users, revenue and retention per product and month.",
        ("month", "product_code", "active_users", "revenue_eur", "retention_rate"),
        15 * _MB,
        readers={"product-analytics": 3, "finance": 2, "marketing-analytics": 1},
    ),
    Table(
        "iceberg.marketing.web_sessions", "marketing-analytics", "marketing",
        "Website sessions with landing page and campaign attribution.",
        ("event_date", "session_id", "landing_page", "utm_campaign", "country"),
        3 * _GB,
        readers={"marketing-analytics": 6},
    ),
    Table(
        "iceberg.marketing.campaign_touchpoints", "marketing-analytics", "marketing",
        "Marketing touchpoints per contact and campaign.",
        ("event_date", "contact_id", "campaign_id", "channel", "email"),
        700 * _MB, contains_pii=True,
        readers={"marketing-analytics": 4},
    ),
    Table(
        "iceberg.marketing.newsletter_subscribers", "marketing-analytics", "marketing",
        "Current newsletter subscribers and their preferences.",
        ("contact_id", "email", "subscribed_at", "language"),
        50 * _MB, contains_pii=True,
        readers={"marketing-analytics": 2},
    ),
    Table(
        "iceberg.finance.subscriptions", "finance", "finance",
        "Active and churned subscriptions per customer account.",
        ("account_id", "plan", "status", "start_date", "end_date", "mrr_eur"),
        300 * _MB, contains_pii=True,
        readers={"finance": 6, "product-analytics": 1},
    ),
    Table(
        "iceberg.finance.invoices", "finance", "finance",
        "Issued invoices with amounts and payment status.",
        ("invoice_id", "account_id", "issued_at", "amount_eur", "status"),
        500 * _MB,
        readers={"finance": 5},
    ),
    Table(
        "iceberg.finance.revenue_daily", "finance", "finance",
        "Recognized revenue per product and day.",
        ("revenue_date", "product_code", "revenue_eur"),
        25 * _MB,
        readers={"finance": 4, "product-analytics": 2},
    ),
    Table(
        "iceberg.ai.assistant_requests", "ai-assistant", "ai",
        "AI Assistant requests with model, latency and outcome.",
        ("event_date", "request_id", "model", "latency_ms", "accepted", "product_code"),
        4 * _GB,
        readers={"ai-assistant": 7, "product-analytics": 1},
    ),
    Table(
        "iceberg.ai.token_usage", "ai-assistant", "ai",
        "Tokens consumed per request and model, used for cost tracking.",
        ("event_date", "request_id", "model", "input_tokens", "output_tokens"),
        2 * _GB,
        readers={"ai-assistant": 5, "finance": 1},
    ),
    Table(
        "iceberg.ai.model_feedback", "ai-assistant", "ai",
        "Explicit user feedback on AI Assistant answers.",
        ("event_date", "request_id", "rating", "comment_category"),
        150 * _MB,
        readers={"ai-assistant": 3},
    ),
    Table(
        "iceberg.common.products", "data-platform", "common",
        "Product catalog: product codes, names and families.",
        ("product_code", "product_name", "family"),
        1 * _MB,
        readers={team_id: 1.5 for team_id, _, _ in TEAMS},
    ),
    Table(
        "iceberg.common.countries", "data-platform", "common",
        "ISO country codes with region and sales territory.",
        ("country", "country_name", "region"),
        1 * _MB,
        readers={team_id: 1 for team_id, _, _ in TEAMS},
    ),
    Table(
        "iceberg.analytics.legacy_dau_v1", "product-analytics", "product",
        "Deprecated first version of daily active users. Superseded by daily_active_users.",
        ("event_date", "product_code", "dau"),
        40 * _MB,
    ),
    Table(
        "iceberg.marketing.campaigns_2022", "marketing-analytics", "marketing",
        "Archived 2022 campaign results.",
        ("campaign_id", "channel", "spend_eur", "conversions"),
        20 * _MB,
    ),
    Table(
        "iceberg.scratch.tmp_session_experiment", None, "scratch",
        "",
        ("session_id", "variant", "converted"),
        200 * _MB, registered=False,
        readers={"ide-growth": 1.5},
    ),
]

# Tables written by the scheduled Airflow jobs.
AIRFLOW_OUTPUTS = [
    ("iceberg.analytics.daily_active_users", ["iceberg.product_events.ide_sessions", "iceberg.common.products"]),
    ("iceberg.analytics.retention_cohorts", ["iceberg.product_events.ide_sessions"]),
    (
        "iceberg.analytics.feature_adoption",
        ["iceberg.product_events.feature_usage", "iceberg.analytics.daily_active_users"],
    ),
    ("iceberg.analytics.product_kpis", ["iceberg.analytics.daily_active_users", "iceberg.finance.revenue_daily"]),
    ("iceberg.finance.revenue_daily", ["iceberg.finance.invoices", "iceberg.finance.subscriptions"]),
]
