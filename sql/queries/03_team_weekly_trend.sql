-- Historical view: weekly CPU and distinct users per team, with week-over-week change.
-- days_in_period flags partial weeks at the edges of the period.
WITH weekly AS (
    SELECT
        date_trunc('week', usage_date)                      AS week,
        team_id,
        sum(cpu_time_ms) / 3.6E6                            AS cpu_hours,
        cardinality(merge(CAST(users_hll AS HyperLogLog)))  AS distinct_users
    FROM iceberg.marts.team_usage_daily
    WHERE usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY 1, 2
)
SELECT
    week,
    team_id,
    date_diff('day',
              greatest(week, DATE '${start_date}'),
              least(week + INTERVAL '6' DAY, DATE '${end_date}')) + 1  AS days_in_period,
    round(cpu_hours, 2)                                    AS cpu_hours,
    distinct_users,
    round(100 * (cpu_hours / nullif(lag(cpu_hours) OVER (PARTITION BY team_id ORDER BY week), 0) - 1), 1)
                                                            AS cpu_change_pct
FROM weekly
ORDER BY team_id, week
