-- Every query appears once in fct_query.
SELECT count(*) AS failures
FROM (
    SELECT query_id
    FROM iceberg.core.fct_query
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_id
    HAVING count(*) > 1
)
