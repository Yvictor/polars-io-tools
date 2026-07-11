\timing on

CREATE UNLOGGED TABLE benchmark_events AS
SELECT
    value AS id,
    TIMESTAMP '2020-01-01 00:00:00' + value * INTERVAL '1 second' AS created_at,
    ((value * 2654435761) % 1000000)::double precision / 100.0 AS amount,
    'category-' || (value % 100)::text AS category,
    md5(value::text) || md5((value * 17)::text) AS payload,
    value % 3 <> 0 AS active,
    CASE WHEN value % 11 = 0 THEN NULL ELSE value * 7 END AS nullable_value
FROM generate_series(1, 8000000) AS value;

ALTER TABLE benchmark_events ADD PRIMARY KEY (id);
ANALYZE benchmark_events;
