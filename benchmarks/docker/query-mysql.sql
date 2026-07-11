SELECT
    id,
    created_at,
    amount,
    category,
    payload,
    CAST(active AS SIGNED) AS active,
    nullable_value
FROM benchmark_events
