CREATE TABLE benchmark_events (
    id BIGINT NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL,
    amount DOUBLE NOT NULL,
    category VARCHAR(20) NOT NULL,
    payload VARCHAR(64) NOT NULL,
    active BOOLEAN NOT NULL,
    nullable_value BIGINT NULL
) ENGINE=InnoDB;

CREATE TABLE digits (digit INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO digits VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

INSERT INTO benchmark_events
SELECT
    value AS id,
    TIMESTAMPADD(SECOND, value, TIMESTAMP('2020-01-01 00:00:00')) AS created_at,
    MOD(value * 2654435761, 1000000) / 100.0 AS amount,
    CONCAT('category-', MOD(value, 100)) AS category,
    CONCAT(MD5(value), MD5(value * 17)) AS payload,
    MOD(value, 3) <> 0 AS active,
    IF(MOD(value, 11) = 0, NULL, value * 7) AS nullable_value
FROM (
    SELECT
        1 + d0.digit
        + d1.digit * 10
        + d2.digit * 100
        + d3.digit * 1000
        + d4.digit * 10000
        + d5.digit * 100000
        + d6.digit * 1000000 AS value
    FROM digits AS d0
    CROSS JOIN digits AS d1
    CROSS JOIN digits AS d2
    CROSS JOIN digits AS d3
    CROSS JOIN digits AS d4
    CROSS JOIN digits AS d5
    CROSS JOIN digits AS d6
) AS generated_rows
WHERE value <= 8000000;

DROP TABLE digits;
ANALYZE TABLE benchmark_events;
