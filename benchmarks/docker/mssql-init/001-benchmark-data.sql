IF DB_ID(N'benchmark') IS NULL
BEGIN
    CREATE DATABASE benchmark;
END;
GO

USE benchmark;
GO

IF OBJECT_ID(N'dbo.benchmark_events', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.benchmark_events (
        id BIGINT NOT NULL PRIMARY KEY,
        created_at DATETIME2(6) NOT NULL,
        amount FLOAT NOT NULL,
        category VARCHAR(20) NOT NULL,
        payload VARCHAR(64) NOT NULL,
        active BIT NOT NULL,
        nullable_value BIGINT NULL
    );

    WITH generated AS (
        SELECT TOP (8000000)
            CONVERT(BIGINT, ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS value
        FROM sys.all_objects AS a
        CROSS JOIN sys.all_objects AS b
        CROSS JOIN sys.all_objects AS c
    )
    INSERT INTO dbo.benchmark_events WITH (TABLOCK)
    SELECT
        value,
        DATEADD(SECOND, value, CONVERT(DATETIME2(6), '2020-01-01T00:00:00')),
        CONVERT(FLOAT, (value * 2654435761) % 1000000) / 100.0,
        CONCAT('category-', value % 100),
        CONCAT(REPLICATE('x', 55), RIGHT(CONCAT('000000000', value), 9)),
        CONVERT(BIT, CASE WHEN value % 3 <> 0 THEN 1 ELSE 0 END),
        CASE WHEN value % 11 = 0 THEN NULL ELSE value * 7 END
    FROM generated;

    UPDATE STATISTICS dbo.benchmark_events WITH FULLSCAN;
END;
GO
