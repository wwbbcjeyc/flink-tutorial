CREATE TABLE default.sensor (
id String,
serverTime UInt64,
temperature String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDate(serverTime))
ORDER BY (id)
SAMPLE BY id
SETTINGS index_granularity = 8192