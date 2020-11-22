CREATE TABLE summtt01
(
    `key` String,
    `value` String
)
ENGINE = SummingMergeTree()
ORDER BY key