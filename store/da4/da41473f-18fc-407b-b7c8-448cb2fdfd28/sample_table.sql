ATTACH TABLE _ UUID '64a912c2-5f7f-4270-8ec9-c56077462574'
(
    `id` UInt32,
    `name` String,
    `value` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
