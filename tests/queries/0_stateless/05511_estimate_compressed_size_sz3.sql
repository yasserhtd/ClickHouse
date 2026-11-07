-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: the SZ3 codec is not in the fast test build. no-random-merge-tree-settings: the estimate is compared byte for
-- byte with a part written with the server's MergeTree settings.

-- A codec that compresses vectors takes their dimension from the column, as the writer sets it for every block.

SET max_threads = 1, enable_sz3_codec = 1, allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_ecs_sz3;
CREATE TABLE t_ecs_sz3 (v Array(Float32) CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.001)), f Array(Float32) CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_sz3 SELECT arrayMap(i -> sin(number / 100 + i / 10), range(16)), arrayMap(i -> sin(number / 100 + i / 10), range(16)) FROM numbers(8192);

-- As itself, and from a column of the same values with another codec.
SELECT
    column_data_compressed_bytes = estimate AS matches_on_disk_size,
    column_data_compressed_bytes = converted AS matches_converted
FROM system.parts_columns
CROSS JOIN
(
    SELECT
        estimateCompressedSize('Array(Float32)', 'SZ3(''ALGO_LORENZO_REG'', ''ABS'', 0.001)')(v) AS estimate,
        estimateCompressedSize('Array(Float32)', 'SZ3(''ALGO_LORENZO_REG'', ''ABS'', 0.001)')(f) AS converted
    FROM t_ecs_sz3
) AS estimates
WHERE database = currentDatabase() AND table = 't_ecs_sz3' AND active AND column = 'v';

-- With keys, each group sets the dimension for its own codec.
SELECT count(), min(estimate) > 0 FROM (SELECT estimateCompressedSize('Array(Float32)', 'SZ3(''ALGO_LORENZO_REG'', ''ABS'', 0.001)')(v) AS estimate FROM t_ecs_sz3 GROUP BY length(v) % 2);

DROP TABLE t_ecs_sz3;
