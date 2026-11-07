#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The serialization of the shared data of JSON cuts the blocks of its data stream itself, by the offset of the stream. Without
# adaptive write buffers (a MergeTree setting of the server, so clickhouse-local is run with it), that stream has the whole
# compressed block as its buffer from the start, as in the writer, and the estimate still matches the merged part.

$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1;
DROP TABLE IF EXISTS t_ecs_no_adaptive;
CREATE TABLE t_ecs_no_adaptive (j JSON(SHARED a UInt64, SHARED b UInt64) CODEC(LZ4), p JSON CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0, ratio_of_defaults_for_sparse_serialization = 1,
         use_adaptive_write_buffer_for_dynamic_subcolumns = 0;
INSERT INTO t_ecs_no_adaptive SELECT CAST(concat('{\"a\":', toString(number), ',\"b\":', toString(number * 2), '}'), 'JSON(SHARED a UInt64, SHARED b UInt64)'), concat('{\"a\":', toString(number), ',\"b\":\"', toString(number % 100), '\"}')::JSON FROM numbers(8192);
INSERT INTO t_ecs_no_adaptive SELECT CAST(concat('{\"a\":', toString(number), ',\"b\":', toString(number * 2), '}'), 'JSON(SHARED a UInt64, SHARED b UInt64)'), concat('{\"a\":', toString(number), ',\"b\":\"', toString(number % 100), '\"}')::JSON FROM numbers(8192, 2048);
OPTIMIZE TABLE t_ecs_no_adaptive FINAL;
SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'j' AS column, estimateCompressedSize('JSON(SHARED a UInt64, SHARED b UInt64)', 'LZ4')(j) AS estimate FROM t_ecs_no_adaptive
    UNION ALL SELECT 'p', estimateCompressedSize('JSON', 'LZ4')(p) FROM t_ecs_no_adaptive
) AS estimates USING (column)
WHERE table = 't_ecs_no_adaptive' AND active
ORDER BY column;
" -- --merge_tree.use_adaptive_write_buffer_for_dynamic_subcolumns=0 --merge_tree.index_granularity_bytes=0
