#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part with min_columns_to_activate_adaptive_write_buffer streams or more gives every stream an adaptive buffer, whose blocks
# start small. The estimate knows the streams of its column alone: when they reach the threshold, so does the part.

# The threshold at 1 (a MergeTree setting of the server, so clickhouse-local is run with it): a String column, two streams.
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1;
DROP TABLE IF EXISTS t_ecs_adaptive;
CREATE TABLE t_ecs_adaptive (s String CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, min_columns_to_activate_adaptive_write_buffer = 1;
INSERT INTO t_ecs_adaptive SELECT toString(number % 1000) FROM numbers(24576);
SELECT column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
CROSS JOIN (SELECT estimateCompressedSize('String', 'LZ4')(s) AS estimate FROM t_ecs_adaptive) AS estimates
WHERE table = 't_ecs_adaptive' AND active AND column = 's';
" -- --merge_tree.min_columns_to_activate_adaptive_write_buffer=1

# The default threshold, 500: a Tuple of 500 numbers has 500 streams.
elements=$(seq -s ', ' 1 500 | sed 's/[0-9]\+/UInt16/g')
values=$(seq -s ', ' 1 500 | sed 's/\([0-9]\+\)/toUInt16(number * \1)/g')
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1;
DROP TABLE IF EXISTS t_ecs_streams;
CREATE TABLE t_ecs_streams (t Tuple($elements) CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_ecs_streams SELECT tuple($values) FROM numbers(24576);
SELECT column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
CROSS JOIN (SELECT estimateCompressedSize('Tuple($elements)', 'LZ4')(t) AS estimate FROM t_ecs_streams) AS estimates
WHERE table = 't_ecs_streams' AND active AND column = 't';
"
