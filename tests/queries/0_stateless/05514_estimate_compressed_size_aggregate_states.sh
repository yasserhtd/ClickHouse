#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The writer sizes the granules of a block by the bytes of its columns as they will be written; for aggregate function states,
# which live in an arena the column does not count, it sizes them as serialized. The estimate counts them the same way, so
# states straight from an aggregation give the size of the part they would make. The granularity in bytes is a MergeTree
# setting of the server, so clickhouse-local is run with a small one.

$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1;
DROP TABLE IF EXISTS t_ecs_states_src;
DROP TABLE IF EXISTS t_ecs_states;
CREATE TABLE t_ecs_states_src (st AggregateFunction(groupArray, String)) ENGINE = Memory;
INSERT INTO t_ecs_states_src SELECT groupArrayState(repeat(char(97 + number % 26), 4096)) FROM numbers(1024) GROUP BY number;
CREATE TABLE t_ecs_states (st AggregateFunction(groupArray, String) CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity_bytes = 4096;
INSERT INTO t_ecs_states SELECT st FROM t_ecs_states_src;
SELECT
    column_data_compressed_bytes = from_memory AS matches_from_memory,
    column_data_compressed_bytes = from_part AS matches_from_part
FROM system.parts_columns
CROSS JOIN
(
    SELECT
        (SELECT estimateCompressedSize('AggregateFunction(groupArray, String)', 'LZ4')(st) FROM t_ecs_states_src) AS from_memory,
        (SELECT estimateCompressedSize('AggregateFunction(groupArray, String)', 'LZ4')(st) FROM t_ecs_states) AS from_part
) AS estimates
WHERE table = 't_ecs_states' AND active AND column = 'st';
" -- --merge_tree.index_granularity_bytes=4096
