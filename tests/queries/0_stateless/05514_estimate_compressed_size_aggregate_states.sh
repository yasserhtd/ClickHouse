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

# The granules of a group are sized by the rows each block gives it, as the writer sizes the granules of a partition by its rows of
# a block: rows that get wider from one block to the next make smaller granules, in a group as in the rows of the group alone.
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1, max_block_size = 8192;
DROP TABLE IF EXISTS t_ecs_widths;
CREATE TABLE t_ecs_widths (k UInt8, s String) ENGINE = Memory;
INSERT INTO t_ecs_widths SELECT number % 2, repeat('x', 10) FROM numbers(16384);
INSERT INTO t_ecs_widths SELECT number % 2, repeat('x', 200) FROM numbers(16384);
SELECT k, e = (SELECT estimateCompressedSize('String', 'LZ4')(s) FROM t_ecs_widths WHERE k = 0) AS same_as_the_rows_alone
FROM (SELECT k, estimateCompressedSize('String', 'LZ4')(s) AS e FROM t_ecs_widths GROUP BY k)
ORDER BY k;
" -- --merge_tree.index_granularity_bytes=131072

# Aggregation in order gives a state a range of a block: the range sizes its granules, not the whole block it shares with
# other groups. Short and long strings in one block of one part, grouped either way, give the size of their own parts.
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1, max_block_size = 8192;
DROP TABLE IF EXISTS t_ecs_ranges;
DROP TABLE IF EXISTS t_ecs_ranges_parts;
CREATE TABLE t_ecs_ranges (k UInt8, s String CODEC(LZ4)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_ecs_ranges SELECT number >= 4096, repeat('x', if(number < 4096, 10, 200)) FROM numbers(8192);
CREATE TABLE t_ecs_ranges_parts (k UInt8, s String CODEC(LZ4)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_ecs_ranges_parts SELECT * FROM t_ecs_ranges;
SELECT k, column_data_compressed_bytes = ordinary AS matches_ordinary, column_data_compressed_bytes = in_order AS matches_in_order
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('String', 'LZ4')(s) AS ordinary FROM t_ecs_ranges GROUP BY k SETTINGS optimize_aggregation_in_order = 0) AS o ON toUInt8(partition) = o.k
INNER JOIN (SELECT k, estimateCompressedSize('String', 'LZ4')(s) AS in_order FROM t_ecs_ranges GROUP BY k SETTINGS optimize_aggregation_in_order = 1) AS i ON o.k = i.k
WHERE table = 't_ecs_ranges_parts' AND active AND column = 's'
ORDER BY k;
" -- --merge_tree.index_granularity_bytes=131072

# An open granule meets a block whose granules are smaller than the rows it has left: as the writer adjusts its last mark, the
# granule closes where it stands, or shrinks to the granule of the block. The merged part gets the blocks of the two parts,
# 3000 short rows then 8192 wide ones, which a UNION ALL of the two sources gives the estimate too. The estimate ends one
# compressed block more than the part in this case (16 blocks of the sizes and the characters against 17), so it is within one
# block header and checksum, 25 bytes, with keys as without.
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1, max_block_size = 8192;
DROP TABLE IF EXISTS t_ecs_open;
CREATE TABLE t_ecs_open (s String CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_ecs_open SELECT repeat('x', 10) FROM numbers(3000);
INSERT INTO t_ecs_open SELECT repeat('x', 200) FROM numbers(8192);
OPTIMIZE TABLE t_ecs_open FINAL;
SELECT abs(toInt64(column_data_compressed_bytes) - toInt64(keyless)) <= 25 AS within_a_block_header, keyless = grouped AS same_with_keys
FROM system.parts_columns
CROSS JOIN
(
    SELECT
        (SELECT estimateCompressedSize('String', 'LZ4')(s) FROM (SELECT repeat('x', 10) AS s FROM numbers(3000) UNION ALL SELECT repeat('x', 200) FROM numbers(8192))) AS keyless,
        (SELECT any(e) FROM (SELECT k, estimateCompressedSize('String', 'LZ4')(s) AS e FROM (SELECT 0 AS k, repeat('x', 10) AS s FROM numbers(3000) UNION ALL SELECT 0, repeat('x', 200) FROM numbers(8192)) GROUP BY k)) AS grouped
) AS estimates
WHERE table = 't_ecs_open' AND active AND column = 's';
" -- --merge_tree.index_granularity_bytes=131072

# A sparse column that a table stores dense (Nullable without allow_sparse) is sized dense before its rows reach the groups.
$CLICKHOUSE_LOCAL --multiquery --query "
SET max_threads = 1, allow_suspicious_fixed_string_types = 1;
DROP TABLE IF EXISTS t_ecs_dense;
CREATE TABLE t_ecs_dense (k UInt8, f Nullable(FixedString(4000)) CODEC(LZ4)) ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_ecs_dense SELECT number % 2, if(number % 50 = 0, toFixedString(toString(number), 4000), NULL) FROM numbers(1000);
SELECT k, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('Nullable(FixedString(4000))', 'LZ4')(f) AS estimate FROM t_ecs_dense GROUP BY k) AS estimates ON toUInt8(partition) = k
WHERE table = 't_ecs_dense' AND active AND column = 'f'
ORDER BY k;
" -- --merge_tree.index_granularity_bytes=4096
