-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the estimates are compared byte for byte with parts written with the server's MergeTree settings,
-- which the function mirrors, and with a fixed granularity and fixed compressed block sizes.

-- `estimateCompressedSize` returns the compressed size of a column stored as a given type: the rows are converted to the target
-- type and compressed as MergeTree writes a wide part. The type of the argument plays no part in the result, so the estimates of
-- one column for different types and codecs are comparable with each other and with the sizes in `system.parts_columns`.
-- The target type is always named, so every estimate says which representation it measures.

SET max_threads = 1, max_block_size = 8192, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- A table takes the compressed block sizes from the query when its own are not set, and so does the estimate.
SET min_compress_block_size = 65536, max_compress_block_size = 1048576;

DROP TABLE IF EXISTS t_ecs;
DROP TABLE IF EXISTS t_ecs_parts;

-- One part of three granules; the keys interleave, so GROUP BY adds the rows one at a time. Every column holds the same values
-- as its neighbour in another type.
CREATE TABLE t_ecs (k UInt32, s String CODEC(LZ4), lc LowCardinality(String) CODEC(LZ4), ip4 IPv4 CODEC(LZ4), ip6 IPv6 CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs
SELECT number % 3, concat('value_', toString(number % 16)), concat('value_', toString(number % 16)), toIPv4(number * 7919), toIPv6(toIPv4(number * 7919))
FROM numbers(3 * 8192);

-- The same rows, one part per key, each of one granule: what a group of GROUP BY would be on disk.
CREATE TABLE t_ecs_parts (k UInt32, s String CODEC(LZ4), lc LowCardinality(String) CODEC(LZ4), ip4 IPv4 CODEC(LZ4), ip6 IPv6 CODEC(LZ4))
ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_parts SELECT * FROM t_ecs;

SELECT 'The size of a part, estimated from the column itself';

SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 's' AS column, estimateCompressedSize('String', 'LZ4', 65536)(s) AS estimate FROM t_ecs
    UNION ALL
    SELECT 'lc' AS column, estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(lc) AS estimate FROM t_ecs
    UNION ALL
    SELECT 'ip4' AS column, estimateCompressedSize('IPv4', 'LZ4', 65536)(ip4) AS estimate FROM t_ecs
    UNION ALL
    SELECT 'ip6' AS column, estimateCompressedSize('IPv6', 'LZ4', 65536)(ip6) AS estimate FROM t_ecs
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs' AND active
ORDER BY column;

SELECT 'The size of a part, estimated from a column of another type holding the same values';

-- String as LowCardinality(String), LowCardinality(String) as String, IPv4 as IPv6, IPv6 as IPv4.
SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'lc' AS column, estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) AS estimate FROM t_ecs
    UNION ALL
    SELECT 's' AS column, estimateCompressedSize('String', 'LZ4', 65536)(lc) AS estimate FROM t_ecs
    UNION ALL
    SELECT 'ip6' AS column, estimateCompressedSize('IPv6', 'LZ4', 65536)(ip4) AS estimate FROM t_ecs
    UNION ALL
    SELECT 'ip4' AS column, estimateCompressedSize('IPv4', 'LZ4', 65536)(ip6) AS estimate FROM t_ecs
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs' AND active
ORDER BY column;

-- The codec and the block size can be given in any order after the type. The type of the argument plays no part: the
-- LowCardinality(String) column and the String column give the same sizes for the same target types.
SELECT
    estimateCompressedSize('LowCardinality(String)', 65536, 'LZ4')(s) = estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s),
    estimateCompressedSize('String', 'LZ4', 65536)(lc) = estimateCompressedSize('String', 'LZ4', 65536)(s),
    estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(lc) = estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s)
FROM t_ecs;

-- Comparable sizes: dictionary encoding stores these 16 values in fewer bytes, the mapped IPv6 addresses take more bytes than IPv4.
SELECT
    estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) < estimateCompressedSize('String', 'LZ4', 65536)(s),
    estimateCompressedSize('IPv6', 'LZ4', 65536)(ip4) > estimateCompressedSize('IPv4', 'LZ4', 65536)(ip4)
FROM t_ecs;

SELECT 'A sparse column: measured sparse when the target type can be stored sparse, dense otherwise';

DROP TABLE IF EXISTS t_ecs_sparse;
CREATE TABLE t_ecs_sparse (k UInt32, s String CODEC(LZ4), lc LowCardinality(String) CODEC(LZ4), n UInt64 CODEC(LZ4), m Int64 CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_ecs_sparse SELECT number % 3, if(number % 1000 = 0, toString(number), '') AS s, s, if(number % 1000 = 0, number, 0) AS n, n FROM numbers(3 * 8192);

-- `s` and `n` are stored sparse, `lc` and `m` hold the same values: `lc` dense, as `LowCardinality` cannot be sparse, `m` sparse.
SELECT column, serialization_kind, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 's' AS column, estimateCompressedSize('String', 'LZ4', 65536)(s) AS estimate FROM t_ecs_sparse
    UNION ALL
    SELECT 'lc' AS column, estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) AS estimate FROM t_ecs_sparse
    UNION ALL
    SELECT 'n' AS column, estimateCompressedSize('UInt64', 'LZ4', 65536)(n) AS estimate FROM t_ecs_sparse
    UNION ALL
    SELECT 'm' AS column, estimateCompressedSize('Int64', 'LZ4', 65536)(n) AS estimate FROM t_ecs_sparse
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_sparse' AND active
ORDER BY column;

-- The sparse String column is smaller than its dictionary encoding here, and the estimates say so.
SELECT estimateCompressedSize('String', 'LZ4', 65536)(s) < estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs_sparse;

SELECT
    (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs_sparse SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs_sparse),
    (SELECT estimateCompressedSize('UInt64', 'LZ4', 65536)(n) FROM t_ecs_sparse SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('UInt64', 'LZ4', 65536)(n) FROM t_ecs_sparse),
    (SELECT estimateCompressedSize('Int64', 'LZ4', 65536)(n) FROM t_ecs_sparse SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('Int64', 'LZ4', 65536)(n) FROM t_ecs_sparse);

-- With keys, the sparse column reaches the state of each group one row at a time, and stays sparse.
DROP TABLE IF EXISTS t_ecs_sparse_parts;
CREATE TABLE t_ecs_sparse_parts (k UInt32, n UInt64 CODEC(LZ4), m Int64 CODEC(LZ4))
ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_ecs_sparse_parts SELECT k, n, m FROM t_ecs_sparse;

SELECT k, serialization_kind, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('Int64', 'LZ4', 65536)(n) AS estimate FROM t_ecs_sparse GROUP BY k) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecs_sparse_parts' AND active AND column = 'm'
ORDER BY k;

-- Through -State as well: the states of the groups merge into the sum of the sparse parts.
SELECT estimateCompressedSizeMerge('Int64', 'LZ4', 65536)(st)
    = (SELECT sum(column_data_compressed_bytes) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecs_sparse_parts' AND active AND column = 'm')
FROM (SELECT estimateCompressedSizeState('Int64', 'LZ4', 65536)(n) AS st FROM t_ecs_sparse GROUP BY k);

DROP TABLE t_ecs_sparse_parts;
DROP TABLE t_ecs_sparse;

-- A column stored sparse in one part and dense in another is estimated as the two parts it is, with and without keys.
DROP TABLE IF EXISTS t_ecs_mixed;
CREATE TABLE t_ecs_mixed (k UInt8, n UInt64 CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_ecs_mixed SELECT 0, if(number % 1000 = 0, number, 0) FROM numbers(8192);
INSERT INTO t_ecs_mixed SELECT 0, number * 7919 FROM numbers(8192);

SELECT
    groupArray(serialization_kind),
    (SELECT estimateCompressedSize('UInt64', 'LZ4', 65536)(n) FROM t_ecs_mixed) = sum(column_data_compressed_bytes),
    (SELECT any(estimate) FROM (SELECT k, estimateCompressedSize('UInt64', 'LZ4', 65536)(n) AS estimate FROM t_ecs_mixed GROUP BY k)) = sum(column_data_compressed_bytes)
FROM (SELECT serialization_kind, column_data_compressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecs_mixed' AND active AND column = 'n' ORDER BY name);

DROP TABLE t_ecs_mixed;

SELECT 'The rows are serialized one granule at a time, whatever the block size';

-- A granule that a block leaves incomplete is completed with the rows of the next block, also for a numeric dictionary.
SELECT
    (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs SETTINGS max_block_size = 8192),
    (SELECT estimateCompressedSize('String', 'LZ4', 65536)(lc) FROM t_ecs SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('String', 'LZ4', 65536)(lc) FROM t_ecs SETTINGS max_block_size = 8192),
    (SELECT estimateCompressedSize('IPv6', 'LZ4', 65536)(ip4) FROM t_ecs SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('IPv6', 'LZ4', 65536)(ip4) FROM t_ecs SETTINGS max_block_size = 8192),
    (SELECT estimateCompressedSize('LowCardinality(UInt32)', 'LZ4', 65536)(toUInt32(number % 16)) FROM numbers(10000) SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('LowCardinality(UInt32)', 'LZ4', 65536)(toUInt32(number % 16)) FROM numbers(10000) SETTINGS max_block_size = 8192),
    (SELECT estimateCompressedSize('LowCardinality(UInt32)', 'LZ4', 65536)(toLowCardinality(toUInt32(number % 16))) FROM numbers(10000) SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('LowCardinality(UInt32)', 'LZ4', 65536)(toUInt32(number % 16)) FROM numbers(10000) SETTINGS max_block_size = 8192);

-- Wide rows: a block of 8192 rows that holds more bytes than index_granularity_bytes is one granule, as the writer sizes it. With
-- the default compressed block sizes, a block ends at a granule boundary once it holds min_compress_block_size bytes, so the
-- granule boundaries show in the size.
DROP TABLE IF EXISTS t_ecs_wide;
CREATE TABLE t_ecs_wide (s String CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_wide SELECT repeat(char(97 + number % 26), 2000) FROM numbers(8192);

SELECT column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
CROSS JOIN (SELECT estimateCompressedSize('String', 'LZ4')(s) AS estimate FROM t_ecs_wide) AS estimates
WHERE database = currentDatabase() AND table = 't_ecs_wide' AND active AND column = 's';

DROP TABLE t_ecs_wide;

SELECT 'GROUP BY: each group is estimated as the part it would be';

SELECT k, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) AS estimate FROM t_ecs GROUP BY k) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecs_parts' AND active AND column = 'lc'
ORDER BY k;

SELECT k, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('IPv6', 'LZ4', 65536)(ip4) AS estimate FROM t_ecs GROUP BY k) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecs_parts' AND active AND column = 'ip6'
ORDER BY k;

SELECT 'States and combinators';

-- -State and -Merge round-trip. The states of several groups merge into the sum of their sizes, as the parts they would be.
SELECT estimateCompressedSizeMerge('LowCardinality(String)', 'LZ4', 65536)(st) = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs)
FROM (SELECT estimateCompressedSizeState('LowCardinality(String)', 'LZ4', 65536)(s) AS st FROM t_ecs);

SELECT estimateCompressedSizeMerge('LowCardinality(String)', 'LZ4', 65536)(st)
    = (SELECT sum(column_data_compressed_bytes) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecs_parts' AND active AND column = 'lc')
FROM (SELECT estimateCompressedSizeState('LowCardinality(String)', 'LZ4', 65536)(s) AS st FROM t_ecs GROUP BY k);

SELECT toTypeName(estimateCompressedSizeState('LowCardinality(String)', 'LZ4', 65536)(s)) FROM t_ecs;

-- A state serialized and read back.
SELECT finalizeAggregation(CAST(unhex(hex(st)), 'AggregateFunction(estimateCompressedSize(\'LowCardinality(String)\', \'LZ4\', 65536), String)'))
    = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs)
FROM (SELECT estimateCompressedSizeState('LowCardinality(String)', 'LZ4', 65536)(s) AS st FROM t_ecs);

-- -If: the same rows as a WHERE, read in the same blocks; a group of the partitioned table. (A dense column: the aggregator passes
-- a sparse argument of -If dense.)
SELECT
    estimateCompressedSizeIf('LowCardinality(String)', 'LZ4', 65536)(s, k = 0)
        = (SELECT column_data_compressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecs_parts' AND active AND column = 'lc' AND partition = '0'),
    estimateCompressedSizeIf('LowCardinality(String)', 'LZ4', 65536)(s, k = 3) = 0
FROM t_ecs;

-- -Array adds the elements one at a time.
SELECT estimateCompressedSizeArray('LowCardinality(String)', 'LZ4', 65536)(arr) = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(arrayJoin(arr)) FROM (SELECT [s, s] AS arr FROM t_ecs))
FROM (SELECT [s, s] AS arr FROM t_ecs);

SELECT 'Window functions';

-- A growing frame accumulates the sizes of its rows: estimated as LowCardinality(String), the String column gives the same
-- sizes as the LowCardinality(String) column, row by row, and other sizes than the String column.
SELECT countIf(r_s_as_lc != r_lc), countIf(r_s_as_lc = r_s), min(r_lc) > 0
FROM
(
    SELECT
        estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_s_as_lc,
        estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(lc) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_lc,
        estimateCompressedSize('String', 'LZ4', 65536)(s) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_s
    FROM (SELECT * FROM t_ecs LIMIT 1000)
);

SELECT 'No rows, and the empty string';

SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4')(s) FROM t_ecs WHERE 0;
SELECT estimateCompressedSize('String', 'LZ4')(''), estimateCompressedSize('LowCardinality(String)', 'LZ4')('');

SELECT 'Errors';

-- Converting is checked as `accurateCast` does; a conversion that does not exist is rejected when the query is analyzed.
SELECT estimateCompressedSize('UInt8', 'LZ4')(number) FROM numbers(1000); -- { serverError CANNOT_CONVERT_TYPE }
SELECT estimateCompressedSize('Array(UInt8)', 'LZ4')(number) FROM numbers(1) WHERE 0; -- { serverError TYPE_MISMATCH }
-- The codec is validated against the target type.
SELECT estimateCompressedSize('String', 'T64')(number) FROM numbers(1) WHERE 0; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
-- The first parameter is the target type.
SELECT estimateCompressedSize('LowCardinality(Strin)', 'LZ4')(number) FROM numbers(1); -- { serverError UNKNOWN_TYPE }
SELECT estimateCompressedSize('LZ4', 'UInt64')(number) FROM numbers(1); -- { serverError UNKNOWN_TYPE }
SELECT estimateCompressedSize(65536, 'UInt64')(number) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressedSize(number) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
-- At most one codec, one block size, three parameters, one argument.
SELECT estimateCompressedSize('UInt64', 'LZ4', 'ZSTD')(number) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressedSize('UInt64', 1024, 2048)(number) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressedSize('UInt64', 'LZ4', 65536, 'Int8')(number) FROM numbers(1); -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT estimateCompressedSize('String', 'LZ4')(s, lc) FROM t_ecs; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE t_ecs_parts;
DROP TABLE t_ecs;
