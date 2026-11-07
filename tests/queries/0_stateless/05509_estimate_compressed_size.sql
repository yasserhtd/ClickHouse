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

SELECT 'Nullable, Array, Tuple, FixedString, Decimal, DateTime64, and conversions into them';

DROP TABLE IF EXISTS t_ecs_types;
CREATE TABLE t_ecs_types
(
    n Nullable(UInt32) CODEC(LZ4), arr Array(UInt32) CODEC(LZ4), arr_s Array(String) CODEC(LZ4), arr_lc Array(LowCardinality(String)) CODEC(LZ4),
    t Tuple(UInt32, String) CODEC(LZ4), fs FixedString(8) CODEC(LZ4), d Decimal(18, 4) CODEC(LZ4), dt DateTime64(3) CODEC(LZ4),
    u UInt32 CODEC(LZ4), ns Nullable(String) CODEC(LZ4), u64 UInt64 CODEC(LZ4), n64 Nullable(UInt64) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1;

-- The Nullable numbers are built as the function converts them, with the default value under NULL.
INSERT INTO t_ecs_types
SELECT
    accurateCast(if(number % 7 = 0, NULL, number), 'Nullable(UInt32)'), [number, number + 1], [toString(number % 16), toString(number % 16)], [toString(number % 16), toString(number % 16)],
    (number, toString(number % 16)), toFixedString(toString(number % 1000), 8), toDecimal64(number, 4), toDateTime64(number, 3),
    number, if(number % 7 = 0, NULL, toString(number)), number, accurateCast(if(number % 7 = 0, NULL, number), 'Nullable(UInt64)')
FROM numbers(3 * 8192);

-- Each column as itself.
SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'n' AS column, estimateCompressedSize('Nullable(UInt32)', 'LZ4', 65536)(n) AS estimate FROM t_ecs_types
    UNION ALL SELECT 'arr', estimateCompressedSize('Array(UInt32)', 'LZ4', 65536)(arr) FROM t_ecs_types
    UNION ALL SELECT 'arr_s', estimateCompressedSize('Array(String)', 'LZ4', 65536)(arr_s) FROM t_ecs_types
    UNION ALL SELECT 'arr_lc', estimateCompressedSize('Array(LowCardinality(String))', 'LZ4', 65536)(arr_lc) FROM t_ecs_types
    UNION ALL SELECT 't', estimateCompressedSize('Tuple(UInt32, String)', 'LZ4', 65536)(t) FROM t_ecs_types
    UNION ALL SELECT 'fs', estimateCompressedSize('FixedString(8)', 'LZ4', 65536)(fs) FROM t_ecs_types
    UNION ALL SELECT 'd', estimateCompressedSize('Decimal(18, 4)', 'LZ4', 65536)(d) FROM t_ecs_types
    UNION ALL SELECT 'dt', estimateCompressedSize('DateTime64(3)', 'LZ4', 65536)(dt) FROM t_ecs_types
    UNION ALL SELECT 'u', estimateCompressedSize('UInt32', 'LZ4', 65536)(u) FROM t_ecs_types
    UNION ALL SELECT 'ns', estimateCompressedSize('Nullable(String)', 'LZ4', 65536)(ns) FROM t_ecs_types
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_types' AND active
ORDER BY column;

-- Converted: a wider Nullable number as a narrower one, a narrower number, a String array as a LowCardinality one and back, a
-- number as a Decimal and as a DateTime64, a String as FixedString, a String as Nullable(String).
SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'n' AS column, estimateCompressedSize('Nullable(UInt32)', 'LZ4', 65536)(n64) AS estimate FROM t_ecs_types
    UNION ALL SELECT 'u', estimateCompressedSize('UInt32', 'LZ4', 65536)(u64) FROM t_ecs_types
    UNION ALL SELECT 'arr_lc', estimateCompressedSize('Array(LowCardinality(String))', 'LZ4', 65536)(arr_s) FROM t_ecs_types
    UNION ALL SELECT 'arr_s', estimateCompressedSize('Array(String)', 'LZ4', 65536)(arr_lc) FROM t_ecs_types
    UNION ALL SELECT 'd', estimateCompressedSize('Decimal(18, 4)', 'LZ4', 65536)(u) FROM t_ecs_types
    UNION ALL SELECT 'dt', estimateCompressedSize('DateTime64(3)', 'LZ4', 65536)(u) FROM t_ecs_types
    UNION ALL SELECT 'fs', estimateCompressedSize('FixedString(8)', 'LZ4', 65536)(toString(u % 1000)) FROM t_ecs_types
    UNION ALL SELECT 'ns', estimateCompressedSize('Nullable(String)', 'LZ4', 65536)(if(u % 7 = 0, NULL, toString(u))) FROM t_ecs_types
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_types' AND active
ORDER BY column;

DROP TABLE t_ecs_types;

-- The granules of a block are sized by the converted rows, as the writer sizes them by the block it writes: short strings
-- estimated as a wide FixedString make granules of fewer rows.
SET allow_suspicious_fixed_string_types = 1;

DROP TABLE IF EXISTS t_ecs_fixed;
CREATE TABLE t_ecs_fixed (f FixedString(4000) CODEC(LZ4), s String CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_fixed SELECT toFixedString(toString(number % 100), 4000), toString(number % 100) FROM numbers(8192);

SELECT
    column_data_compressed_bytes = from_string AS matches_from_string,
    column_data_compressed_bytes = from_fixed AS matches_from_fixed,
    column_data_compressed_bytes = grouped AS matches_grouped
FROM system.parts_columns
CROSS JOIN
(
    SELECT
        (SELECT estimateCompressedSize('FixedString(4000)', 'LZ4')(s) FROM t_ecs_fixed) AS from_string,
        (SELECT estimateCompressedSize('FixedString(4000)', 'LZ4')(f) FROM t_ecs_fixed) AS from_fixed,
        (SELECT any(e) FROM (SELECT k, estimateCompressedSize('FixedString(4000)', 'LZ4')(s) AS e FROM (SELECT s, 0 AS k FROM t_ecs_fixed) GROUP BY k)) AS grouped
) AS estimates
WHERE database = currentDatabase() AND table = 't_ecs_fixed' AND active AND column = 'f';

DROP TABLE t_ecs_fixed;

-- The rows are converted with the conversion settings of the query, as `accurateCast` converts them.
SET date_time_input_format = 'best_effort';
SELECT
    estimateCompressedSize('DateTime', 'LZ4')(concat('07 Nov 2026 12:00:', leftPad(toString(number % 60), 2, '0')))
    = estimateCompressedSize('DateTime', 'LZ4')(toDateTime('2026-11-07 12:00:00', 'UTC') + number % 60)
FROM numbers(1000)
SETTINGS session_timezone = 'UTC';
SET date_time_input_format = 'basic';

SELECT 'Specialized codecs on the data substream';

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_ecs_t64;
CREATE TABLE t_ecs_t64 (lc LowCardinality(UInt32) CODEC(T64), n Nullable(UInt32) CODEC(T64), arr Array(UInt32) CODEC(T64), dd UInt32 CODEC(DoubleDelta, LZ4), n64 Nullable(UInt64) CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_t64 SELECT number % 1000, accurateCast(if(number % 7 = 0, NULL, number), 'Nullable(UInt32)'), [number, number + 1], number, accurateCast(if(number % 7 = 0, NULL, number), 'Nullable(UInt64)') FROM numbers(3 * 8192);

-- As themselves, and converted from plain numbers.
SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size, column_data_compressed_bytes = converted AS matches_converted
FROM system.parts_columns
INNER JOIN
(
    SELECT 'lc' AS column, estimateCompressedSize('LowCardinality(UInt32)', 'T64', 65536)(lc) AS estimate, estimateCompressedSize('LowCardinality(UInt32)', 'T64', 65536)(dd % 1000) AS converted FROM t_ecs_t64
    UNION ALL SELECT 'n', estimateCompressedSize('Nullable(UInt32)', 'T64', 65536)(n), estimateCompressedSize('Nullable(UInt32)', 'T64', 65536)(n64) FROM t_ecs_t64
    UNION ALL SELECT 'arr', estimateCompressedSize('Array(UInt32)', 'T64', 65536)(arr), estimateCompressedSize('Array(UInt32)', 'T64', 65536)([dd, dd + 1]) FROM t_ecs_t64
    UNION ALL SELECT 'dd', estimateCompressedSize('UInt32', 'DoubleDelta, LZ4', 65536)(dd), estimateCompressedSize('UInt32', 'DoubleDelta, LZ4', 65536)(toUInt64(dd)) FROM t_ecs_t64
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_t64' AND active
ORDER BY column;

DROP TABLE t_ecs_t64;

SELECT 'Dynamic and JSON are estimated with the layouts of a merged part, Map and Variant as a part stores them';

-- A part merged from two parts: its JSON shared data has the layout of merged parts, which a part written by one insert has not.
DROP TABLE IF EXISTS t_ecs_layouts;
CREATE TABLE t_ecs_layouts (d Dynamic CODEC(LZ4), j JSON CODEC(LZ4), js JSON(max_dynamic_paths = 4) CODEC(LZ4), jl JSON(a LowCardinality(String)) CODEC(LZ4), raw String CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_layouts
SELECT
    if(number % 3 = 0, number::Dynamic, toString(number)::Dynamic),
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')::JSON,
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')::JSON(max_dynamic_paths = 4),
    concat('{"a":"', toString(number % 16), '"}')::JSON(a LowCardinality(String)),
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')
FROM numbers(8192);

INSERT INTO t_ecs_layouts
SELECT
    if(number % 3 = 0, number::Dynamic, toString(number)::Dynamic),
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')::JSON,
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')::JSON(max_dynamic_paths = 4),
    concat('{"a":"', toString(number % 16), '"}')::JSON(a LowCardinality(String)),
    concat('{"a":', toString(number), ',"b":"', toString(number % 100), '","c', toString(number % 10), '":1}')
FROM numbers(8192, 2048);

OPTIMIZE TABLE t_ecs_layouts FINAL;

SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'd' AS column, estimateCompressedSize('Dynamic', 'LZ4')(d) AS estimate FROM t_ecs_layouts
    UNION ALL SELECT 'j', estimateCompressedSize('JSON', 'LZ4')(j) FROM t_ecs_layouts
    UNION ALL SELECT 'js', estimateCompressedSize('JSON(max_dynamic_paths = 4)', 'LZ4')(js) FROM t_ecs_layouts
    UNION ALL SELECT 'jl', estimateCompressedSize('JSON(a LowCardinality(String))', 'LZ4')(jl) FROM t_ecs_layouts
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_layouts' AND active
ORDER BY column;

-- The raw String column estimated as JSON: the size of the JSON column.
SELECT column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
CROSS JOIN (SELECT estimateCompressedSize('JSON', 'LZ4')(raw) AS estimate FROM t_ecs_layouts) AS estimates
WHERE database = currentDatabase() AND table = 't_ecs_layouts' AND active AND column = 'j';

-- Whatever the blocks of the query, also for LowCardinality inside JSON and Dynamic, whose types only the data reveals.
SELECT
    (SELECT estimateCompressedSize('Dynamic', 'LZ4')(d) FROM t_ecs_layouts SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('Dynamic', 'LZ4')(d) FROM t_ecs_layouts),
    (SELECT estimateCompressedSize('JSON', 'LZ4')(j) FROM t_ecs_layouts SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('JSON', 'LZ4')(j) FROM t_ecs_layouts),
    (SELECT estimateCompressedSize('JSON(max_dynamic_paths = 4)', 'LZ4')(js) FROM t_ecs_layouts SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('JSON(max_dynamic_paths = 4)', 'LZ4')(js) FROM t_ecs_layouts),
    (SELECT estimateCompressedSize('JSON(a LowCardinality(String))', 'LZ4')(jl) FROM t_ecs_layouts SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('JSON(a LowCardinality(String))', 'LZ4')(jl) FROM t_ecs_layouts),
    (SELECT estimateCompressedSize('JSON', 'LZ4')(raw) FROM t_ecs_layouts SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('JSON', 'LZ4')(raw) FROM t_ecs_layouts),
    (SELECT estimateCompressedSize('Dynamic', 'LZ4')(CAST([toLowCardinality(toString(number % 16))], 'Dynamic')) FROM numbers(10000) SETTINGS max_block_size = 1000)
    = (SELECT estimateCompressedSize('Dynamic', 'LZ4')(CAST([toLowCardinality(toString(number % 16))], 'Dynamic')) FROM numbers(10000) SETTINGS max_block_size = 8192);

-- Dynamic and JSON columns whose structure changes between blocks are written with the structure of the first block.
SELECT estimateCompressedSize('Dynamic', 'LZ4', 65536)(d) > 0
FROM (SELECT if(number < 100, number::Dynamic, toString(number)::Dynamic) AS d FROM numbers(300))
SETTINGS max_block_size = 100;

SELECT k, estimateCompressedSize('JSON', 'LZ4', 65536)(j) > 0
FROM (SELECT number % 3 AS k, if(number < 100, '{"a":1}', concat('{"b', toString(number % 100), '":"x"}'))::JSON AS j FROM numbers(300))
GROUP BY k ORDER BY k
SETTINGS max_block_size = 100;

-- The serialization of the shared typed paths of JSON cuts the blocks of its data stream itself, by the offset of the stream,
-- also when a granule fills the adaptive buffer exactly (8192 rows of 8 bytes). As every JSON column, it is estimated with the
-- layout of a merged part: a part written by one insert is a few dozen bytes smaller.
DROP TABLE IF EXISTS t_ecs_shared;
CREATE TABLE t_ecs_shared (j JSON(SHARED a UInt64, SHARED b UInt64) CODEC(LZ4), s JSON(SHARED a UInt64) CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_shared
SELECT
    CAST(concat('{"a":', toString(number), ',"b":', toString(number * 2), '}'), 'JSON(SHARED a UInt64, SHARED b UInt64)'),
    CAST(concat('{"a":', toString(number), '}'), 'JSON(SHARED a UInt64)')
FROM numbers(8192);

INSERT INTO t_ecs_shared
SELECT
    CAST(concat('{"a":', toString(number), ',"b":', toString(number * 2), '}'), 'JSON(SHARED a UInt64, SHARED b UInt64)'),
    CAST(concat('{"a":', toString(number), '}'), 'JSON(SHARED a UInt64)')
FROM numbers(8192, 1808);

OPTIMIZE TABLE t_ecs_shared FINAL;

SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'j' AS column, estimateCompressedSize('JSON(SHARED a UInt64, SHARED b UInt64)', 'LZ4')(j) AS estimate FROM t_ecs_shared
    UNION ALL SELECT 's', estimateCompressedSize('JSON(SHARED a UInt64)', 'LZ4')(s) FROM t_ecs_shared
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_shared' AND active
ORDER BY column;

DROP TABLE t_ecs_shared;

SELECT rows, estimateCompressedSize('JSON(SHARED a UInt64)', 'NONE')(j) > rows * 8
FROM (SELECT number, CAST(concat('{"a":', toString(number), '}'), 'JSON(SHARED a UInt64)') AS j FROM numbers(16384))
ARRAY JOIN [8191, 8192, 10000, 16384] AS rows
WHERE number < rows
GROUP BY rows
ORDER BY rows;

DROP TABLE t_ecs_layouts;

-- Map and Variant match the part one insert writes; a part merged from several is a few hundred bytes larger.
DROP TABLE IF EXISTS t_ecs_layouts0;
CREATE TABLE t_ecs_layouts0 (m Map(String, UInt64) CODEC(LZ4), v Variant(UInt64, String) CODEC(LZ4), ml Map(LowCardinality(String), UInt64) CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_ecs_layouts0
SELECT
    map(concat('k', toString(number % 7)), number, concat('k', toString(number % 11)), number * 2),
    if(number % 2 = 0, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)),
    map(concat('k', toString(number % 7)), number, concat('k', toString(number % 11)), number * 2)
FROM numbers(8192 + 2048);

SELECT column, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'm' AS column, estimateCompressedSize('Map(String, UInt64)', 'LZ4')(m) AS estimate FROM t_ecs_layouts0
    UNION ALL SELECT 'v', estimateCompressedSize('Variant(UInt64, String)', 'LZ4')(v) FROM t_ecs_layouts0
    UNION ALL SELECT 'ml', estimateCompressedSize('Map(LowCardinality(String), UInt64)', 'LZ4')(ml) FROM t_ecs_layouts0
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecs_layouts0' AND active
ORDER BY column;

-- The Map converted: as a Map with LowCardinality keys, and the other way round.
SELECT
    estimateCompressedSize('Map(LowCardinality(String), UInt64)', 'LZ4')(m) = estimateCompressedSize('Map(LowCardinality(String), UInt64)', 'LZ4')(ml),
    estimateCompressedSize('Map(String, UInt64)', 'LZ4')(ml) = estimateCompressedSize('Map(String, UInt64)', 'LZ4')(m)
FROM t_ecs_layouts0;

SELECT
    (SELECT estimateCompressedSize('Map(String, UInt64)', 'LZ4')(m) FROM t_ecs_layouts0 SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('Map(String, UInt64)', 'LZ4')(m) FROM t_ecs_layouts0),
    (SELECT estimateCompressedSize('Variant(UInt64, String)', 'LZ4')(v) FROM t_ecs_layouts0 SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('Variant(UInt64, String)', 'LZ4')(v) FROM t_ecs_layouts0),
    (SELECT estimateCompressedSize('Map(LowCardinality(String), UInt64)', 'LZ4')(ml) FROM t_ecs_layouts0 SETTINGS max_block_size = 1000) = (SELECT estimateCompressedSize('Map(LowCardinality(String), UInt64)', 'LZ4')(ml) FROM t_ecs_layouts0);

DROP TABLE t_ecs_layouts0;

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

-- A group of a plain type is serialized in batches of rows; its size is still that of its part, whatever the block size.
SELECT k, column_data_compressed_bytes = estimate AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN (SELECT k, estimateCompressedSize('IPv4', 'LZ4', 65536)(ip4) AS estimate FROM t_ecs GROUP BY k SETTINGS max_block_size = 1000) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecs_parts' AND active AND column = 'ip4'
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

-- With keys, only the rows that pass the condition are converted: a rejected row need not fit the target type.
SELECT k, count(), estimateCompressedSizeIf('UInt8', 'LZ4')(number, number < 10) FROM (SELECT intHash64(number % 2) AS k, number FROM numbers(1000)) GROUP BY k ORDER BY k;
SELECT estimateCompressedSizeIf('UInt8', 'LZ4')(number, number < 10) = estimateCompressedSize('UInt8', 'LZ4')(toUInt8(number)) FROM numbers(10);

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

-- A growing frame gives a result after every row and closes the part of its state each time: the rows after the first result
-- are parts of their own. The whole frame is one part, the size of the aggregate.
SELECT
    any(w) = (SELECT estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) FROM t_ecs),
    max(g) > any(w)
FROM
(
    SELECT
        estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) OVER () AS w,
        estimateCompressedSize('LowCardinality(String)', 'LZ4', 65536)(s) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g
    FROM t_ecs
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
