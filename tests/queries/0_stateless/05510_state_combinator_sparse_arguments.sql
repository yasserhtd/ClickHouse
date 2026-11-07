-- The `-State` combinator passes a sparse argument to the nested function as it is, so a function with `-State` gives the
-- same state as without, with and without keys, whether the function has its own handling of sparse columns (`sum`) or not.

SET max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS t_state_sparse;
CREATE TABLE t_state_sparse (k UInt8, n UInt64, s String, nn Nullable(UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_state_sparse SELECT number % 3, if(number % 100 = 0, number, 0), if(number % 100 = 0, toString(number), ''), if(number % 100 = 0, number, NULL) FROM numbers(30000);

SELECT DISTINCT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_state_sparse' AND active AND column IN ('n', 's', 'nn') ORDER BY column;

SELECT
    finalizeAggregation(sumState(n)) = sum(n),
    finalizeAggregation(countState(n)) = count(n),
    finalizeAggregation(uniqExactState(n)) = uniqExact(n),
    finalizeAggregation(groupBitOrState(n)) = groupBitOr(n),
    finalizeAggregation(maxState(s)) = max(s),
    finalizeAggregation(uniqExactState(s)) = uniqExact(s),
    finalizeAggregation(sumState(nn)) = sum(nn),
    finalizeAggregation(countState(nn)) = count(nn),
    finalizeAggregation(avgState(nn)) = avg(nn)
FROM t_state_sparse;

SELECT
    k,
    finalizeAggregation(sumState(n)) = sum(n),
    finalizeAggregation(countState(n)) = count(n),
    finalizeAggregation(uniqExactState(n)) = uniqExact(n),
    finalizeAggregation(maxState(s)) = max(s),
    finalizeAggregation(sumState(nn)) = sum(nn),
    finalizeAggregation(countState(nn)) = count(nn)
FROM t_state_sparse
GROUP BY k
ORDER BY k;

-- Merged back, the states of the groups give the totals.
SELECT sumMerge(st_sum) = (SELECT sum(n) FROM t_state_sparse), uniqExactMerge(st_uniq) = (SELECT uniqExact(n) FROM t_state_sparse), countMerge(st_count) = (SELECT count(nn) FROM t_state_sparse)
FROM (SELECT sumState(n) AS st_sum, uniqExactState(n) AS st_uniq, countState(nn) AS st_count FROM t_state_sparse GROUP BY k);

-- Through `-If` and over a window frame as well.
SELECT finalizeAggregation(sumStateIf(n, k = 0)) = sumIf(n, k = 0), finalizeAggregation(countStateIf(nn, k = 1)) = countIf(nn, k = 1) FROM t_state_sparse;

SELECT countIf(finalizeAggregation(st) != r)
FROM (SELECT sumState(n) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS st, sum(n) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t_state_sparse);

DROP TABLE t_state_sparse;
