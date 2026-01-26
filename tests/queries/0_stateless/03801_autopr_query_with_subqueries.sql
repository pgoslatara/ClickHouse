-- Tags: stateful

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- External aggregation is not supported as of now
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- Unsupported case: filtering by set built from subquery
set send_logs_level='trace', send_logs_source_regexp='';
SELECT * FROM test.hits WHERE CounterID IN (SELECT CounterID % 1000 FROM test.hits) FORMAT Null SETTINGS log_comment='query_1';
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off
-- WITH
--     [96, 500000, 11189312, 2359808, 64, 29920, 82456, 20000, 31064320, 275251200, 48271331/*, 641835*/] AS expected_bytes,
--     arrayJoin(arrayMap(x -> (untuple(x.1), x.2), arrayZip(res, expected_bytes))) AS res
-- SELECT format('{} {} {}', res.1, res.2, res.3)
-- FROM
-- (
--     SELECT groupArray((log_comment, output_bytes)) AS res
--     FROM (
--       SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] output_bytes
--       FROM system.query_log
--       WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
--       ORDER BY event_time_microseconds
--     )
-- )
-- WHERE (greatest(res.2, res.3) / least(res.2, res.3)) > 2.5 AND NOT (res.2 < 100 AND res.3 < 100);

