-- Tags: stateful, no-random-settings

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- External aggregation is not supported as of now
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

SET use_query_condition_cache=0;

-- Check that index analysis is performed only once in both cases: when we choose local plan and when we choose parallel replicas

-- Pre-warm the cache
SELECT URL FROM test.hits WHERE UserID >= 730800628386 FORMAT Null;
SELECT sum(length(URL)) FROM test.hits WHERE UserID >= 730800628386 FORMAT Null;

-- Local plan wins
SELECT URL FROM test.hits WHERE UserID >= 730800628386 FORMAT Null SETTINGS log_comment='query_1';

-- Parallel replicas plan wins
SELECT sum(length(URL)) FROM test.hits WHERE UserID >= 730800628386 FORMAT Null SETTINGS log_comment='query_2';

create table t(a UInt64) engine=MergeTree order by a;
insert into t select number from numbers(1000000);

-- Pre-warm the cache
SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) FORMAT Null;

--set send_logs_level='trace', send_logs_source_regexp='';
SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) FORMAT Null SETTINGS log_comment='query_3';
set send_logs_level='none', send_logs_source_regexp='';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['IndexAnalysisRounds'] index_analysis_rounds
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds;

