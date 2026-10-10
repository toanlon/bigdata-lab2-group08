-- Đánh dấu các điều kiện chất lượng trên W.
CREATE OR REPLACE VIEW before_flags AS
SELECT *,
    COALESCE(
        NULLIF(TRIM(record_id), '') IS NOT NULL
        AND NULLIF(TRIM(sensor_id), '') IS NOT NULL
        AND NULLIF(TRIM(event_time), '') IS NOT NULL
        AND NULLIF(TRIM(ingest_time), '') IS NOT NULL
        AND NULLIF(TRIM(reading), '') IS NOT NULL
        AND NULLIF(TRIM(unit), '') IS NOT NULL,
        false
    ) AS required_ok,

    COALESCE(
        isfinite(value_num)
        AND unit IN ('C', 'F')
        AND isfinite(temperature_c_unrounded)
        AND temperature_c_unrounded BETWEEN getvariable('min_c') AND getvariable('max_c'),
        false
    ) AS value_ok,

    EXISTS (
        SELECT 1
        FROM registry r
        WHERE UPPER(TRIM(r.sensor_id)) = winner_values.sensor_id
    ) AS sensor_ok,

    COALESCE(
        event_ts IS NOT NULL
        AND ingest_ts IS NOT NULL
        AND event_ts <= ingest_ts
        AND ingest_ts <= CAST(getvariable('as_of') AS TIMESTAMP),
        false
    ) AS chronology_ok,

    EPOCH(ingest_ts - event_ts) AS lag_seconds
FROM winner_values;


-- Tử số và mẫu số cho từng metric.
CREATE OR REPLACE VIEW before_metric_counts AS

SELECT
    'Q_REQUIRED' AS rule_id,
    'W: winners before validity filtering' AS cohort,
    COUNT(*) FILTER (WHERE required_ok) AS numerator,
    COUNT(*) AS denominator,
    1.0 AS threshold,
    'BLOCK' AS severity
FROM before_flags

UNION ALL

SELECT
    'Q_UNIQUE',
    'Key-eligible physical records before deduplication',
    COUNT(DISTINCT record_id),
    COUNT(*),
    1.0,
    'BLOCK'
FROM key_eligible

UNION ALL

SELECT
    'Q_VALUE',
    'W: winners before validity filtering',
    COUNT(*) FILTER (WHERE value_ok),
    COUNT(*),
    1.0,
    'BLOCK'
FROM before_flags

UNION ALL

SELECT
    'Q_SENSOR',
    'W: winners before validity filtering',
    COUNT(*) FILTER (WHERE sensor_ok),
    COUNT(*),
    1.0,
    'BLOCK'
FROM before_flags

UNION ALL

SELECT
    'Q_CHRONOLOGY',
    'W: winners before validity filtering',
    COUNT(*) FILTER (WHERE chronology_ok),
    COUNT(*),
    1.0,
    'BLOCK'
FROM before_flags

UNION ALL

SELECT
    'Q_TIMELY',
    'W: chronologically valid winners only',
    COUNT(*) FILTER (
        WHERE chronology_ok AND lag_seconds <= getvariable('timely_seconds')
    ),
    COUNT(*) FILTER (WHERE chronology_ok),
    0.95,
    'BLOCK'
FROM before_flags;


CREATE OR REPLACE VIEW quality_before AS
SELECT *,
       CASE
           WHEN denominator = 0 THEN NULL
           ELSE numerator::DOUBLE / denominator
       END AS value,
       CASE
           WHEN denominator = 0 THEN 'NOT_EVALUATED'
           ELSE 'EVALUATED'
       END AS status
FROM before_metric_counts;


-- Báo riêng những dòng không tham gia tập W.
-- Các nhóm bên dưới được tính không chồng lấn.
CREATE OR REPLACE VIEW intake_summary AS
SELECT
    (SELECT COUNT(*) FROM typed) AS raw_count,

    (SELECT COUNT(*)
     FROM typed
     WHERE NOT COALESCE(parse_ok, false)
    ) AS parse_failure_count,

    (SELECT COUNT(*)
     FROM typed
     WHERE COALESCE(parse_ok, false)
       AND NOT COALESCE(
           regexp_full_match(record_id, getvariable('record_id_pattern')),
           false
       )
    ) AS key_rejected_count,

    (SELECT COUNT(*)
     FROM typed
     WHERE COALESCE(parse_ok, false)
       AND COALESCE(
           regexp_full_match(record_id, getvariable('record_id_pattern')),
           false
       )
       AND ingest_ts IS NULL
    ) AS ingest_parse_rejected_count,

    (SELECT COUNT(*) FROM key_eligible) AS key_eligible_count,

    (SELECT COUNT(*) FROM ranked_versions
     WHERE version_rank > 1
    ) AS duplicate_excess_count,

    (SELECT COUNT(*) FROM winners) AS winner_count,

    (SELECT COUNT(*) FROM before_flags
     WHERE NOT chronology_ok
    ) AS timeliness_excluded_count;


-- Lỗi trường bắt buộc trong toàn bộ đầu vào phân tích được.
CREATE OR REPLACE VIEW parsed_intake_required_summary AS
SELECT
    COUNT(*) AS parsed_count,
    COUNT(*) FILTER (
        WHERE NULLIF(TRIM(record_id), '') IS NULL
           OR NULLIF(TRIM(sensor_id), '') IS NULL
           OR NULLIF(TRIM(event_time), '') IS NULL
           OR NULLIF(TRIM(ingest_time), '') IS NULL
           OR NULLIF(TRIM(reading), '') IS NULL
           OR NULLIF(TRIM(unit), '') IS NULL
    ) AS required_failure_count
FROM typed
WHERE COALESCE(parse_ok, false);


-- Khảo sát các loại lỗi; một dòng có thể mắc nhiều lỗi.
CREATE OR REPLACE VIEW defect_counts AS
SELECT 'REQUIRED' AS defect,
       COUNT(*) AS affected_rows
FROM before_flags
WHERE NOT required_ok

UNION ALL

SELECT 'VALUE', COUNT(*)
FROM before_flags
WHERE NOT value_ok

UNION ALL

SELECT 'UNKNOWN_SENSOR', COUNT(*)
FROM before_flags
WHERE NOT sensor_ok

UNION ALL

SELECT 'CHRONOLOGY', COUNT(*)
FROM before_flags
WHERE NOT chronology_ok

UNION ALL

SELECT 'LATE', COUNT(*)
FROM before_flags
WHERE chronology_ok AND lag_seconds > getvariable('timely_seconds');


-- Tham chiếu nguồn để kiểm tra lỗi.
-- Không đưa operator_email hoặc raw_payload vào kết quả.
CREATE OR REPLACE VIEW defect_source_refs AS
SELECT
    source_object,
    source_row,
    record_id,
    required_ok,
    value_ok,
    sensor_ok,
    chronology_ok
FROM before_flags
WHERE NOT required_ok
   OR NOT value_ok
   OR NOT sensor_ok
   OR NOT chronology_ok;