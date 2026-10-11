-- Evaluate the exported, reloaded Parquet relation: candidate.
CREATE OR REPLACE VIEW after_flags AS
SELECT c.*,
    coalesce(NULLIF(trim(record_id),'') IS NOT NULL
      AND NULLIF(trim(sensor_id),'') IS NOT NULL
      AND NULLIF(trim(site),'') IS NOT NULL
      AND event_time_utc IS NOT NULL AND ingest_time_utc IS NOT NULL
      AND temperature_c IS NOT NULL AND is_late IS NOT NULL
      AND NULLIF(trim(source_object),'') IS NOT NULL
      AND source_row IS NOT NULL AND NULLIF(trim(source_sha256),'') IS NOT NULL,false) AS required_ok,
    coalesce(isfinite(temperature_c)
      AND temperature_c BETWEEN getvariable('min_c') AND getvariable('max_c'),false) AS value_ok,
    EXISTS(SELECT 1 FROM registry r WHERE r.sensor_id=c.sensor_id AND r.site=c.site) AS sensor_ok,
    coalesce(event_time_utc<=ingest_time_utc
      AND ingest_time_utc<=cast(getvariable('as_of') AS TIMESTAMP),false) AS chronology_ok,
    epoch(ingest_time_utc-event_time_utc) AS lag_seconds
FROM candidate c;

CREATE OR REPLACE VIEW after_metric_counts AS
SELECT 'Q_REQUIRED' AS rule_id,'All exported candidate rows' AS cohort,
    count(*) FILTER(WHERE required_ok) AS numerator,count(*) AS denominator FROM after_flags
UNION ALL SELECT 'Q_UNIQUE','All exported candidate rows',count(DISTINCT record_id),count(*) FROM after_flags
UNION ALL SELECT 'Q_VALUE','All exported candidate rows',count(*) FILTER(WHERE value_ok),count(*) FROM after_flags
UNION ALL SELECT 'Q_SENSOR','All exported candidate rows',count(*) FILTER(WHERE sensor_ok),count(*) FROM after_flags
UNION ALL SELECT 'Q_CHRONOLOGY','All exported candidate rows',count(*) FILTER(WHERE chronology_ok),count(*) FROM after_flags
UNION ALL SELECT 'Q_TIMELY','Chronologically valid exported candidate rows',
    count(*) FILTER(WHERE chronology_ok AND lag_seconds<=getvariable('timely_seconds')),
    count(*) FILTER(WHERE chronology_ok) FROM after_flags;

CREATE OR REPLACE VIEW qa_matches AS
SELECT q.record_id,q.reference_c,c.temperature_c,
    abs(c.temperature_c-q.reference_c) AS absolute_error_c
FROM qa_reference q JOIN candidate c USING(record_id);