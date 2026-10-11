CREATE OR REPLACE VIEW typed AS
SELECT * EXCLUDE(record_id,sensor_id,unit,source_row,parse_ok),
    upper(trim(record_id)) AS record_id,
    upper(trim(sensor_id)) AS sensor_id,
    upper(trim(unit)) AS unit,
    cast(source_row AS BIGINT) AS source_row,
    cast(parse_ok AS BOOLEAN) AS parse_ok,
    try_cast(reading AS DOUBLE) AS value_num,
    CASE WHEN regexp_full_match(event_time, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
         THEN try_strptime(event_time, getvariable('time_format')) END AS event_ts,
    CASE WHEN regexp_full_match(ingest_time, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
         THEN try_strptime(ingest_time, getvariable('time_format')) END AS ingest_ts
FROM raw_envelopes;