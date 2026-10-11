-- Classify every physical input; do not drop malformed rows.
CREATE OR REPLACE VIEW preclassified AS
SELECT *, CASE WHEN NOT coalesce(parse_ok,false) THEN ['PARSE']
    ELSE list_filter([
        CASE WHEN NOT coalesce(regexp_full_match(record_id,getvariable('record_id_pattern')),false) THEN 'KEY' END,
        CASE WHEN ingest_ts IS NULL THEN 'INGEST_PARSE' END
    ], x -> x IS NOT NULL) END AS pre_reasons
FROM typed;

CREATE OR REPLACE VIEW key_eligible AS
SELECT * EXCLUDE(pre_reasons) FROM preclassified WHERE len(pre_reasons)=0;

CREATE OR REPLACE VIEW ranked_versions AS
SELECT *, row_number() OVER (
    PARTITION BY record_id ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
) AS version_rank FROM key_eligible;

CREATE OR REPLACE VIEW winners AS
SELECT * EXCLUDE(version_rank) FROM ranked_versions WHERE version_rank=1;

CREATE OR REPLACE VIEW winner_values AS
SELECT *, CASE WHEN unit='C' THEN value_num
               WHEN unit='F' THEN (value_num-32)*5.0/9.0 END AS temperature_c_unrounded
FROM winners;

CREATE OR REPLACE VIEW winner_flags AS
SELECT *,
    coalesce(NULLIF(trim(record_id),'') IS NOT NULL
        AND NULLIF(trim(sensor_id),'') IS NOT NULL
        AND NULLIF(trim(event_time),'') IS NOT NULL
        AND NULLIF(trim(ingest_time),'') IS NOT NULL
        AND NULLIF(trim(reading),'') IS NOT NULL
        AND NULLIF(trim(unit),'') IS NOT NULL,false) AS required_ok,
    coalesce(isfinite(value_num),false) AS numeric_ok,
    coalesce(list_contains(getvariable('supported_units'),unit),false) AS unit_ok,
    coalesce(isfinite(temperature_c_unrounded)
        AND temperature_c_unrounded BETWEEN getvariable('min_c') AND getvariable('max_c'),false) AS range_ok,
    event_ts IS NOT NULL AND ingest_ts IS NOT NULL AS time_parse_ok,
    coalesce(event_ts <= ingest_ts AND ingest_ts <= cast(getvariable('as_of') AS TIMESTAMP),false) AS chronology_ok,
    EXISTS(SELECT 1 FROM registry r WHERE r.sensor_id=winner_values.sensor_id) AS sensor_ok,
    epoch(ingest_ts-event_ts) AS lag_seconds
FROM winner_values;

CREATE OR REPLACE VIEW classified_winners AS
SELECT *, list_filter([
    CASE WHEN NOT required_ok THEN 'REQUIRED' END,
    CASE WHEN NOT numeric_ok THEN 'NUMERIC' END,
    CASE WHEN NOT unit_ok THEN 'UNIT' END,
    CASE WHEN numeric_ok AND unit_ok AND NOT range_ok THEN 'RANGE' END,
    CASE WHEN NOT time_parse_ok THEN 'TIME_PARSE' END,
    CASE WHEN time_parse_ok AND NOT chronology_ok THEN 'TIME_ORDER' END,
    CASE WHEN NOT sensor_ok THEN 'REFERENCE' END
], x -> x IS NOT NULL) AS reasons
FROM winner_flags;

CREATE OR REPLACE VIEW curated AS
SELECT w.record_id::VARCHAR AS record_id,
       w.sensor_id::VARCHAR AS sensor_id, r.site::VARCHAR AS site,
       w.event_ts::TIMESTAMP AS event_time_utc,
       w.ingest_ts::TIMESTAMP AS ingest_time_utc,
       w.temperature_c_unrounded::DECIMAL(8,2) AS temperature_c,
       (w.lag_seconds>getvariable('timely_seconds'))::BOOLEAN AS is_late,
       w.source_object::VARCHAR AS source_object,
       w.source_row::BIGINT AS source_row,
       w.source_sha256::VARCHAR AS source_sha256
FROM classified_winners w JOIN registry r USING(sensor_id)
WHERE len(w.reasons)=0;

CREATE OR REPLACE VIEW quarantine AS
SELECT source_object,source_row,source_sha256,record_id,sensor_id,
       event_time,ingest_time,reading,unit,operator_email,raw_payload,
       pre_reasons[1] AS primary_reason, to_json(pre_reasons)::VARCHAR AS all_reasons
FROM preclassified WHERE len(pre_reasons)>0
UNION ALL
SELECT source_object,source_row,source_sha256,record_id,sensor_id,
       event_time,ingest_time,reading,unit,operator_email,raw_payload,
       reasons[1] AS primary_reason, to_json(reasons)::VARCHAR AS all_reasons
FROM classified_winners WHERE len(reasons)>0;

CREATE OR REPLACE VIEW duplicates AS
SELECT d.source_object,d.source_row,d.source_sha256,d.record_id,
       d.ingest_ts AS ingest_time_utc,
       w.source_object AS winner_source_object,
       w.source_row AS winner_source_row,
       w.source_sha256 AS winner_source_sha256,
       'SUPERSEDED'::VARCHAR AS reason
FROM ranked_versions d JOIN winners w USING(record_id)
WHERE d.version_rank>1;