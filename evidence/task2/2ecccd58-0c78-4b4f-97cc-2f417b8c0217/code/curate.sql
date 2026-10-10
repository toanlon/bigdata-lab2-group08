-- Bản ghi đủ điều kiện tham gia chọn phiên bản thắng.
CREATE OR REPLACE VIEW key_eligible AS
SELECT *
FROM typed
WHERE COALESCE(parse_ok, false)
  AND COALESCE(
        regexp_full_match(record_id, getvariable('record_id_pattern')),
        false
      )
  AND ingest_ts IS NOT NULL;


-- Xếp hạng các phiên bản của cùng record_id.
CREATE OR REPLACE VIEW ranked_versions AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY record_id
           ORDER BY ingest_ts DESC,
                    source_object ASC,
                    source_row ASC
       ) AS version_rank
FROM key_eligible;


-- W: bản thắng trước khi lọc lỗi hợp lệ.
CREATE OR REPLACE VIEW winners AS
SELECT * EXCLUDE (version_rank)
FROM ranked_versions
WHERE version_rank = 1;


-- Chuyển đổi nhiệt độ, chưa làm tròn.
-- Đơn vị không hỗ trợ trả về NULL.
CREATE OR REPLACE VIEW winner_values AS
SELECT *,
       CASE
           WHEN unit = 'C' THEN value_num
           WHEN unit = 'F' THEN (value_num - 32) * 5.0 / 9.0
           ELSE NULL
       END AS temperature_c_unrounded
FROM winners;