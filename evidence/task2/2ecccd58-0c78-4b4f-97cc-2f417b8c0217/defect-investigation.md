# Khảo sát chất lượng trước xử lý — Task 2

Run ID: 2ecccd58-0c78-4b4f-97cc-2f417b8c0217

Nguồn số liệu:
- quality_before.json
- defect_samples.json

Tập W gồm 9.960 bản thắng sau chọn phiên bản,
trước khi lọc lỗi hợp lệ.

Các tham chiếu nguồn bên dưới thuộc:
s3://research-raw/lab2/inputs/batch-01/observations_a.csv

source_row là thứ tự bản ghi dữ liệu, không tính header.

## 1. Cảm biến không tồn tại trong danh mục

Có 30/9.960 bản thắng không khớp danh mục cảm biến.
Ví dụ R000311, source_row=311, có sensor_id=S99
và sensor_ok=false.

Ảnh hưởng: không thể gắn site đáng tin cậy từ registry.
Hướng xử lý Task 3: cách ly theo quy tắc tham chiếu registry,
không tự sửa mã cảm biến.

## 2. Thời gian không hợp lệ

Có 70/9.960 bản thắng không đạt quy tắc thời gian.
Một dạng lỗi quan sát được là timestamp không phân tích được:
R000241, source_row=241, có event_time="not-a-time".

Trường này không trống nên vẫn đạt completeness,
nhưng không đạt temporal consistency.
Không suy ra rằng cả 70 dòng đều mắc cùng dạng lỗi này.

Hướng xử lý Task 3: cách ly theo lý do thời gian tương ứng;
không thay bằng thời gian hiện tại.

## 3. Dữ liệu đến trễ

Trong 9.890 bản thắng hợp lệ về thời gian, có 200 dòng
đến trễ hơn 900 giây.

Ví dụ R000381, source_row=381:
- event_time: 2026-02-02T03:10:30Z
- ingest_time: 2026-02-02T03:40:30Z
- Độ trễ: 1.800 giây.

Hướng xử lý Task 3: giữ nếu các điều kiện khác hợp lệ,
đánh dấu is_late=true.

## Lưu ý khi diễn giải

Có 110 bản thắng thiếu trường bắt buộc và 250 bản thắng
không đạt kiểm tra giá trị. Các nhóm lỗi có thể chồng lấn:
R000001 xuất hiện trong cả REQUIRED và VALUE.

Không cộng số lượng các nhóm lỗi để suy ra tổng dòng bị loại.
70 dòng không hợp lệ về thời gian bị loại khỏi mẫu số
timeliness; vì vậy mẫu số chỉ tiêu này là 9.890, không phải 9.960.

Đây là báo cáo trước xử lý. Chưa đánh giá chất lượng sau xử lý,
độ bao phủ hoặc độ khớp QA; chưa kết luận đủ điều kiện phát hành.
