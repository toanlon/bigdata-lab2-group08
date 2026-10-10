import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

path = Path("contract.json")
out = Path("evidence/task1")
review_path = out / "contract-review.json"
checksum_path = out / "contract.sha256"

if not path.is_file():
    raise SystemExit("Không tìm thấy contract.json. Chạy từ thư mục gốc dự án.")

if review_path.exists() or checksum_path.exists():
    raise SystemExit("Đã có file chốt contract; kiểm tra trước khi tiếp tục.")

data = path.read_bytes()
json.loads(data)  # Kiểm tra JSON hợp lệ.

author = input("Tên người soạn contract: ").strip()
reviewer = input("Tên thành viên đã review: ").strip()

if not author or not reviewer:
    raise SystemExit("Cần nhập đầy đủ tên.")

if author.casefold() == reviewer.casefold():
    raise SystemExit("Người review phải là thành viên khác.")

confirmation = input("Review đã hoàn tất và đạt? Nhập YES: ").strip()
if confirmation != "YES":
    raise SystemExit("Chưa chốt contract.")

if path.read_bytes() != data:
    raise SystemExit("Contract vừa thay đổi. Cần kiểm tra lại trước khi chốt.")

digest = hashlib.sha256(data).hexdigest()
out.mkdir(parents=True, exist_ok=True)

record = {
    "contract_file": "contract.json",
    "contract_sha256": digest,
    "author": author,
    "reviewer": reviewer,
    "approved_at": datetime.now(timezone.utc).isoformat(),
    "decision": "APPROVED"
}

with review_path.open("x", encoding="utf-8") as f:
    json.dump(record, f, indent=2, ensure_ascii=False)
    f.write("\n")

with checksum_path.open("x", encoding="utf-8") as f:
    f.write(f"{digest}  contract.json\n")

print(json.dumps(record, indent=2, ensure_ascii=False))
