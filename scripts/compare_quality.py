import csv
import hashlib
import json
import math
import uuid
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

before_path = Path(
    "evidence/task2/2ecccd58-0c78-4b4f-97cc-2f417b8c0217/"
    "quality_before.json"
)
after_path = Path(
    "evidence/task2/after-abb46f3d-0d82-45ba-8cc1-f0c933935816/"
    "quality_after.json"
)

before = json.loads(before_path.read_text())
after = json.loads(after_path.read_text())

# Chỉ so sánh khi hai báo cáo dùng cùng nguồn và contract.
for field in ["contract_sha256", "input_manifest_sha256"]:
    if not before.get(field) or before[field] != after.get(field):
        raise SystemExit(f"Không thể so sánh: {field} thiếu hoặc khác nhau.")

def index_metrics(report):
    result = {}
    for item in report["metrics"]:
        rule = item["rule_id"]
        if rule in result:
            raise ValueError(f"Metric bị lặp: {rule}")
        n, d = item["numerator"], item["denominator"]
        if not 0 <= n <= d:
            raise ValueError(f"Tử số/mẫu số không hợp lệ: {rule}")
        if d == 0:
            if item["value"] is not None:
                raise ValueError(f"Mẫu số 0 nhưng value khác null: {rule}")
        elif (
            item["status"] != "EVALUATED"
            or item["value"] is None
            or not math.isclose(item["value"], n / d)
        ):
            raise ValueError(f"Metric không nhất quán: {rule}")
        result[rule] = item
    return result

b = index_metrics(before)
a = index_metrics(after)

common = [
    "Q_REQUIRED", "Q_VALUE", "Q_SENSOR",
    "Q_CHRONOLOGY", "Q_UNIQUE", "Q_TIMELY"
]
reference = ["Q_REFERENCE_COVERAGE", "Q_REFERENCE_AGREEMENT"]

for rule in common:
    if rule not in b or rule not in a:
        raise SystemExit(f"Thiếu metric: {rule}")
    if b[rule]["threshold"] != a[rule]["threshold"]:
        raise SystemExit(f"Ngưỡng before/after khác nhau: {rule}")

for rule in reference:
    if rule not in a:
        raise SystemExit(f"Thiếu metric after: {rule}")

out = Path("evidence/task2") / f"comparison-{uuid.uuid4()}"
out.mkdir(parents=True, exist_ok=False)

rows = []
for rule in common + reference:
    row = {"rule_id": rule}
    for label, metrics in [("before", b), ("after", a)]:
        item = metrics.get(rule)
        for field in [
            "numerator", "denominator", "value",
            "cohort", "status", "threshold"
        ]:
            row[f"{label}_{field}"] = item.get(field) if item else None
    item = a[rule]
    row["after_meets_threshold"] = (
        item["status"] == "EVALUATED"
        and item["value"] is not None
        and item["value"] >= item["threshold"]
    )
    rows.append(row)

with (out / "before_after.csv").open(
    "w", encoding="utf-8-sig", newline=""
) as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

# Sáu metric chung; reference chỉ có after nên không vẽ before giả.
fig, ax = plt.subplots(figsize=(12, 6))
positions = list(range(len(common)))
width = 0.36

for shift, data, label, color in [
    (-width / 2, b, "Before", "#6b7280"),
    (width / 2, a, "After", "#007f86")
]:
    values = [
        100 * data[r]["value"]
        if data[r]["value"] is not None else float("nan")
        for r in common
    ]
    bars = ax.bar(
        [x + shift for x in positions], values,
        width, label=label, color=color
    )
    ax.bar_label(bars, fmt="%.2f%%", fontsize=8, padding=3)

ax.set_xticks(positions, [r.removeprefix("Q_") for r in common])
ax.set_ylim(0, 110)
ax.set_yticks(range(0, 101, 20))
ax.set_ylabel("Quality (%)")
ax.set_title("Task 2 — Before / After Curation")
ax.legend(loc="lower right")
ax.grid(axis="y", alpha=0.2)
ax.set_axisbelow(True)
fig.text(
    0.5, 0.025,
    "Cohorts differ. Exact numerators, denominators and cohorts: before_after.csv",
    ha="center", fontsize=9
)
fig.tight_layout(rect=(0, 0.06, 1, 1))
fig.savefig(out / "before_after.png", dpi=200)
plt.close(fig)

def display(item):
    if item is None:
        return "Chưa đo"
    n, d = item["numerator"], item["denominator"]
    if item["value"] is None:
        return f"{n}/{d} — NOT_EVALUATED"
    return f"{n}/{d} = {item['value']:.2%}"

lines = [
    "# Bổ sung Task 2 — Chất lượng sau xử lý",
    "",
    f"- Before run: `{before['run_id']}`",
    f"- After run: `{after['run_id']}`",
    "- Hash contract và input manifest của hai báo cáo khớp nhau.",
    "",
    "| Chỉ số | Before | After | After đạt ngưỡng |",
    "| --- | --- | --- | --- |"
]
for row in rows:
    rule = row["rule_id"]
    result = "Đạt" if row["after_meets_threshold"] else "Không đạt"
    lines.append(
        f"| {rule} | {display(b.get(rule))} | "
        f"{display(a[rule])} | {result} |"
    )

lines += [
    "",
    "## Giải thích kết quả",
    "",
    "Before đo trên winners trước lọc tính hợp lệ; riêng uniqueness "
    "dùng tập key-eligible. After đo trên curated. Timeliness ở cả hai "
    "lần chỉ đo trên các dòng có chronology hợp lệ. Vì cohort thay đổi, "
    "không diễn giải đây là phép đo trên cùng một tập dòng.",
    "",
    "Timeliness giảm nhẹ từ 9690/9890 xuống 9410/9610. "
    "Cả hai cohort có 200 dòng late; sau xử lý, mẫu số giảm "
    "nhưng các dòng late còn hợp lệ vẫn được giữ lại.",
    "",
    "Reference coverage đạt 100/100. Agreement đạt 95/100 trong "
    "sai số 0,05 °C, đúng ngưỡng 95%. Năm ID không đạt agreement "
    "không bị xóa hoặc sửa theo reference để cải thiện kết quả.",
    "",
    "Các defect trước xử lý có thể chồng lặp, không cộng chúng "
    "để suy ra số dòng quarantine.",
    "",
    "Các kết quả này là quan sát chất lượng; không thay thế "
    "gate G01–G09 hoặc approval phát hành.",
    "",
    "![Biểu đồ before/after](before_after.png)",
    "",
    "Chi tiết tử số, mẫu số, cohort và ngưỡng: `before_after.csv`.",
]
(out / "task2-after-report.md").write_text(
    "\n".join(lines) + "\n", encoding="utf-8"
)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

manifest = {
    "before_run_id": before["run_id"],
    "after_run_id": after["run_id"],
    "contract_sha256": before["contract_sha256"],
    "input_manifest_sha256": before["input_manifest_sha256"],
    "source_reports": [
        {"path": str(p), "sha256": sha(p)}
        for p in [before_path, after_path]
    ],
    "comparison_script_sha256": sha(Path(__file__)),
    "source_code_hashes": {
        "before": before.get("code_sha256"),
        "after": after.get("code_sha256")
    },
    "outputs": [
        {"file": p.name, "sha256": sha(p)}
        for p in sorted(out.iterdir()) if p.is_file()
    ]
}
(out / "comparison-manifest.json").write_text(
    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
)

for rule in common + reference:
    print(f"{rule}: {display(b.get(rule))} -> {display(a[rule])}")
print("\nSaved:", out)
