import hashlib
import json
import platform
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def query_rows(con, query):
    cursor = con.execute(query)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(
        f"Unsupported JSON type: {type(value).__name__}"
    )


def save_json(path, value):
    Path(path).write_text(
        json.dumps(
            value,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
            default=json_default,
        ) + "\n",
        encoding="utf-8",
    )


# Kiểm tra contract đã chốt.
contract = json.loads(Path("contract.json").read_text(encoding="utf-8"))
contract_hash = sha256("contract.json")
frozen_hash = Path("contract.sha256").read_text().split()[0]
review = json.loads(Path("contract-review.json").read_text(encoding="utf-8"))

if contract_hash != frozen_hash:
    raise RuntimeError("CONTRACT_INTEGRITY: contract khác bản đã chốt")

if (
    review.get("decision") != "APPROVED"
    or review.get("contract_sha256") != contract_hash
):
    raise RuntimeError("CONTRACT_REVIEW: review không khớp contract")


# Kiểm tra snapshot đúng phiên bản.
manifest = json.loads(
    Path("trusted_manifest.json").read_text(encoding="utf-8")
)

source_hashes = {}
for obj in manifest["objects"]:
    path = Path("snapshot") / obj["key"]
    digest = sha256(path)

    if path.stat().st_size != obj["bytes"] or digest != obj["sha256"]:
        raise RuntimeError("INPUT_INTEGRITY: " + obj["key"])

    source_hashes[obj["key"]] = digest


# Nhận diện code thực thi.
code_files = [
    "typed.sql", "curate.sql", "quality.sql", "run_quality.py"
]
code_hashes = {
    name: sha256(name)
    for name in sorted(code_files)
}
canonical_code = json.dumps(
    code_hashes, sort_keys=True, separators=(",", ":")
).encode("utf-8")
code_hash = hashlib.sha256(canonical_code).hexdigest()

run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc).isoformat()

with duckdb.connect() as con:
    parameters = {
        "record_id_pattern": contract["record_id_pattern"],
        "as_of": contract["as_of"],
        "min_c": contract["temperature"]["min_c"],
        "max_c": contract["temperature"]["max_c"],
        "timely_seconds": contract["time"]["timely_max_lag_seconds"],
    }

    # Đưa tham số vào bảng một dòng rồi gán SQL variables.
    con.execute("""
        CREATE TEMP TABLE policy (
            record_id_pattern VARCHAR,
            as_of VARCHAR,
            min_c DOUBLE,
            max_c DOUBLE,
            timely_seconds BIGINT
        )
    """)
    con.execute(
        "INSERT INTO policy VALUES (?, ?, ?, ?, ?)",
        list(parameters.values()),
    )

    for name in parameters:
        con.execute(
            f"SET VARIABLE {name} = (SELECT {name} FROM policy)"
        )

    for filename in ("typed.sql", "curate.sql", "quality.sql"):
        con.execute(Path(filename).read_text(encoding="utf-8"))

    metrics = query_rows(
        con, "SELECT * FROM quality_before ORDER BY rule_id"
    )

    threshold_keys = {
        "Q_REQUIRED": "required_completeness",
        "Q_UNIQUE": "business_key_uniqueness",
        "Q_VALUE": "value_validity",
        "Q_SENSOR": "registry_consistency",
        "Q_CHRONOLOGY": "temporal_consistency",
        "Q_TIMELY": "timeliness",
    }

    # Cập nhật ngưỡng từ contract cho kết quả và các truy vấn tiếp theo.
    threshold_cases = " ".join(
        f"WHEN '{rule_id}' THEN "
        f"{float(contract['release_thresholds'][key])}"
        for rule_id, key in threshold_keys.items()
    )
    con.execute(f"""
        CREATE OR REPLACE VIEW quality_before_contract AS
        SELECT * EXCLUDE (threshold),
               CASE rule_id {threshold_cases} END AS threshold
        FROM quality_before
    """)
    metrics = query_rows(
        con, "SELECT * FROM quality_before_contract ORDER BY rule_id"
    )

    for metric in metrics:
        metric.update({
            "dataset_id": "before-curation",
            "dataset_version": sha256("trusted_manifest.json"),
            "run_id": run_id,
            "contract_version": contract["contract_version"],
        })

    intake = query_rows(con, "SELECT * FROM intake_summary")[0]
    required_intake = query_rows(
        con, "SELECT * FROM parsed_intake_required_summary"
    )[0]
    defects = query_rows(
        con, "SELECT * FROM defect_counts ORDER BY defect"
    )

    # Các điều kiện cố định trong code, không nhận SQL từ người dùng.
    categories = {
        "REQUIRED": "NOT required_ok",
        "VALUE": "NOT value_ok",
        "UNKNOWN_SENSOR": "NOT sensor_ok",
        "CHRONOLOGY": "NOT chronology_ok",
        "LATE": (
            "chronology_ok AND "
            "lag_seconds > getvariable('timely_seconds')"
        ),
    }

    samples = {}
    for category, condition in categories.items():
        samples[category] = query_rows(con, f"""
            SELECT
                source_object, source_row, record_id,
                sensor_id, unit, event_time, ingest_time,
                required_ok, value_ok, sensor_ok, chronology_ok
            FROM before_flags
            WHERE {condition}
            ORDER BY source_object, source_row
            LIMIT 3
        """)

report = {
    "run_id": run_id,
    "started_at": started_at,
    "completed_at": datetime.now(timezone.utc).isoformat(),
    "store_id": "bd-g08-objects",
    "dataset_id": "before-curation",
    "dataset_version": sha256("trusted_manifest.json"),
    "contract_version": contract["contract_version"],
    "as_of": contract["as_of"],
    "input_manifest_sha256": sha256("trusted_manifest.json"),
    "source_sha256": source_hashes,
    "envelopes_sha256": sha256("envelopes.csv"),
    "contract_sha256": contract_hash,
    "code_sha256": code_hash,
    "code_files_sha256": code_hashes,
    "python_version": platform.python_version(),
    "duckdb_version": duckdb.__version__,
    "intake": intake,
    "parsed_intake_required": required_intake,
    "metrics": metrics,
    "defects": defects,
    "pending_after_task3": [
        "after_metrics",
        "reference_coverage",
        "reference_agreement",
        "before_after_chart",
    ],
}

output = Path("evidence/task2") / run_id
output.mkdir(parents=True, exist_ok=False)

save_json(output / "quality_before.json", report)
save_json(output / "defect_samples.json", samples)

print("Run ID:", run_id)
print("Input counts:", json.dumps(intake))
print("\nBEFORE QUALITY")
for metric in metrics:
    value = metric["value"]
    display = "NOT_EVALUATED" if value is None else f"{value:.2%}"
    print(
        f"{metric['rule_id']}: "
        f"{metric['numerator']}/{metric['denominator']} = {display}"
    )

print("\nDEFECT COUNTS")
for defect in defects:
    print(f"{defect['defect']}: {defect['affected_rows']}")

print("\nSaved:", output)
