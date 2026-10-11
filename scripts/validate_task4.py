import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "catalog.json"
RUN_ID = "abb46f3d-0d82-45ba-8cc1-f0c933935816"
RUN_DIR = ROOT / "evidence" / "task3" / RUN_ID

def load_json(path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)

def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def main():
    errors = []
    catalog = load_json(CATALOG)
    schema = load_json(ROOT / "datasets/Lab2-datasets/schemas/metadata_catalog_schema.json")
    datasets = catalog.get("datasets", [])
    by_id = {d.get("dataset_id"): d for d in datasets}

    if len(datasets) < 3:
        errors.append("Catalog must contain at least three datasets.")
    if len(by_id) != len(datasets):
        errors.append("dataset_id values must be unique.")

    # Standard-library structural checks for the fields required by this schema.
    required = schema["properties"]["datasets"]["items"]["required"]
    for i, ds in enumerate(datasets):
        for key in required:
            if key not in ds:
                errors.append(f"datasets[{i}] missing {key}")
        for col in ds.get("columns", []):
            for key in ("name", "type", "meaning", "unit", "nullable"):
                if key not in col:
                    errors.append(f"{ds.get('dataset_id')}: column missing {key}")

        for key in ("input_manifest_sha256", "contract_sha256", "code_sha256"):
            if not re.fullmatch(r"[a-f0-9]{64}", str(ds.get(key, ""))):
                errors.append(f"{ds.get('dataset_id')}: invalid {key}")

        report = ROOT / ds.get("quality_report", "")
        if not report.is_file():
            errors.append(f"{ds.get('dataset_id')}: quality report not found: {report}")

        retention = ds.get("retention", {})
        if retention.get("action") != "dry-run only":
            errors.append(f"{ds.get('dataset_id')}: retention must remain dry-run only")

        if ds.get("provenance", {}).get("run_id") != RUN_ID:
            errors.append(f"{ds.get('dataset_id')}: unexpected provenance run_id")

    # Hashes grounded in the actual contract, trusted manifest, and Task 3 run record.
    contract_hash = sha256(ROOT / "contract.json")
    manifest_hash = sha256(ROOT / "trusted_manifest.json")
    run_record = load_json(RUN_DIR / "run-record.json")
    code_hash = run_record.get("code_sha256")

    for ds in datasets:
        if ds.get("contract_sha256") != contract_hash:
            errors.append(f"{ds.get('dataset_id')}: contract hash mismatch")
        if ds.get("input_manifest_sha256") != manifest_hash:
            errors.append(f"{ds.get('dataset_id')}: input manifest hash mismatch")
        if code_hash and ds.get("code_sha256") != code_hash:
            errors.append(f"{ds.get('dataset_id')}: code hash mismatch")

    curated = by_id.get("curated-candidate")
    quarantine = by_id.get("quarantine-records")
    raw = by_id.get("raw-observations")

    if curated:
        names = {c.get("name") for c in curated.get("columns", [])}
        forbidden = {"operator_email", "raw_payload"}
        if names & forbidden:
            errors.append(f"Curated schema contains restricted columns: {sorted(names & forbidden)}")
        if "temperature_c" not in names:
            errors.append("Curated schema must contain temperature_c.")

    if quarantine:
        if quarantine.get("classification") != "Restricted":
            errors.append("Quarantine must be classified Restricted.")
        names = {c.get("name") for c in quarantine.get("columns", [])}
        if not {"source_object", "source_row", "source_sha256", "primary_reason"} <= names:
            errors.append("Quarantine schema lacks provenance or rejection reason fields.")

    if raw and raw.get("classification") != "Restricted":
        errors.append("Raw observations must be classified Restricted.")

    if run_record.get("status") != "COMPLETED":
        errors.append("Task 3 run record is not COMPLETED.")
    if run_record.get("publication_approved") is not False:
        errors.append("Expected publication_approved=false for this candidate.")

    if errors:
        print("VALIDATION FAILED")
        for e in errors:
            print(" -", e)
        return 1

    print("PASS: semantic Task 4 checks")
    print("Datasets:", ", ".join(sorted(by_id)))
    print("Task 3 completed; candidate remains unpublished.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
