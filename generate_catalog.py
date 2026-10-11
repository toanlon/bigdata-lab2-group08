import json
import hashlib
from pathlib import Path
import duckdb

def get_sha256(filepath):
    p = Path(filepath)
    if not p.exists():
        return "0000000000000000000000000000000000000000000000000000000000000000"
    return hashlib.sha256(p.read_bytes()).hexdigest()

con = duckdb.connect()

# Đếm số lượng dòng thực tế nếu đã có file
try:
    curated_count = con.execute("SELECT COUNT(*) FROM 'candidate/curated.parquet'").fetchone()[0]
except Exception:
    curated_count = 0

try:
    quarantine_count = con.execute("SELECT COUNT(*) FROM read_csv_auto('quarantine.csv')").fetchone()[0]
except Exception:
    quarantine_count = 0

catalog_data = {
    "datasets": [
        {
            "dataset_id": "raw-observations",
            "title": "Raw Telemetry Observations Collection",
            "purpose": "Original heterogeneous temperature telemetry intake",
            "owner": "Data Ingestion Team",
            "steward": "Data Engineer",
            "classification": "Restricted",
            "allowed_use": "synthetic teaching use",
            "location": [
                "gs://research-raw-bdgxx/lab2/inputs/batch-01/observations_a.csv",
                "gs://research-raw-bdgxx/lab2/inputs/batch-01/observations_b.jsonl"
            ],
            "schema_version": "raw-v1",
            "columns": [
                {"name": "record_id", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw business ID or alias 'id'"},
                {"name": "sensor_id", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw sensor ID or alias 'sensor'"},
                {"name": "event_time", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw timestamp or alias 'timestamp'"},
                {"name": "ingest_time", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw arrival time or alias 'arrived_at'"},
                {"name": "reading", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw temperature reading or alias 'temperature'"},
                {"name": "unit", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Raw unit C/F or alias 'temperature_unit'"},
                {"name": "operator_email", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Restricted operator email or alias 'operator'"}
            ],
            "as_of": "2026-02-09T00:00:00Z",
            "row_count": 10205,
            "input_manifest_sha256": get_sha256("trusted_manifest.json"),
            "contract_sha256": get_sha256("contract.json"),
            "code_sha256": get_sha256("stage_inputs.py"),
            "quality_report": "quality_before.json",
            "retention": {
                "days": 30,
                "start_basis": "retrieval_time",
                "hold": False,
                "dependent_release_ids": ["run-001"],
                "action": "dry-run only"
            },
            "provenance": {
                "job_name": "stage_inputs",
                "run_id": "run-001"
            }
        },
        {
            "dataset_id": "quarantine-records",
            "title": "Quarantined Observation Records",
            "purpose": "Restricted diagnostic ledger of invalid, malformed, or unparseable records",
            "owner": "Data Quality Team",
            "steward": "Quality Engineer",
            "classification": "Restricted",
            "allowed_use": "synthetic teaching use",
            "location": ["gs://research-raw-bdgxx/lab2/staging/run-001/quarantine.csv"],
            "schema_version": "quarantine-v1",
            "columns": [
                {"name": "source_object", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Source file URI"},
                {"name": "source_row", "type": "BIGINT", "unit": "None", "nullable": False, "meaning": "Source line or row ordinal"},
                {"name": "primary_reason", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Primary rejection reason code"},
                {"name": "all_reasons", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Commas-separated all rejection reason codes"}
            ],
            "as_of": "2026-02-09T00:00:00Z",
            "row_count": quarantine_count,
            "input_manifest_sha256": get_sha256("trusted_manifest.json"),
            "contract_sha256": get_sha256("contract.json"),
            "code_sha256": get_sha256("curate.sql"),
            "quality_report": "quality_after.json",
            "retention": {
                "days": 7,
                "start_basis": "run_completion",
                "hold": False,
                "dependent_release_ids": [],
                "action": "dry-run only"
            },
            "provenance": {
                "job_name": "curate",
                "run_id": "run-001"
            }
        },
        {
            "dataset_id": "curated-candidate",
            "title": "Curated Telemetry Temperature Observations",
            "purpose": "Normalized and quality-checked temperature measurements for research analysis",
            "owner": "Data Curator Team",
            "steward": "Lead Data Engineer",
            "classification": "Internal",
            "allowed_use": "synthetic teaching use",
            "location": ["gs://research-raw-bdgxx/lab2/staging/run-001/curated.parquet"],
            "schema_version": "research-telemetry-v1",
            "columns": [
                {"name": "record_id", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Unique record business identifier"},
                {"name": "sensor_id", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Sensor hardware identifier"},
                {"name": "site", "type": "VARCHAR", "unit": "None", "nullable": True, "meaning": "Site location mapped from registry"},
                {"name": "event_time_utc", "type": "TIMESTAMP", "unit": "ISO8601 UTC", "nullable": False, "meaning": "Time reading was taken"},
                {"name": "ingest_time_utc", "type": "TIMESTAMP", "unit": "ISO8601 UTC", "nullable": False, "meaning": "Time reading entered system"},
                {"name": "temperature_c", "type": "DECIMAL(8,2)", "unit": "Celsius", "nullable": False, "meaning": "Selected observation converted to Celsius"},
                {"name": "is_late", "type": "BOOLEAN", "unit": "None", "nullable": False, "meaning": "Flag if ingest lag > 900s"},
                {"name": "source_object", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Source file URI"},
                {"name": "source_row", "type": "BIGINT", "unit": "None", "nullable": False, "meaning": "Source line/row ordinal"},
                {"name": "source_sha256", "type": "VARCHAR", "unit": "None", "nullable": False, "meaning": "Source file SHA256 digest"}
            ],
            "as_of": "2026-02-09T00:00:00Z",
            "row_count": curated_count,
            "input_manifest_sha256": get_sha256("trusted_manifest.json"),
            "contract_sha256": get_sha256("contract.json"),
            "code_sha256": get_sha256("curate.sql"),
            "quality_report": "quality_after.json",
            "retention": {
                "days": 90,
                "start_basis": "publication_time",
                "hold": False,
                "dependent_release_ids": [],
                "action": "dry-run only"
            },
            "provenance": {
                "job_name": "curate",
                "run_id": "run-001"
            }
        }
    ]
}

with open("catalog.json", "w", encoding="utf-8") as f:
    json.dump(catalog_data, f, indent=2)

print("catalog.json updated with all 3 required datasets!")
