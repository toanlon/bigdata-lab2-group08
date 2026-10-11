#!/usr/bin/env python3
"""Lab 2 Task 3: deterministic curation, exported-data checks and restricted staging.

Uses the already frozen contract. Never writes to research-release.
"""
import argparse
import csv
import hashlib
import json
import os
import random
import re
import sys
import traceback
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).resolve().parent / "task3"
FIELDS = ["source_object", "source_row", "source_sha256", "parse_ok",
          "record_id", "sensor_id", "event_time", "ingest_time", "reading",
          "unit", "operator_email", "raw_payload"]
SCHEMA = list(zip(
    ["record_id", "sensor_id", "site", "event_time_utc", "ingest_time_utc",
     "temperature_c", "is_late", "source_object", "source_row", "source_sha256"],
    ["VARCHAR", "VARCHAR", "VARCHAR", "TIMESTAMP", "TIMESTAMP", "DECIMAL(8,2)",
     "BOOLEAN", "VARCHAR", "BIGINT", "VARCHAR"]))


def now():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def encode(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False,
                               allow_nan=False, default=encode) + "\n", encoding="utf-8")


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def event(out, record, kind, outputs):
    namespace = "urn:bigdata:" + record["store_id"]
    dataset = lambda name: {"namespace": namespace, "name": name}
    data = {"eventType": kind, "eventTime": now(),
            "producer": "urn:vnu-uet:bigdata:lab2:student-pipeline",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {"runId": record["run_id"]},
            "job": {"namespace": "vnu-uet.bigdata.lab2", "name": "curate-and-verify"},
            "inputs": [dataset("s3://research-raw/lab2/inputs/batch-01/" + name)
                       for name in ["observations_a.csv", "observations_b.jsonl", "sensors.csv", "qa_reference.csv"]]
                      + [dataset("urn:sha256:" + record["contract_sha256"])],
            "outputs": [dataset(name) for name in outputs]}
    with (out / "lineage.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")


def csv_rows(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def validate_contract(c):
    # Stop if this implementation does not implement the frozen policy.
    require(c["as_of"] == "2026-02-09T00:00:00Z", "Unexpected AS_OF: review implementation")
    require(c["record_id_pattern"] == "R[0-9]{6}", "Unsupported record ID policy")
    require(c["time"]["format"] == "%Y-%m-%dT%H:%M:%SZ", "Unsupported time format")
    require(c["time"]["retain_otherwise_valid_late_rows"] is True, "Late rows must be retained")
    require(c["temperature"]["supported_units"] == ["C", "F"], "Unsupported units policy")
    require(c["temperature"]["validate_before_rounding"] is True, "Bounds must precede rounding")
    require(c["temperature"]["require_finite"] is True, "Finite values required")
    require(c["deduplication"]["fallback_to_older_valid_version"] is False, "Fallback prohibited")
    expected_order = [{"column": "ingest_ts", "direction": "DESC"},
                      {"column": "source_object", "direction": "ASC"},
                      {"column": "source_row", "direction": "ASC"}]
    require(c["deduplication"]["order"] == expected_order, "Unsupported winner ordering")
    require(c["reason_precedence"]["pre_key"] == ["PARSE", "KEY", "INGEST_PARSE"], "Pre-key precedence mismatch")
    require(c["reason_precedence"]["winner"] == ["REQUIRED", "NUMERIC", "UNIT", "RANGE", "TIME_PARSE", "TIME_ORDER", "REFERENCE"], "Winner precedence mismatch")
    require([(x["name"], x["type"]) for x in c["release_columns"]] == SCHEMA, "Contract schema mismatch")
    require(c["qa_reference"]["expected_reference_ids"] == 100, "Expected 100 reference IDs")


def pipeline(c, rows, registry):
    db = duckdb.connect()
    db.execute("SET TimeZone='UTC'; SET threads=2; SET memory_limit='512MB'")
    variables = {"record_id_pattern": c["record_id_pattern"],
                 "time_format": c["time"]["format"], "as_of": c["as_of"],
                 "supported_units": c["temperature"]["supported_units"],
                 "min_c": c["temperature"]["min_c"], "max_c": c["temperature"]["max_c"],
                 "timely_seconds": c["time"]["timely_max_lag_seconds"]}
    for name, value in variables.items():
        db.execute(f"SET VARIABLE {name} = ?", [value])
    db.execute("CREATE TABLE raw_envelopes (" + ",".join(f"{x} VARCHAR" for x in FIELDS) + ")")
    if rows:
        db.executemany("INSERT INTO raw_envelopes VALUES (" + ",".join("?" for _ in FIELDS) + ")",
                       [[r.get(x) for x in FIELDS] for r in rows])
    db.execute("CREATE TABLE registry(sensor_id VARCHAR, site VARCHAR)")
    if registry:
        db.executemany("INSERT INTO registry VALUES (?,?)", registry)
    for name in ["typed.sql", "curate.sql"]:
        db.execute((SQL_DIR / name).read_text())
    return db


def query(db, sql, params=None):
    result = db.execute(sql, params or [])
    names = [x[0] for x in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def logical_hash(db, table):
    # Full logical rows; decimal values have exact string representations.
    records = [json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
               for row in query(db, f"SELECT * FROM {table}")]
    return hashlib.sha256("\n".join(sorted(records)).encode()).hexdigest()


def replay(c, rows, registry, db):
    tables = ["curated", "quarantine", "duplicates", "winners"]
    baseline = {t: logical_hash(db, t) for t in tables}
    reversed_sources = sorted(rows, key=lambda r: (r["source_object"], int(r["source_row"])), reverse=True)
    shuffled = rows.copy()
    random.Random(20260209).shuffle(shuffled)
    results = []
    for name, items in [("reverse_source_enumeration", reversed_sources), ("shuffle_seed_20260209", shuffled)]:
        other = pipeline(c, items, registry)
        hashes = {t: logical_hash(other, t) for t in tables}
        other.close()
        results.append({"case": name, "logical_hashes": hashes, "equal": hashes == baseline})
    return {"check_id": "C02", "pass": all(r["equal"] for r in results),
            "comparison": "Full logical rows, not Parquet bytes; physical provenance unchanged",
            "baseline": baseline, "replays": results}


def fixtures(c):
    def row(number, **changes):
        item = dict(zip(FIELDS, ["fixture://observations", str(number), "0"*64, "true",
            f"R{number:06d}", "S01", "2026-02-01T00:00:00Z", "2026-02-01T00:01:00Z",
            "10", "C", "", "{}"]))
        item.update(changes)
        return item
    cases = [
        ("F32_to_C0", [row(1, reading="32", unit="F")], "temperature"),
        ("unsupported_K", [row(2, unit="K")], "UNIT"),
        ("nonfinite_NaN", [row(3, reading="NaN")], "NUMERIC"),
        ("blank_optional_email", [row(4, operator_email="   ")], "retain"),
        ("newer_invalid_no_fallback", [row(5), row(6, record_id="R000005", reading="100", ingest_time="2026-02-01T00:02:00Z")], "no_fallback")]
    results = []
    for name, rows, expected in cases:
        db = pipeline(c, rows, [("S01", "fixture-site")])
        out = {t: query(db, f"SELECT * FROM {t}") for t in ["curated", "quarantine", "duplicates"]}
        if expected == "temperature":
            passed = len(out["curated"]) == 1 and out["curated"][0]["temperature_c"] == Decimal("0.00")
        elif expected == "retain":
            passed = len(out["curated"]) == 1 and not out["quarantine"]
        elif expected == "no_fallback":
            passed = (not out["curated"] and len(out["quarantine"]) == 1 and len(out["duplicates"]) == 1
                      and out["quarantine"][0]["source_row"] == 6
                      and out["duplicates"][0]["source_row"] == 5
                      and out["duplicates"][0]["winner_source_row"] == 6)
        else:
            passed = not out["curated"] and len(out["quarantine"]) == 1 and expected in json.loads(out["quarantine"][0]["all_reasons"])
        explanations = {"temperature": "32 F converts to 0.00 C and is retained",
                        "retain": "Optional blank email does not reject a row; release schema omits it",
                        "no_fallback": "Newer invalid winner quarantined; older valid row remains SUPERSEDED",
                        "UNIT": "Unsupported unit rejected, never assumed Celsius",
                        "NUMERIC": "Nonfinite numeric input rejected before conversion/rounding"}
        results.append({"case_id": name, "expected_behavior": explanations[expected],
                        "pass": bool(passed), "observed": out})
        db.close()
    return {"check_id": "C04", "pass": all(x["pass"] for x in results),
            "data_kind": "separate synthetic fixtures, never mixed into candidate", "cases": results}


def conservation(db):
    db.execute("CREATE VIEW output_ids AS SELECT 'curated' kind,source_object,source_row FROM candidate UNION ALL SELECT 'quarantine',source_object,source_row FROM exported_quarantine UNION ALL SELECT 'duplicates',source_object,source_row FROM exported_duplicates")
    sql = {
        "missing_physical_ids": "SELECT source_object,source_row::BIGINT source_row FROM raw_envelopes EXCEPT SELECT source_object,source_row FROM output_ids",
        "extra_physical_ids": "SELECT source_object,source_row FROM output_ids EXCEPT SELECT source_object,source_row::BIGINT FROM raw_envelopes",
        "repeated_or_overlapping_physical_ids": "SELECT source_object,source_row,count(*) n FROM output_ids GROUP BY 1,2 HAVING count(*)<>1",
        "repeated_curated_record_ids": "SELECT record_id,count(*) n FROM candidate GROUP BY 1 HAVING count(*)<>1"}
    failures = {name: query(db, text) for name, text in sql.items()}
    counts = {t: db.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
              for t in ["raw_envelopes", "candidate", "exported_quarantine", "exported_duplicates"]}
    total_ok = counts["raw_envelopes"] == sum(counts[t] for t in counts if t != "raw_envelopes")
    return {"check_id": "C01", "pass": total_ok and not any(failures.values()),
            "counts": counts, "count_reconciliation": total_ok, "failures": failures, "queries": sql}


def quality(db, c, references, run_id):
    db.execute("CREATE TABLE qa_reference(record_id VARCHAR,reference_c DECIMAL(18,8))")
    db.executemany("INSERT INTO qa_reference VALUES (?,?)", references)
    db.execute((SQL_DIR / "quality.sql").read_text())
    thresholds = c["release_thresholds"]
    mapping = dict(Q_REQUIRED="required_completeness", Q_UNIQUE="business_key_uniqueness",
                   Q_VALUE="value_validity", Q_SENSOR="registry_consistency",
                   Q_CHRONOLOGY="temporal_consistency", Q_TIMELY="timeliness")
    rows = query(db, "SELECT * FROM after_metric_counts ORDER BY rule_id")
    matched = db.execute("SELECT count(DISTINCT record_id) FROM qa_matches").fetchone()[0]
    agreement = db.execute("SELECT count(DISTINCT record_id) FROM qa_matches WHERE absolute_error_c<=?", [Decimal(str(c["qa_reference"]["agreement_tolerance_c"]))]).fetchone()[0]
    rows += [dict(rule_id="Q_REFERENCE_COVERAGE", cohort="All frozen reference IDs", numerator=matched, denominator=len(references)),
             dict(rule_id="Q_REFERENCE_AGREEMENT", cohort="Matched frozen reference IDs", numerator=agreement, denominator=matched)]
    mapping.update(Q_REFERENCE_COVERAGE="reference_coverage", Q_REFERENCE_AGREEMENT="reference_agreement")
    for r in rows:
        n, d = r["numerator"], r["denominator"]
        r.update(dataset_id="curated-candidate", run_id=run_id, contract_version=c["contract_version"],
                 value=n/d if d else None, status="EVALUATED" if d else "NOT_EVALUATED",
                 threshold=thresholds[mapping[r["rule_id"]]], severity="BLOCK")
    evaluated = [{"rule_id": r["rule_id"], "pass": r["value"] is not None and r["value"] >= r["threshold"]} for r in rows]
    late = db.execute("SELECT count(*) FROM after_flags WHERE is_late").fetchone()[0]
    mismatches = db.execute("SELECT count(*) FROM after_flags WHERE is_late IS DISTINCT FROM (lag_seconds>getvariable('timely_seconds'))").fetchone()[0]
    source_invalid = db.execute("SELECT count(*) FROM candidate c JOIN classified_winners w USING(source_object,source_row) WHERE NOT w.required_ok OR len(w.reasons)>0").fetchone()[0]
    total = db.execute("SELECT count(*) FROM candidate").fetchone()[0]
    check = {"check_id": "C05", "pass": total > 0 and mismatches == 0 and source_invalid == 0 and all(x["pass"] for x in evaluated),
             "evaluations": evaluated, "curated_count": total, "retained_late_count": late,
             "late_flag_mismatches": mismatches, "invalid_source_winners_released": source_invalid}
    return {"run_id": run_id, "metrics": rows, "retained_late_count": late}, check


def verify_inputs(work, c):
    manifest = load(work / "trusted_manifest.json")
    objects = manifest["objects"]
    required = {"observations_a.csv", "observations_b.jsonl", "sensors.csv", "qa_reference.csv"}
    require({o["key"] for o in objects} == required, "Unexpected source manifest objects")
    source_checks = []
    for obj in objects:
        path = work / "snapshot" / obj["key"]
        require(sha(path) == obj["sha256"], f"Source hash mismatch: {path}")
        size = obj.get("bytes", obj.get("size_bytes", obj.get("size")))
        if size is not None:
            require(path.stat().st_size == size, f"Source size mismatch: {path}")
        source_checks.append({"key": obj["key"], "sha256": sha(path), "bytes": path.stat().st_size})
    rows = csv_rows(work / "envelopes.csv")
    require(bool(rows) and set(FIELDS) <= set(rows[0]), "Envelope fields missing")
    expected = {"s3://research-raw/lab2/inputs/batch-01/" + o["key"]: o for o in objects if o["key"].startswith("observations_")}
    require({r["source_object"] for r in rows} == set(expected), "Unexpected envelope source URI")
    for source, obj in expected.items():
        group = [r for r in rows if r["source_object"] == source]
        require(sorted(int(r["source_row"]) for r in group) == list(range(1, obj["records"]+1)), "Physical source ordinals incomplete or repeated")
        require(all(r["source_sha256"] == obj["sha256"] for r in group), "Envelope source hash mismatch")
        # Reconstruct mapped fields from verified source bytes, not from a claimed hash alone.
        data_fields = ["record_id", "sensor_id", "event_time", "ingest_time", "reading", "unit", "operator_email"]
        json_fields = ["id", "sensor", "timestamp", "arrived_at", "temperature", "temperature_unit", "operator"]
        source_path = work / "snapshot" / obj["key"]
        with source_path.open(encoding="utf-8", newline="") as f:
            originals = list(csv.DictReader(f)) if obj["key"].endswith(".csv") else list(f)
        for original, envelope in zip(originals, sorted(group, key=lambda r: int(r["source_row"]))):
            payload = json.dumps(original) if isinstance(original, dict) else original
            valid = True
            try:
                mapped = original if isinstance(original, dict) else json.loads(original)
                if not isinstance(mapped, dict):
                    raise ValueError("Not object")
                if obj["key"].endswith(".jsonl"):
                    mapped = {a: mapped.get(b) for a,b in zip(data_fields, json_fields)}
                mapped = {k: "" if mapped.get(k) is None else str(mapped[k]) for k in data_fields}
            except (ValueError, TypeError):
                mapped = {k: "" for k in data_fields}
                valid = False
            require(all(envelope[k] == mapped[k] for k in data_fields)
                    and envelope["parse_ok"].lower() == str(valid).lower()
                    and envelope["raw_payload"] == payload,
                    "Envelope mapping/payload differs from verified source: " + source + ":" + envelope["source_row"])
    require(all(r["parse_ok"] in ["true", "false", "True", "False"] for r in rows), "Invalid parse flags")
    registry = [(r["sensor_id"].strip().upper(), r["site"].strip()) for r in csv_rows(work / "snapshot/sensors.csv")]
    require(bool(registry) and len({r[0] for r in registry}) == len(registry) and all(a and b for a,b in registry), "Registry IDs/sites invalid")
    references = [(r["record_id"].strip().upper(), Decimal(r[c["qa_reference"]["reference_column"]])) for r in csv_rows(work / "snapshot/qa_reference.csv")]
    require(len(references) == c["qa_reference"]["expected_reference_ids"] and len({r[0] for r in references}) == len(references), "Reference must contain 100 unique IDs")
    require(all(re.fullmatch(c["record_id_pattern"], a) and b.is_finite() for a,b in references), "Invalid reference ID/value")
    return rows, registry, references, source_checks


def stage_files(out, run_id):
    import boto3
    # Uses existing ingestor credentials, never the owner or release credentials.
    client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    results = []
    for path in sorted(out.rglob("*")):
        if not path.is_file():
            continue
        key = f"lab2/staging/{run_id}/" + path.relative_to(out).as_posix()
        client.upload_file(str(path), "research-raw", key)
        response = client.get_object(Bucket="research-raw", Key=key)
        digest = hashlib.sha256()
        for chunk in response["Body"].iter_chunks(chunk_size=1024*1024):
            digest.update(chunk)
        require(digest.hexdigest() == sha(path), "Uploaded object hash mismatch: " + key)
        results.append({"uri": "s3://research-raw/" + key, "sha256": sha(path), "bytes": path.stat().st_size})
    return {"run_id": run_id, "status": "VERIFIED", "objects": results,
            "note": "This local upload receipt does not include its own hash. No release publication."}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", default="/work")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--stage", action="store_true")
    args = parser.parse_args()
    require(args.operator.strip(), "Operator must be nonempty")
    work = Path(args.workdir).resolve()
    run_id = str(uuid.uuid4())
    out = work / "runs/task3" / run_id
    out.mkdir(parents=True, exist_ok=False)
    record = {"run_id": run_id, "start_time": now(), "operator": args.operator,
              "image_ref": args.image_ref, "store_id": os.environ.get("STORE_ID", "bd-g08-objects"),
              "status": "STARTED", "publication_approved": False,
              "python_version": sys.version, "duckdb_version": duckdb.__version__}
    print("Run ID:", run_id, flush=True)
    print("Output:", out, flush=True)
    lineage_started = False
    lineage_terminal = False
    try:
        c = load(work / "contract.json")
        validate_contract(c)
        frozen_hash = (work / "contract.sha256").read_text().strip().split()[0]
        require(frozen_hash == sha(work / "contract.json"), "Frozen contract hash mismatch")
        review = load(work / "contract-review.json")
        reviewed_hash = review.get("contract_sha256", review.get("sha256"))
        if reviewed_hash is not None:
            require(reviewed_hash == frozen_hash, "Review refers to a different contract hash")
        record.update(as_of=c["as_of"], contract_sha256=frozen_hash,
                      contract_review_sha256=sha(work / "contract-review.json"),
                      contract_review=review,
                      input_manifest_sha256=sha(work / "trusted_manifest.json"),
                      envelope_sha256=sha(work / "envelopes.csv"),
                      input_inventory_sha256=sha(work / "input_inventory.json"))
        code_paths = [Path(__file__).resolve()] + sorted(SQL_DIR.glob("*.sql"))
        code = []
        for path in code_paths:
            relative = path.relative_to(Path(__file__).resolve().parent)
            dest = out / "code" / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(path.read_bytes())
            code.append({"filename": relative.as_posix(), "sha256": sha(path)})
        code.sort(key=lambda x: x["filename"])
        record["code_files"] = code
        record["code_sha256"] = hashlib.sha256(json.dumps(code, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for name in ["contract.json", "contract.sha256", "contract-review.json", "trusted_manifest.json", "input_inventory.json"]:
            target = out / "inputs" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((work / name).read_bytes())
        rows, registry, references, source_checks = verify_inputs(work, c)
        save(out / "source-integrity.json", source_checks)
        event(out, record, "START", [])
        lineage_started = True
        db = pipeline(c, rows, registry)
        artifacts = out / "candidate"
        artifacts.mkdir()
        db.execute(f"COPY (SELECT * FROM curated ORDER BY record_id) TO {literal(artifacts / 'curated.parquet')} (FORMAT PARQUET)")
        for table in ["quarantine", "duplicates"]:
            db.execute(f"COPY (SELECT * FROM {table} ORDER BY source_object,source_row) TO {literal(artifacts / (table+'.csv'))} (FORMAT CSV, HEADER true)")
        db.execute("CREATE TABLE candidate AS SELECT * FROM read_parquet(?)", [str(artifacts / "curated.parquet")])
        for table in ["quarantine", "duplicates"]:
            db.execute(f"CREATE TABLE exported_{table} AS SELECT * REPLACE(source_row::BIGINT AS source_row) FROM read_csv(?,all_varchar=true)", [str(artifacts / (table+".csv"))])
        checks = [conservation(db), replay(c, rows, registry, db)]
        observed_schema = [(r[0],r[1]) for r in db.execute("DESCRIBE candidate").fetchall()]
        schema_check = {"check_id": "C03", "pass": observed_schema == SCHEMA,
                        "expected": c["release_columns"],
                        "observed": [{"name": n, "type": t} for n,t in observed_schema]}
        checks += [schema_check, fixtures(c)]
        report, after_check = quality(db, c, references, run_id)
        report.update(contract_sha256=frozen_hash, code_sha256=record["code_sha256"], input_manifest_sha256=record["input_manifest_sha256"])
        checks.append(after_check)
        for item in checks:
            save(out / (item["check_id"] + ".json"), item)
            print(item["check_id"] + ": " + ("PASS" if item["pass"] else "FAIL"), flush=True)
        save(out / "quality_after.json", report)
        save(out / "reference-comparison.json", query(db, "SELECT * FROM qa_matches ORDER BY record_id"))
        counts = checks[0]["counts"]
        save(out / "schema-counts.json", {"columns": schema_check["observed"], "counts": counts,
             "retained_late_count": report["retained_late_count"],
             "fractions_of_physical_input": {k: v / len(rows) for k,v in counts.items() if k != "raw_envelopes"}})
        save(out / "quarantine-summary.json", query(db, "SELECT primary_reason,count(*) AS count FROM exported_quarantine GROUP BY 1 ORDER BY 1"))
        save(out / "checks.json", {"run_id": run_id, "pass": all(x["pass"] for x in checks), "checks": checks})
        artifact_records = [{"path": p.relative_to(out).as_posix(), "sha256": sha(p), "bytes": p.stat().st_size,
                             "staging_uri": "s3://research-raw/lab2/staging/" + run_id + "/" + p.relative_to(out).as_posix() if args.stage else None}
                            for p in sorted(artifacts.iterdir())]
        save(out / "artifact-manifest.json", {"run_id": run_id, "artifacts": artifact_records,
             "schema": schema_check["observed"], "counts": counts,
             "note": "Staging URIs are planned until stage-upload.json confirms uploaded bytes; this manifest excludes its own hash."})
        # Actual local output URIs exist before emitting COMPLETE. Task 4 still validates the official schema.
        event(out, record, "COMPLETE", [p.as_uri() for p in sorted(artifacts.iterdir())])
        lineage_terminal = True
        record.update(status="COMPLETED" if all(x["pass"] for x in checks) else "CHECKS_FAILED", end_time=now(), counts=counts)
        record["outputs"] = [{"path": p.relative_to(out).as_posix(), "sha256": sha(p), "bytes": p.stat().st_size}
                             for p in sorted(out.rglob("*")) if p.is_file()]
        record["lineage_validation"] = "NOT_EVALUATED: formal OpenLineage validation belongs to subsequent lineage work"
        save(out / "run-record.json", record)
        print("Counts:", json.dumps(counts), flush=True)
        for r in report["metrics"]:
            print(f"{r['rule_id']}: {r['numerator']}/{r['denominator']} = {r['value']}", flush=True)
        print("Retained late rows:", report["retained_late_count"], flush=True)
        if args.stage:
            receipt = stage_files(out, run_id)
            save(out / "stage-upload.json", receipt)
            print("Restricted staging: VERIFIED; research-raw/lab2/staging/" + run_id, flush=True)
        print("Saved:", out, flush=True)
        db.close()
        return 0 if record["status"] == "COMPLETED" else 2
    except Exception as exc:
        if lineage_started and not lineage_terminal:
            event(out, record, "FAIL", [])
        record.update(status="FAILED", end_time=now(), error_type=type(exc).__name__, error=str(exc))
        save(out / "run-record.json", record)
        traceback.print_exc()
        print("Failed run evidence:", out, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())