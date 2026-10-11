import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "abb46f3d-0d82-45ba-8cc1-f0c933935816"
OUT = ROOT / "evidence" / "task4" / RUN_ID / "retention-dry-run.json"

# Fixed evaluation time makes the dry run deterministic.
now = datetime(2026, 10, 11, tzinfo=timezone.utc)

cases = [
    {
        "case_id": "R01",
        "object_id": "expired-scratch",
        "expires_at": "2026-10-01T00:00:00Z",
        "hold": False,
        "active_dependency": False,
        "expected": "ELIGIBLE"
    },
    {
        "case_id": "R02",
        "object_id": "expired-quarantine-hold",
        "expires_at": "2026-10-01T00:00:00Z",
        "hold": True,
        "active_dependency": False,
        "expected": "KEEP/HOLD"
    },
    {
        "case_id": "R03",
        "object_id": "expired-raw-active-release-dependency",
        "expires_at": "2026-10-01T00:00:00Z",
        "hold": False,
        "active_dependency": True,
        "expected": "KEEP/ACTIVE_DEPENDENCY"
    },
    {
        "case_id": "R04",
        "object_id": "unexpired-object",
        "expires_at": "2026-12-01T00:00:00Z",
        "hold": False,
        "active_dependency": False,
        "expected": "KEEP/NOT_EXPIRED"
    }
]

results = []
for c in cases:
    expiry = datetime.fromisoformat(c["expires_at"].replace("Z", "+00:00"))
    if c["hold"]:
        decision = "KEEP/HOLD"
    elif c["active_dependency"]:
        decision = "KEEP/ACTIVE_DEPENDENCY"
    elif now < expiry:
        decision = "KEEP/NOT_EXPIRED"
    else:
        decision = "ELIGIBLE"

    results.append({
        **c,
        "decision": decision,
        "evaluated_at": now.isoformat().replace("+00:00", "Z"),
        "deletion_performed": False,
        "passed": decision == c["expected"]
    })

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps({
    "mode": "DRY_RUN_ONLY",
    "run_id": RUN_ID,
    "evaluated_at": now.isoformat().replace("+00:00", "Z"),
    "deletion_performed": False,
    "results": results
}, indent=2) + "\n", encoding="utf-8")

for r in results:
    print(r["case_id"], r["object_id"], "=>", r["decision"],
          "|", "PASS" if r["passed"] else "FAIL")

if not all(r["passed"] for r in results):
    raise SystemExit(1)

print("Wrote", OUT.relative_to(ROOT))
