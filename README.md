# Big Data Lab 2 — Data Curation and Governance

## Overview

This repository contains the Lab 2 dataset package and evidence for data curation, quality validation, metadata governance, lineage, and retention controls. The supplied observations are synthetic teaching data.

## Dataset package

See `datasets/Lab2-datasets/README.md` for the source-file inventory, field mapping, provenance conventions, evaluation cutoff, and handling rules. Do not modify the original files under `datasets/Lab2-datasets/input/`.

## Task 4: Metadata governance

The root-level `catalog.json` describes three datasets:

- `raw-observations`: restricted source observations.
- `quarantine-records`: rejected records and their provenance and reasons.
- `curated-candidate`: the curated Celsius dataset proposed for downstream use.

The catalog schema is `datasets/Lab2-datasets/schemas/metadata_catalog_schema.json`. Task 4 validation scripts are under `scripts/`.

## Validation

Run the semantic catalog checks:

```bash
python scripts/validate_task4.py
```

Run the retention dry-run cases:

```bash
python scripts/task4_retention_dry_run.py
```

These checks do not delete data or publish a release.

## Evidence

Task 3 execution evidence is stored under `evidence/task3/`. Task 4 evidence is stored under `evidence/task4/`, including the catalog-query and restricted-data examples, accepted-row and quarantine-row traces, lineage validation, retention dry run, and staging reconciliation.

The staging reconciliation compares local artifacts with the staging receipt; it does not independently read back objects from S3. The lineage validation also records the output-URI reconciliation warning. These limitations must be resolved or explicitly reported before claiming full end-to-end verification.

## Release status

The current curated dataset is a candidate only. The Task 3 run record reports `publication_approved=false`. Do not describe the candidate as a published release.
