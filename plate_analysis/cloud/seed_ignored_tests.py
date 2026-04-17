"""
cloud/seed_ignored_tests.py
----------------------------
One-time setup script: scan the GCS bucket for all current top-level folder
names and write them to the Firestore  ignored_tests  collection.

The Cloud Function event router loads this collection on startup and skips
any test_id it finds there, so pre-existing data never triggers the pipeline.

Run this ONCE, before deploying the Cloud Function:

    cd /home/rami/plate_analysis
    python cloud/seed_ignored_tests.py

Safe to re-run: uses merge=True so existing entries are not overwritten.

Firestore schema written:
    Collection: ignored_tests
      Document: {test_id}           ← the folder name is the document ID
        folder_name: str            ← same value, for readability in console
        reason:      "pre-existing"
        added_at:    datetime (UTC)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from google.cloud import firestore, storage

GCS_BUCKET    = os.environ["GCS_BUCKET"]
GCP_PROJECT   = os.environ["GCP_PROJECT"]
FIRESTORE_DB  = os.getenv("FIRESTORE_DB", "plate-analysis")


def seed() -> None:
    gcs = storage.Client(project=GCP_PROJECT)
    db  = firestore.Client(project=GCP_PROJECT, database=FIRESTORE_DB)

    # The GCS "delimiter" API returns virtual top-level folder names as
    # common prefixes (e.g. "test-rami/").  We must exhaust the iterator
    # before reading .prefixes.
    blobs_iter = gcs.list_blobs(GCS_BUCKET, delimiter="/")
    list(blobs_iter)                              # exhaust to populate prefixes
    prefixes = blobs_iter.prefixes or set()       # e.g. {"test-rami/", "180rpm_Demo/", ...}

    folder_names: list[str] = sorted(p.rstrip("/") for p in prefixes)
    print(f"Found {len(folder_names)} top-level folders in gs://{GCS_BUCKET}/")

    if not folder_names:
        print("Nothing to write.")
        return

    now = datetime.now(timezone.utc)
    col = db.collection("ignored_tests")

    # Firestore batch writes are limited to 500 ops; chunk if needed.
    BATCH_SIZE = 400
    total = 0
    for i in range(0, len(folder_names), BATCH_SIZE):
        batch = db.batch()
        chunk = folder_names[i : i + BATCH_SIZE]
        for name in chunk:
            ref = col.document(name)
            batch.set(ref, {
                "folder_name": name,
                "reason":      "pre-existing",
                "added_at":    now,
            }, merge=True)     # merge=True → safe to re-run without data loss
        batch.commit()
        total += len(chunk)
        print(f"  Committed {total}/{len(folder_names)} …")

    print(f"\nDone. {len(folder_names)} test IDs written to Firestore ignored_tests/")
    print("Ignored folders:")
    for name in folder_names:
        print(f"  {name}")


if __name__ == "__main__":
    seed()
