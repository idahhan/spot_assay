"""
cloud/event_router/main.py
--------------------------
Cloud Function Gen 2 — thin event router.

Triggered by: Firebase Storage  object.finalized  (Eventarc)

Responsibility (only these four things):
  1. Parse the object path; reject non-images, /results/ objects,
     malformed paths, and ignored test IDs.
  2. Upsert Firestore state for the test (image count, last image path, …).
  3. In a single Firestore transaction, check whether a queued/running job
     already exists; if not, atomically mark the test as queued.
  4. Enqueue one delayed Cloud Task (task name = test_id + time bucket
     → burst uploads collapse into a single analysis run).

No heavy logic lives here.  All analysis happens in Cloud Run.

Environment variables (set in Cloud Function config):
  GCP_PROJECT        — GCP project ID
  QUEUE_LOCATION     — Cloud Tasks queue region, e.g. "us-central1"
  QUEUE_ID           — Cloud Tasks queue name, e.g. "analysis-queue"
  CLOUD_RUN_URL      — full HTTPS URL for the /analyze endpoint
  CLOUD_RUN_SA_EMAIL — service-account email used for OIDC auth on Cloud Run
  QUIET_PERIOD_SECS  — (optional, default 120) burst-collapse window in seconds
  IGNORED_TESTS      — (optional) comma-separated test IDs to skip
"""

from __future__ import annotations

import math
import os
import re
import time

import functions_framework
from google.cloud import firestore, tasks_v2
from google.protobuf import timestamp_pb2

# ── Config ────────────────────────────────────────────────────────────────────
STORAGE_PREFIX     = "PiCultureCam"     # expected top-level prefix in the bucket
RESULTS_SUBDIR     = "/results/"        # output subdirectory — never re-trigger on these
IMAGE_SUFFIXES     = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}

QUIET_PERIOD_SECS  = int(os.getenv("QUIET_PERIOD_SECS", "120"))

GCP_PROJECT        = os.environ["GCP_PROJECT"]
QUEUE_LOCATION     = os.environ["QUEUE_LOCATION"]
QUEUE_ID           = os.environ["QUEUE_ID"]
CLOUD_RUN_URL      = os.environ["CLOUD_RUN_URL"]
CLOUD_RUN_SA_EMAIL = os.environ["CLOUD_RUN_SA_EMAIL"]

IGNORED_TESTS: frozenset[str] = frozenset(
    t.strip() for t in os.getenv("IGNORED_TESTS", "").split(",") if t.strip()
)

# ── Lazy singletons (reused across warm invocations) ─────────────────────────
_db: firestore.Client | None = None
_tasks: tasks_v2.CloudTasksClient | None = None


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client()
    return _db


def _get_tasks() -> tasks_v2.CloudTasksClient:
    global _tasks
    if _tasks is None:
        _tasks = tasks_v2.CloudTasksClient()
    return _tasks


# ── Helpers ───────────────────────────────────────────────────────────────────
_SAFE_RE = re.compile(r"[^a-zA-Z0-9_-]")


def _sanitize(s: str) -> str:
    """Reduce test_id to characters valid in a Cloud Tasks task name."""
    return _SAFE_RE.sub("-", s)[:200]


def _queue_path() -> str:
    return _get_tasks().queue_path(GCP_PROJECT, QUEUE_LOCATION, QUEUE_ID)


def _task_name(test_id: str) -> str:
    """
    Stable task name within a QUIET_PERIOD_SECS window.

    Any number of uploads for the same test within the same window produce the
    same task name → Cloud Tasks deduplicates them automatically (ALREADY_EXISTS).
    After the window expires, the bucket number increments → a fresh task is
    allowed.

    Format: {queue_path}/tasks/{safe_test_id}--{bucket_number}
    """
    bucket = math.floor(time.time() / QUIET_PERIOD_SECS)
    return f"{_queue_path()}/tasks/{_sanitize(test_id)}--{bucket}"


# ── Firestore transaction ─────────────────────────────────────────────────────

@firestore.transactional
def _upsert_and_try_queue(
    transaction: firestore.Transaction,
    doc_ref: firestore.DocumentReference,
    test_id: str,
    image_path: str,
) -> bool:
    """
    Atomically:
      - upsert image-arrival fields on the test document
      - mark status → 'queued' ONLY IF the test is currently idle/complete/failed
        (i.e. not already queued or running)

    Returns True if this invocation should enqueue a Cloud Task.
    """
    snap = doc_ref.get(transaction=transaction)
    existing: dict = snap.to_dict() if snap.exists else {}
    status: str = existing.get("status", "idle")

    should_queue = status not in ("queued", "running")

    # Build the single write (one write per doc per transaction)
    fields: dict = {
        "test_id":         test_id,
        "last_image_path": image_path,
        "last_image_time": firestore.SERVER_TIMESTAMP,
        "image_count":     firestore.Increment(1),
    }

    if not snap.exists:
        # First image ever seen for this test
        fields["created_at"] = firestore.SERVER_TIMESTAMP

    if should_queue:
        fields["status"]           = "queued"
        fields["last_enqueued_at"] = firestore.SERVER_TIMESTAMP
    # If already queued/running we still update image metadata but don't change status.

    if snap.exists:
        transaction.update(doc_ref, fields)
    else:
        transaction.set(doc_ref, fields)

    return should_queue


def _enqueue_task(test_id: str, task_name: str) -> None:
    """Create a delayed Cloud Task that calls Cloud Run /analyze."""
    import json

    client = _get_tasks()
    scheduled_epoch = int(time.time()) + QUIET_PERIOD_SECS

    ts = timestamp_pb2.Timestamp()
    ts.FromSeconds(scheduled_epoch)

    task = {
        "name":          task_name,
        "schedule_time": ts,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url":         CLOUD_RUN_URL,
            "headers":     {"Content-Type": "application/json"},
            "body":        json.dumps({"test_id": test_id}).encode(),
            "oidc_token":  {"service_account_email": CLOUD_RUN_SA_EMAIL},
        },
    }

    try:
        client.create_task(request={"parent": _queue_path(), "task": task})
    except Exception as exc:
        if "ALREADY_EXISTS" in str(exc):
            # Same time bucket → same task already queued → burst successfully collapsed
            pass
        else:
            raise


# ── Entry point ───────────────────────────────────────────────────────────────

@functions_framework.cloud_event
def on_image_finalized(cloud_event) -> None:
    """Triggered by Firebase Storage object.finalized events."""
    data: dict       = cloud_event.data
    object_path: str = data["name"]  # e.g. PiCultureCam/test123/2024-04-02_14-00-00.jpg

    # ── Guard 1: only image files ─────────────────────────────────────────────
    dot = object_path.rfind(".")
    suffix = object_path[dot:].lower() if dot != -1 else ""
    if suffix not in IMAGE_SUFFIXES:
        return

    # ── Guard 2: never re-trigger on result outputs ───────────────────────────
    if RESULTS_SUBDIR in object_path:
        return

    # ── Guard 3: path must be PiCultureCam/{test_id}/{filename} ──────────────
    parts = object_path.split("/")
    if len(parts) < 3 or parts[0] != STORAGE_PREFIX:
        return

    test_id: str = parts[1]

    # ── Guard 4: explicitly ignored tests ─────────────────────────────────────
    if test_id in IGNORED_TESTS:
        return

    # ── Upsert Firestore + conditionally mark queued (single transaction) ─────
    db      = _get_db()
    doc_ref = db.collection("tests").document(test_id)
    txn     = db.transaction()

    should_queue = _upsert_and_try_queue(txn, doc_ref, test_id, object_path)

    if not should_queue:
        # A queued or running job already exists.
        # The time-bucket task name provides secondary safety: if the function
        # retried and the transaction mistakenly returned False, the Cloud Tasks
        # ALREADY_EXISTS response would still prevent a duplicate task.
        return

    # ── Enqueue delayed task ──────────────────────────────────────────────────
    _enqueue_task(test_id, _task_name(test_id))
