"""
cloud/event_router/main.py
--------------------------
Cloud Function Gen 2 — thin event router.

Triggered by: Firebase Storage  object.finalized  (Eventarc)

Responsibility (only these four things):
  1. Parse the object path; reject non-images, result artifacts,
     malformed paths, and ignored test IDs.
  2. Upsert Firestore state for the test (image count, last image path, …).
  3. In a single Firestore transaction, check whether a queued/running job
     already exists; if not, atomically mark the test as queued.
  4. Enqueue one delayed Cloud Task (task name = test_id + time bucket
     → burst uploads collapse into a single analysis run).

Storage path format (no fixed prefix):
  {test_id}/{timestamp}.jpg
  e.g. test-rami/2026-04-16_16-33-41.jpg

Ignored tests (pre-existing folders) are stored in the Firestore
collection  ignored_tests/{test_id}  and loaded once per instance.
Run  cloud/seed_ignored_tests.py  once before deploying to populate it.

Environment variables (set in Cloud Function config):
  GCP_PROJECT        — GCP project ID
  QUEUE_LOCATION     — Cloud Tasks queue region, e.g. "us-central1"
  QUEUE_ID           — Cloud Tasks queue name, e.g. "analysis-queue"
  CLOUD_RUN_URL      — full HTTPS URL for the /analyze endpoint
  CLOUD_RUN_SA_EMAIL — service-account email used for OIDC auth on Cloud Run
  QUIET_PERIOD_SECS  — (optional, default 120) burst-collapse window in seconds
"""

from __future__ import annotations

import logging
import math
import os
import re
import time

import functions_framework
from google.cloud import firestore, tasks_v2
from google.protobuf import timestamp_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("event_router")

# ── Static config ─────────────────────────────────────────────────────────────
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}

# Any object whose path contains one of these strings is a pipeline output.
# Checking against the full path catches nested paths like
# test-rami/results/plate_grids/foo.png.
RESULT_INDICATORS: frozenset[str] = frozenset([
    "/results/",
    "well_colors.csv",
    "latest_summary.png",
    "/plate_grids/",
    "/overlays/",
])

QUIET_PERIOD_SECS  = int(os.getenv("QUIET_PERIOD_SECS", "120"))

GCP_PROJECT        = os.environ["GCP_PROJECT"]
QUEUE_LOCATION     = os.environ["QUEUE_LOCATION"]
QUEUE_ID           = os.environ["QUEUE_ID"]
CLOUD_RUN_URL      = os.environ["CLOUD_RUN_URL"]
CLOUD_RUN_SA_EMAIL = os.environ["CLOUD_RUN_SA_EMAIL"]
FIRESTORE_DB       = os.getenv("FIRESTORE_DB", "plate-analysis")

# ── Lazy singletons ───────────────────────────────────────────────────────────
_db: firestore.Client | None = None
_tasks: tasks_v2.CloudTasksClient | None = None
_ignored_tests: frozenset[str] | None = None   # loaded from Firestore once per instance


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(database=FIRESTORE_DB)
    return _db


def _get_tasks() -> tasks_v2.CloudTasksClient:
    global _tasks
    if _tasks is None:
        _tasks = tasks_v2.CloudTasksClient()
    return _tasks


def _get_ignored_tests() -> frozenset[str]:
    """
    Load the ignored-test-ID set from Firestore, cached for the instance lifetime.

    Pre-existing folder names are written to  ignored_tests/{test_id}  by
    cloud/seed_ignored_tests.py before the first deployment.  New entries take
    effect on the next cold start.
    """
    global _ignored_tests
    if _ignored_tests is not None:
        return _ignored_tests
    docs = _get_db().collection("ignored_tests").stream()
    _ignored_tests = frozenset(d.id for d in docs)
    log.info("Loaded %d ignored test IDs from Firestore.", len(_ignored_tests))
    return _ignored_tests


# ── Helpers ───────────────────────────────────────────────────────────────────
_SAFE_RE = re.compile(r"[^a-zA-Z0-9_-]")


def _sanitize(s: str) -> str:
    """Reduce a string to characters valid in a Cloud Tasks task name."""
    return _SAFE_RE.sub("-", s)[:200]


def _queue_path() -> str:
    return _get_tasks().queue_path(GCP_PROJECT, QUEUE_LOCATION, QUEUE_ID)


def _task_name(test_id: str) -> str:
    """
    Stable task name within a QUIET_PERIOD_SECS window.

    Any number of uploads for the same test within the same window produce the
    same task name → Cloud Tasks deduplicates them automatically (ALREADY_EXISTS).
    After the window expires, the bucket number increments → a fresh task is allowed.

    Format: {queue_path}/tasks/{safe_test_id}--{bucket_number}
    """
    bucket = math.floor(time.time() / QUIET_PERIOD_SECS)
    return f"{_queue_path()}/tasks/{_sanitize(test_id)}--{bucket}"


def _is_result_path(path: str) -> bool:
    """Return True if the object is a pipeline-generated output artifact."""
    return any(ind in path for ind in RESULT_INDICATORS)


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

    Returns True if this invocation should enqueue a Cloud Task.
    """
    snap = doc_ref.get(transaction=transaction)
    existing: dict = snap.to_dict() if snap.exists else {}
    status: str = existing.get("status", "idle")

    should_queue = status not in ("queued", "running")

    fields: dict = {
        "test_id":         test_id,
        "last_image_path": image_path,
        "last_image_time": firestore.SERVER_TIMESTAMP,
        "image_count":     firestore.Increment(1),
    }

    if not snap.exists:
        fields["created_at"] = firestore.SERVER_TIMESTAMP

    if should_queue:
        fields["status"]           = "queued"
        fields["last_enqueued_at"] = firestore.SERVER_TIMESTAMP

    if snap.exists:
        transaction.update(doc_ref, fields)
    else:
        transaction.set(doc_ref, fields)

    return should_queue


def _cloud_run_audience(url: str) -> str:
    """Return the Cloud Run base URL (no path) for use as the OIDC audience.

    Cloud Run validates that the OIDC token's `aud` claim equals the service
    base URL.  Cloud Tasks defaults to using the full task URL (including the
    /analyze path) when no audience is specified, which causes a mismatch and
    a 403.  Stripping the path fixes it.
    """
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _enqueue_task(test_id: str, task_name: str) -> None:
    """Create a delayed Cloud Task that calls Cloud Run /analyze."""
    import json

    client = _get_tasks()
    scheduled_epoch = int(time.time()) + QUIET_PERIOD_SECS

    ts = timestamp_pb2.Timestamp()
    ts.FromSeconds(scheduled_epoch)

    audience = _cloud_run_audience(CLOUD_RUN_URL)
    log.info(
        "[DEBUG] Enqueueing task | target_url=%s | oidc_audience=%s | sa_email=%s | task_name=%s",
        CLOUD_RUN_URL, audience, CLOUD_RUN_SA_EMAIL, task_name,
    )

    task = {
        "name":          task_name,
        "schedule_time": ts,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url":         CLOUD_RUN_URL,
            "headers":     {"Content-Type": "application/json"},
            "body":        json.dumps({"test_id": test_id}).encode(),
            "oidc_token":  {
                "service_account_email": CLOUD_RUN_SA_EMAIL,
                "audience":              audience,
            },
        },
    }

    try:
        client.create_task(request={"parent": _queue_path(), "task": task})
        log.info("[DEBUG] Task created successfully for test_id=%s", test_id)
    except Exception as exc:
        if "ALREADY_EXISTS" in str(exc):
            # Same time bucket → burst successfully collapsed
            log.info("[DEBUG] Task already exists (burst collapsed) for test_id=%s", test_id)
        else:
            raise


# ── Entry point ───────────────────────────────────────────────────────────────

@functions_framework.cloud_event
def on_image_finalized(cloud_event) -> None:
    """Triggered by Firebase Storage object.finalized events.

    Expected object path format:  {test_id}/{filename}
    Example:  test-rami/2026-04-16_16-33-41.jpg
    """
    data: dict       = cloud_event.data
    object_path: str = data["name"]

    # ── Guard 1: only image files ─────────────────────────────────────────────
    dot    = object_path.rfind(".")
    suffix = object_path[dot:].lower() if dot != -1 else ""
    if suffix not in IMAGE_SUFFIXES:
        return

    # ── Guard 2: skip pipeline result artifacts ───────────────────────────────
    if _is_result_path(object_path):
        return

    # ── Guard 3: path must have at least two segments: {test_id}/{filename} ───
    parts = object_path.split("/")
    if len(parts) < 2:
        log.warning("Ignoring malformed path (< 2 segments): %s", object_path)
        return

    test_id: str = parts[0]

    # ── Guard 4: skip pre-existing / explicitly ignored tests ─────────────────
    if test_id in _get_ignored_tests():
        return

    # ── Upsert Firestore + conditionally mark queued (single transaction) ─────
    db      = _get_db()
    doc_ref = db.collection("tests").document(test_id)
    txn     = db.transaction()

    should_queue = _upsert_and_try_queue(txn, doc_ref, test_id, object_path)

    if not should_queue:
        # A queued or running job already exists.
        # Time-bucket task name is secondary safety against function retries.
        return

    # ── Enqueue delayed task ──────────────────────────────────────────────────
    _enqueue_task(test_id, _task_name(test_id))
    log.info("Enqueued analysis task for test_id=%s (image: %s)", test_id, object_path)
