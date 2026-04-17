"""
cloud/analysis_service/main.py
-------------------------------
Cloud Run analysis service.

POST /analyze   body: { "test_id": "..." }
GET  /healthz   → 200  (no env vars required)

Processing flow:
  1. Parse and validate request.
  2. Acquire Firestore lock (atomic transaction) — exit cleanly if another
     job already holds it.
  3. List and download all images for the test from GCS.
  4. Run the spot_assay pipeline (always full reprocess — avoids incremental
     state bugs; acceptable at current scale).
  5. Upload results to stable GCS paths (safe overwrite every run).
  6. Atomically release the lock and update Firestore with the outcome.
  7. If new images arrived while this run was active, self-re-queue a
     catch-up task so those images don't get missed.

Storage layout (all under the same GCS bucket):
  PiCultureCam/{test_id}/{timestamp}.jpg           ← source images
  PiCultureCam/{test_id}/results/well_colors.csv   ← stable latest CSV
  PiCultureCam/{test_id}/results/latest_summary.png
  PiCultureCam/{test_id}/results/plate_grids/      ← per-image grids

Environment variables (read lazily at first POST request, not at import):
  GCS_BUCKET         — GCS bucket name (e.g. "myproject.appspot.com")
  GCP_PROJECT        — GCP project ID
  QUEUE_LOCATION     — Cloud Tasks queue region
  QUEUE_ID           — Cloud Tasks queue name
  CLOUD_RUN_URL      — this service's own /analyze URL (for self-requeue)
  CLOUD_RUN_SA_EMAIL — service account for OIDC auth
  WEIGHTS_PATH       — (optional) defaults to /app/weights/yolo_well_best.pt
  QUIET_PERIOD_SECS  — (optional, default 120) must match event_router value
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from flask import Flask, request, jsonify
from google.cloud import firestore, storage
from google.protobuf import timestamp_pb2

# ── Sys-path: tools/ is copied alongside this file in the container ───────────
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from tools.spot_assay import SpotAssayConfig, process_folder  # noqa: E402

# ── Constants (not env-dependent) ────────────────────────────────────────────
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
_SAFE_RE       = re.compile(r"[^a-zA-Z0-9_-]")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("analysis_service")

app = Flask(__name__)

# ── Lazy singletons ───────────────────────────────────────────────────────────
_db: firestore.Client | None = None
_gcs: storage.Client | None = None


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        cfg = get_config()
        _db = firestore.Client(database=cfg.firestore_db)
    return _db


def _get_gcs() -> storage.Client:
    global _gcs
    if _gcs is None:
        _gcs = storage.Client()
    return _gcs


# ── Runtime config (read lazily, cached after first call) ─────────────────────

@dataclass
class Config:
    gcs_bucket:         str
    gcp_project:        str
    queue_location:     str
    queue_id:           str
    cloud_run_url:      str
    cloud_run_sa_email: str
    weights_path:       Path
    quiet_period_secs:  int
    firestore_db:       str


_config: Config | None = None


def get_config() -> Config:
    """
    Read all required env vars and return a Config.

    Cached after the first call so os.environ is only hit once per instance.
    Raises KeyError (with a clear message) if a required var is missing —
    the caller catches this and returns a JSON 500 rather than crashing.
    """
    global _config
    if _config is not None:
        return _config

    required = [
        "GCS_BUCKET", "GCP_PROJECT", "QUEUE_LOCATION",
        "QUEUE_ID", "CLOUD_RUN_URL", "CLOUD_RUN_SA_EMAIL",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise KeyError(f"Missing required environment variables: {', '.join(missing)}")

    _config = Config(
        gcs_bucket         = os.environ["GCS_BUCKET"],
        gcp_project        = os.environ["GCP_PROJECT"],
        queue_location     = os.environ["QUEUE_LOCATION"],
        queue_id           = os.environ["QUEUE_ID"],
        cloud_run_url      = os.environ["CLOUD_RUN_URL"],
        cloud_run_sa_email = os.environ["CLOUD_RUN_SA_EMAIL"],
        weights_path       = Path(os.getenv("WEIGHTS_PATH", "/app/weights/yolo_well_best.pt")),
        quiet_period_secs  = int(os.getenv("QUIET_PERIOD_SECS", "120")),
        firestore_db       = os.getenv("FIRESTORE_DB", "plate-analysis"),
    )
    return _config


# ── Firestore lock: claim ─────────────────────────────────────────────────────

@firestore.transactional
def _claim_lock(
    transaction: firestore.Transaction,
    doc_ref: firestore.DocumentReference,
    job_id: str,
) -> bool:
    """
    Atomically claim the analysis lock for this test.

    Returns True if the lock was acquired.
    Returns False if another job is already running — caller should exit cleanly.
    """
    snap = doc_ref.get(transaction=transaction)
    data: dict = snap.to_dict() or {}

    if data.get("status") == "running":
        log.info("Lock held by job %s — exiting cleanly.", data.get("current_job_id"))
        return False

    fields = {
        "status":         "running",
        "current_job_id": job_id,
        "last_run_at":    firestore.SERVER_TIMESTAMP,
        "error_message":  None,
    }
    if snap.exists:
        transaction.update(doc_ref, fields)
    else:
        transaction.set(doc_ref, fields)
    return True


# ── Firestore lock: release ───────────────────────────────────────────────────

@firestore.transactional
def _release_lock(
    transaction: firestore.Transaction,
    doc_ref: firestore.DocumentReference,
    job_id: str,
    processed_count: int,
    outcome: dict,
) -> bool:
    """
    Atomically release the lock and write the run outcome.

    Compares image_count (current total) against processed_count (images
    seen at analysis start).  If new images arrived during this run,
    marks status as 'queued' instead of 'complete' and returns True to
    signal the caller to enqueue a catch-up task.

    Returns True if a catch-up task should be enqueued.
    """
    snap = doc_ref.get(transaction=transaction)
    data: dict = snap.to_dict() or {}

    if data.get("current_job_id") != job_id:
        log.warning("Lock ownership changed — skipping release for job %s.", job_id)
        return False

    current_total      = data.get("image_count", 0)
    new_images_arrived = current_total > processed_count

    fields = {
        **outcome,
        "current_job_id":             None,
        "last_processed_image_count": processed_count,
        "completed_at":               firestore.SERVER_TIMESTAMP,
    }

    if new_images_arrived:
        fields["status"]           = "queued"
        fields["last_enqueued_at"] = firestore.SERVER_TIMESTAMP
        log.info(
            "New images arrived during processing (%d → %d); will re-queue.",
            processed_count, current_total,
        )

    transaction.update(doc_ref, fields)
    return new_images_arrived


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _list_test_images(cfg: Config, test_id: str) -> list[storage.Blob]:
    """Return all source image blobs for a test, excluding results/ outputs."""
    client = _get_gcs()
    prefix = f"{test_id}/"
    blobs  = client.list_blobs(cfg.gcs_bucket, prefix=prefix)
    return [
        b for b in blobs
        if "/results/" not in b.name
        and Path(b.name).suffix.lower() in IMAGE_SUFFIXES
    ]


def _download_images(blobs: list[storage.Blob], dest: Path) -> None:
    """Download blobs into dest/, preserving only the basename."""
    dest.mkdir(parents=True, exist_ok=True)
    for blob in blobs:
        local = dest / Path(blob.name).name
        blob.download_to_filename(str(local))
        log.debug("Downloaded %s", blob.name)


def _upload_dir(cfg: Config, local_dir: Path, gcs_prefix: str) -> dict[str, str]:
    """
    Recursively upload everything in local_dir to gcs_prefix/.

    Returns a mapping of relative path → gs:// URI for every file uploaded.
    All uploads overwrite the existing object — results are always the latest run.
    """
    client  = _get_gcs()
    bucket  = client.bucket(cfg.gcs_bucket)
    out_map: dict[str, str] = {}

    for local_file in local_dir.rglob("*"):
        if not local_file.is_file():
            continue
        relative = local_file.relative_to(local_dir)
        gcs_path = f"{gcs_prefix}/{relative}"
        blob     = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_file))
        gcs_uri  = f"gs://{cfg.gcs_bucket}/{gcs_path}"
        out_map[str(relative)] = gcs_uri
        log.info("Uploaded  %s  →  %s", local_file.name, gcs_uri)

    return out_map


# ── Self-requeue helper ───────────────────────────────────────────────────────

def _enqueue_catchup(cfg: Config, test_id: str) -> None:
    """Enqueue a new analysis task for images that arrived during this run."""
    from google.cloud import tasks_v2

    tc      = tasks_v2.CloudTasksClient()
    parent  = tc.queue_path(cfg.gcp_project, cfg.queue_location, cfg.queue_id)
    safe_id = _SAFE_RE.sub("-", test_id)[:200]
    bucket  = math.floor(time.time() / cfg.quiet_period_secs)
    name    = f"{parent}/tasks/{safe_id}--{bucket}"

    ts = timestamp_pb2.Timestamp()
    ts.FromSeconds(int(time.time()) + 10)

    from urllib.parse import urlparse
    parsed   = urlparse(cfg.cloud_run_url)
    audience = f"{parsed.scheme}://{parsed.netloc}"   # base URL only, no /analyze path

    task = {
        "name":          name,
        "schedule_time": ts,
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url":         cfg.cloud_run_url,
            "headers":     {"Content-Type": "application/json"},
            "body":        json.dumps({"test_id": test_id}).encode(),
            "oidc_token":  {
                "service_account_email": cfg.cloud_run_sa_email,
                "audience":              audience,
            },
        },
    }

    try:
        tc.create_task(request={"parent": parent, "task": task})
        log.info("Catch-up task enqueued for %s.", test_id)
    except Exception as exc:
        if "ALREADY_EXISTS" in str(exc):
            log.info("Catch-up task already exists for %s (burst collapsed).", test_id)
        else:
            log.warning("Catch-up enqueue failed for %s: %s", test_id, exc)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def root():
    """Lightweight health check — no env vars, no GCP calls."""
    return jsonify({"status": "ok"}), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    """Alias of / — same response, separate route registration."""
    return jsonify({"status": "ok"}), 200


@app.route("/analyze", methods=["POST"])
def analyze():
    # ── Resolve config (fails fast with JSON error if env vars are missing) ───
    try:
        cfg = get_config()
    except KeyError as exc:
        log.error("Configuration error: %s", exc)
        return jsonify({"error": str(exc)}), 500

    body    = request.get_json(force=True) or {}
    test_id = str(body.get("test_id", "")).strip()

    if not test_id:
        return jsonify({"error": "missing test_id"}), 400

    job_id  = str(uuid.uuid4())
    db      = _get_db()
    doc_ref = db.collection("tests").document(test_id)

    # ── Step 1: Claim the per-test lock ───────────────────────────────────────
    if not _claim_lock(db.transaction(), doc_ref, job_id):
        return jsonify({"status": "skipped", "reason": "lock held by another job"}), 200

    log.info("[%s] job=%s lock acquired", test_id, job_id)

    tmp_dir = Path(tempfile.mkdtemp(prefix=f"spot_assay_{test_id}_"))
    try:
        # ── Step 2: List and download all source images ────────────────────────
        blobs = _list_test_images(cfg, test_id)
        if not blobs:
            log.warning("[%s] no images found in GCS", test_id)
            outcome = {"status": "failed", "error_message": "no images found in storage"}
            _release_lock(db.transaction(), doc_ref, job_id, 0, outcome)
            return jsonify({"status": "failed", "reason": "no images"}), 200

        images_dir  = tmp_dir / "images"
        results_dir = tmp_dir / "results"
        results_dir.mkdir()

        _download_images(blobs, images_dir)
        image_count = len(blobs)
        log.info("[%s] downloaded %d image(s)", test_id, image_count)

        # ── Step 3: Run spot_assay pipeline ───────────────────────────────────
        spot_cfg = SpotAssayConfig()   # defaults: H=MPC, G=GPC, F=NC
        process_folder(
            plates_dir=images_dir,
            weights_path=cfg.weights_path,
            out_dir=results_dir,
            cfg=spot_cfg,
            reprocess=True,
        )
        log.info("[%s] pipeline complete", test_id)

        # ── Step 4: Upload results (stable overwrite) ─────────────────────────
        gcs_results_prefix = f"{test_id}/results"
        uploaded = _upload_dir(cfg, results_dir, gcs_results_prefix)

        csv_uri     = uploaded.get("well_colors.csv")
        summary_uri = uploaded.get("latest_summary.png")

        # ── Step 5: Release lock + check for catch-up ─────────────────────────
        outcome = {
            "status":         "complete",
            "error_message":  None,
            "result_csv":     csv_uri,
            "result_summary": summary_uri,
        }
        needs_requeue = _release_lock(
            db.transaction(), doc_ref, job_id, image_count, outcome
        )

        if needs_requeue:
            _enqueue_catchup(cfg, test_id)

        return jsonify({
            "status":  "complete",
            "test_id": test_id,
            "images":  image_count,
            "csv":     csv_uri,
            "summary": summary_uri,
        }), 200

    except Exception as exc:
        log.exception("[%s] job=%s failed: %s", test_id, job_id, exc)
        outcome = {
            "status":        "failed",
            "error_message": str(exc),
            "result_csv":    None,
            "result_summary": None,
        }
        try:
            _release_lock(db.transaction(), doc_ref, job_id, 0, outcome)
        except Exception:
            log.exception("[%s] could not release lock after failure", test_id)
        return jsonify({"status": "error", "message": str(exc)}), 500

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
