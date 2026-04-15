"""
tools/hough_grid_annotate.py
----------------------------
Detect 96 well centers in plate images using:
  1. Grayscale + Hough circle detection (full image, 1/3 scale)
  2. Grid-consistency filter  (keep only circles with ≥2 pitch-spaced neighbours)
  3. 1-D KDE density peaks   → 12 column centers + 8 row centers
  4. 96-well grid from every row-center × column-center pair

Output
------
  <out>/hough_centers.json      — Label Studio import file (96 keypointlabels per image)
  <out>/hough_qc.csv            — per-image QC summary
  <out>/overlays/<stem>.png     — overlay: Hough detections (blue) + grid (green)

Usage
-----
cd /home/rami/plate_analysis

python -m tools.hough_grid_annotate \\
    --image-dir /home/rami/firebase-download \\
    --ls-folder firebase-download \\
    --out       /home/rami/hough_annotations \\
    --fallback  /home/rami/firebase-download/preannotations.json

# Pilot (one image):
python -m tools.hough_grid_annotate \\
    --image-dir /home/rami/firebase-download \\
    --ls-folder firebase-download \\
    --out       /home/rami/hough_annotations \\
    --pilot
"""

from __future__ import annotations

import argparse
import csv as csv_mod
import json
import uuid
from pathlib import Path

import cv2
import numpy as np
from scipy.signal import find_peaks
from scipy.spatial.distance import cdist

# ── Constants ─────────────────────────────────────────────────────────────────

ROW_LABELS = list("ABCDEFGH")
COL_LABELS = [str(c) for c in range(1, 13)]
ALL_WELLS  = [f"{r}{c}" for r in ROW_LABELS for c in COL_LABELS]

SCALE      = 3      # downsample factor for Hough
PARAM2     = 10     # Hough accumulator threshold
PARAM1     = 55     # upper Canny threshold for Hough
R_TOL      = 0.30   # ±30% radius tolerance
NB_PITCH_LO = 0.60  # neighbour pitch range [lo, hi] × estimated pitch
NB_PITCH_HI = 1.45
MIN_NB     = 2      # minimum number of pitch-consistent neighbours
KDE_BW_FRAC = 0.25  # KDE bandwidth as fraction of estimated pitch
MIN_CIRCLES = 20    # minimum detections to attempt grid fit
KDE_EXTRA   = 2     # extra peaks to request from KDE (then cleaned by robust fit)

_KP_WIDTH  = 0.3    # Label Studio keypoint display width


# ── Robust linear grid ────────────────────────────────────────────────────────

def robust_linear_grid(peaks: np.ndarray, n: int) -> tuple[np.ndarray, float]:
    """
    Given a 1-D array of KDE peaks (more than n), remove spurious close peaks,
    fit a uniform-pitch linear model  x = x0 + i*pitch  via least-squares, and
    return all n positions.

    Returns (positions_array_shape_n, pitch).
    """
    peaks = np.sort(peaks)
    if len(peaks) < 2:
        raise ValueError("Not enough peaks")

    spacings = np.diff(peaks)
    med_pitch = float(np.median(spacings))

    # Remove too-close spurious peaks (< 0.65 × median pitch)
    keep = np.ones(len(peaks), dtype=bool)
    for i in range(len(spacings)):
        if spacings[i] < med_pitch * 0.65:
            keep[i + 1] = False
    clean = peaks[keep]

    if len(clean) < 2:
        raise ValueError("Too few peaks after cleaning")

    # Re-estimate pitch from cleaned set
    clean_sp  = np.diff(clean)
    med_pitch = float(np.median(clean_sp))

    # Assign integer grid indices to cleaned peaks
    rel = clean - clean[0]
    idx = np.round(rel / med_pitch).astype(int)

    # Least-squares fit: x = x0 + idx * pitch
    A   = np.column_stack([np.ones(len(idx)), idx])
    sol, _, _, _ = np.linalg.lstsq(A, clean, rcond=None)
    x0, pitch = float(sol[0]), abs(float(sol[1]))

    return x0 + np.arange(n) * pitch, pitch


# ── Plate bounding-box (white interior) ───────────────────────────────────────

def detect_plate_bbox(
    img_bgr: np.ndarray,
    pad: int = 15,
) -> tuple[int, int, int, int] | None:
    """
    Find the bright white plate interior via threshold + morphology.
    Returns (x, y, w, h) with a small pad, or None if not found.
    """
    ih, iw = img_bgr.shape[:2]
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 160, 255, cv2.THRESH_BINARY)
    k = cv2.getStructuringElement(cv2.MORPH_RECT, (60, 60))
    opened = cv2.morphologyEx(
        cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, k), cv2.MORPH_OPEN, k)
    contours, _ = cv2.findContours(
        opened, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None
    bx, by, bw, bh = cv2.boundingRect(max(contours, key=cv2.contourArea))
    # must cover at least 10 % of the frame to qualify as the plate
    if bw * bh < iw * ih * 0.10:
        return None
    return (max(0, bx - pad), max(0, by - pad),
            min(iw, bx + bw + pad) - max(0, bx - pad),
            min(ih, by + bh + pad) - max(0, by - pad))


# ── Core detection ────────────────────────────────────────────────────────────

def detect_well_grid(
    img_bgr: np.ndarray,
    param2: int = PARAM2,
    min_col_peaks: int = 12,
    min_row_peaks: int = 8,
    width_frac: float = 0.65,
) -> dict | None:
    """
    Detect 96 well centers and estimated radius.

    Returns None if detection fails (too few circles, or can't find 12×8 peaks).

    Returns dict:
        centers   : {well_label: (cx_px, cy_px)}  original image coords
        col_centers: np.ndarray shape (12,)
        row_centers: np.ndarray shape (8,)
        well_r    : float  median well radius (px)
        col_std   : float  column-spacing std (px)
        row_std   : float  row-spacing std (px)
        n_detected: int    circles surviving filters
        qc_score  : float  [0,1]  regularity score
    """
    ih, iw = img_bgr.shape[:2]
    small   = cv2.resize(img_bgr, (iw // SCALE, ih // SCALE))
    gray_s  = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray_s, (5, 5), 1.5)

    # Estimate pitch and radius from image width
    # (12 wells span ~width_frac of image width; pitch = span / 11)
    pitch_s = int((iw * width_frac / 11) / SCALE)
    est_r_s = max(8, int(pitch_s * 3.175 / 9.0))

    circles = cv2.HoughCircles(
        blurred, cv2.HOUGH_GRADIENT, dp=1,
        minDist=int(pitch_s * 0.78),
        param1=PARAM1, param2=param2,
        minRadius=int(est_r_s * 0.65),
        maxRadius=int(est_r_s * 1.45),
    )
    if circles is None:
        return None

    pts    = circles[0].copy()
    med_r  = float(np.median(pts[:, 2]))
    pts    = pts[np.abs(pts[:, 2] - med_r) / med_r < R_TOL]

    if len(pts) < MIN_CIRCLES:
        return None

    # Grid-consistency filter: keep circles with ≥MIN_NB pitch-spaced neighbours
    pos    = pts[:, :2]
    D      = cdist(pos, pos)
    np.fill_diagonal(D, np.inf)
    nn_d   = D.min(axis=1)
    pitch_est = float(np.median(nn_d[nn_d < np.percentile(nn_d, 80)]))
    plo, phi  = pitch_est * NB_PITCH_LO, pitch_est * NB_PITCH_HI
    n_nb  = np.array([int(np.sum((D[i] >= plo) & (D[i] <= phi)))
                      for i in range(len(pts))])
    pts   = pts[n_nb >= MIN_NB]

    if len(pts) < MIN_CIRCLES:
        return None

    # 1-D KDE density peaks → column and row centers
    # Request up to (n + KDE_EXTRA) peaks; accept as few as n (min required).
    def peaks1d(values: np.ndarray, n_min: int, n_want: int, bw: float) -> np.ndarray | None:
        lo, hi = values.min() - bw, values.max() + bw
        xs     = np.linspace(lo, hi, 3000)
        kde    = np.zeros(len(xs))
        for v in values:
            kde += np.exp(-0.5 * ((xs - v) / bw) ** 2)
        min_sep = max(1, int(len(xs) * bw * 1.2 / (hi - lo)))
        pks, pr = find_peaks(kde, distance=min_sep, height=0)
        if len(pks) < n_min:
            return None
        take  = min(n_want, len(pks))
        top_n = pks[np.argsort(pr["peak_heights"])[::-1][:take]]
        return np.sort(xs[top_n])

    bw         = pitch_est * KDE_BW_FRAC
    # Request extra peaks so robust_linear_grid can discard spurious ones
    col_c_s    = peaks1d(pts[:, 0], min_col_peaks, 12 + KDE_EXTRA, bw)
    row_c_s    = peaks1d(pts[:, 1], min_row_peaks,  8 + KDE_EXTRA, bw)

    if col_c_s is None or row_c_s is None:
        return None

    # Enforce uniform-pitch linear grid (removes spurious KDE peaks)
    try:
        col_c_s, col_pitch = robust_linear_grid(col_c_s, 12)
        row_c_s, row_pitch = robust_linear_grid(row_c_s,  8)
    except ValueError:
        return None

    # Scale back to original resolution and clamp to image bounds
    col_centers = np.clip(col_c_s * SCALE, 0, iw - 1)
    row_centers = np.clip(row_c_s * SCALE, 0, ih - 1)
    well_r      = med_r   * SCALE


    col_spacings = np.diff(col_centers)
    row_spacings = np.diff(row_centers)
    col_std      = float(col_spacings.std())
    row_std      = float(row_spacings.std())

    # Build well-center dict
    centers: dict[str, tuple[float, float]] = {}
    for ri, rl in enumerate(ROW_LABELS):
        for ci, cl in enumerate(COL_LABELS):
            centers[f"{rl}{cl}"] = (float(col_centers[ci]), float(row_centers[ri]))

    # Real QC score: fraction of grid centers that have a detected Hough circle
    # within MATCH_TOL × well_pitch pixels (circles are in downscaled coords → scale up)
    MATCH_TOL   = 0.40          # accept circle within 40% of pitch
    col_pitch_px = float(np.median(col_spacings)) if len(col_spacings) else well_r * 2
    row_pitch_px = float(np.median(row_spacings)) if len(row_spacings) else well_r * 2
    avg_pitch    = (col_pitch_px + row_pitch_px) / 2.0
    match_dist   = MATCH_TOL * avg_pitch

    detected_xy  = pts[:, :2] * SCALE          # upscale to full-res
    grid_xy      = np.array(list(centers.values()), dtype=np.float32)
    D_match      = cdist(grid_xy, detected_xy)  # (96, n_detected)
    n_matched    = int(np.sum(D_match.min(axis=1) <= match_dist))
    qc           = round(n_matched / 96.0, 4)

    return {
        "centers":     centers,
        "col_centers": col_centers,
        "row_centers": row_centers,
        "well_r":      well_r,
        "col_std":     col_std,
        "row_std":     row_std,
        "n_detected":  len(pts),
        "n_matched":   n_matched,
        "match_dist":  round(match_dist, 1),
        "qc_score":    qc,
    }


# ── Label Studio task builder ─────────────────────────────────────────────────

def build_ls_task(
    image_url:  str,
    centers:    dict[str, tuple[float, float]],
    img_w:      int,
    img_h:      int,
    qc_score:   float,
    task_id:    int = 0,
    model_version: str = "hough_grid_v1",
) -> dict:
    result = []
    for well in ALL_WELLS:
        if well not in centers:
            continue
        cx, cy = centers[well]
        result.append({
            "id":             uuid.uuid4().hex[:8],
            "from_name":      "wells",
            "to_name":        "image",
            "type":           "keypointlabels",
            "image_rotation": 0,
            "original_width":  img_w,
            "original_height": img_h,
            "value": {
                "x":              round(cx / img_w * 100.0, 6),
                "y":              round(cy / img_h * 100.0, 6),
                "width":          _KP_WIDTH,
                "keypointlabels": [well],
            },
        })
    return {
        "id": task_id,
        "data": {"image": image_url},
        "predictions": [{
            "model_version": model_version,
            "score":         round(qc_score, 4),
            "result":        result,
        }],
    }


# ── Overlay drawing ───────────────────────────────────────────────────────────

def draw_overlay(
    img_bgr:    np.ndarray,
    detection:  dict,
) -> np.ndarray:
    ov      = img_bgr.copy()
    centers = detection["centers"]
    well_r  = detection["well_r"]
    r_dot   = max(5, int(well_r * 0.13))

    # Grid lines
    ROWS = len(ROW_LABELS)
    COLS = len(COL_LABELS)
    grid = np.array(
        [centers[f"{rl}{cl}"] for rl in ROW_LABELS for cl in COL_LABELS],
        dtype=np.float32,
    ).reshape(ROWS, COLS, 2)
    for ri in range(ROWS):
        pts = grid[ri].astype(np.int32).reshape(-1, 1, 2)
        cv2.polylines(ov, [pts], False, (50, 50, 50), 1, cv2.LINE_AA)
    for ci in range(COLS):
        pts = grid[:, ci].astype(np.int32).reshape(-1, 1, 2)
        cv2.polylines(ov, [pts], False, (50, 50, 50), 1, cv2.LINE_AA)

    # Grid circles (green)
    for well in ALL_WELLS:
        cx, cy = centers[well]
        cv2.circle(ov, (int(cx), int(cy)), int(well_r), (0, 210,   0), 2, cv2.LINE_AA)
        cv2.circle(ov, (int(cx), int(cy)), r_dot,       (0, 230,   0), -1, cv2.LINE_AA)

    # Corner labels
    for well, off in [("A1", (-90, -25)), ("A12", (12, -25)),
                      ("H1", (-90,  35)), ("H12", (12,  35))]:
        cx, cy = centers[well]
        cv2.putText(ov, well, (int(cx) + off[0], int(cy) + off[1]),
                    cv2.FONT_HERSHEY_SIMPLEX, 1.4, (0, 255, 255), 2)

    # QC badge
    qc = detection["qc_score"]
    col  = (0, 200, 0) if qc >= 0.85 else (0, 200, 200) if qc >= 0.70 else (0, 80, 255)
    cv2.rectangle(ov, (8, 6), (420, 90), (20, 20, 20), -1)
    cv2.putText(ov, f"QC {qc:.2f}  detected={detection['n_detected']}",
                (14, 42), cv2.FONT_HERSHEY_SIMPLEX, 0.88, col, 2)
    cv2.putText(ov,
                f"col_std={detection['col_std']:.1f}px  "
                f"row_std={detection['row_std']:.1f}px",
                (14, 76), cv2.FONT_HERSHEY_SIMPLEX, 0.68, (180, 180, 180), 1)
    return ov


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args):
    image_dir  = Path(args.image_dir)
    out_dir    = Path(args.out)
    overlay_dir = out_dir / "overlays"
    out_dir.mkdir(parents=True, exist_ok=True)
    overlay_dir.mkdir(parents=True, exist_ok=True)

    exts   = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
    images = sorted(p for p in image_dir.iterdir() if p.suffix.lower() in exts)
    if not images:
        print(f"No images found in {image_dir}"); return

    # Load fallback preannotations (model-based) if provided
    fallback: dict[str, dict] = {}
    if args.fallback:
        with open(args.fallback) as f:
            fb_tasks = json.load(f)
        for t in fb_tasks:
            fname = t["data"]["image"].split("/")[-1]
            fallback[fname] = t

    ls_folder = args.ls_folder or image_dir.name.strip()

    if args.pilot:
        images = images[:1]
        print(f"[PILOT] {images[0].name}")

    print(f"Processing {len(images)} images …\n")

    tasks, qc_rows = [], []
    n_ok = n_fail = 0
    task_id = 1

    for img_path in images:
        img = cv2.imread(str(img_path))
        if img is None:
            print(f"  [WARN] Cannot read {img_path.name}"); continue

        ih, iw = img.shape[:2]
        det    = detect_well_grid(img)
        image_url = f"/data/local-files/?d={ls_folder}/{img_path.name}"

        if det is not None:
            centers  = det["centers"]
            qc       = det["qc_score"]
            source   = "hough"
            n_ok    += 1
            status   = "OK" if qc >= 0.85 else "WARN" if qc >= 0.70 else "LOW"
            print(f"  {img_path.name}  QC={qc:.2f}  "
                  f"matched={det['n_matched']}/96  det={det['n_detected']}  "
                  f"tol={det['match_dist']:.0f}px"
                  f"  [{status}]")
            ov = draw_overlay(img, det)
        else:
            # Fallback to model preannotations if available
            if img_path.name in fallback:
                fb_task = fallback[img_path.name]
                pred    = fb_task["predictions"][0]
                ow      = pred["result"][0]["original_width"]
                oh      = pred["result"][0]["original_height"]
                centers = {}
                for r in pred["result"]:
                    well = r["value"]["keypointlabels"][0]
                    centers[well] = (r["value"]["x"]/100.0*ow,
                                     r["value"]["y"]/100.0*oh)
                qc      = pred["score"]
                source  = "fallback"
                n_fail += 1
                print(f"  {img_path.name}  [FALLBACK] Hough failed — "
                      f"using model preannotation (score={qc:.2f})")
            else:
                n_fail += 1
                print(f"  {img_path.name}  [FAILED] Hough failed, no fallback")
                continue
            ov = img.copy()
            cv2.rectangle(ov, (8,6),(320,46),(20,20,20),-1)
            cv2.putText(ov, "FALLBACK (Hough failed)", (14,36),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0,80,255), 2)

        mv = "hough_grid_v1" if source == "hough" else "hybrid_net_v1"
        tasks.append(build_ls_task(image_url, centers, iw, ih, qc,
                                   task_id=task_id, model_version=mv))
        task_id += 1
        cv2.imwrite(str(overlay_dir / f"{img_path.stem}_hough.png"), ov)

        row = {
            "image":        img_path.name,
            "source":       source,
            "qc_score":     round(qc, 4),
            "n_matched":    det["n_matched"]   if det else 0,
            "n_detected":   det["n_detected"]  if det else 0,
            "match_dist_px": det["match_dist"] if det else -1,
            "col_std_px":   round(det["col_std"], 2) if det else -1,
            "row_std_px":   round(det["row_std"], 2) if det else -1,
            "well_r_px":    round(det["well_r"],  1) if det else -1,
        }
        qc_rows.append(row)

    # Write JSON
    json_path = out_dir / "hough_centers.json"
    with open(json_path, "w") as f:
        json.dump(tasks, f, indent=2)

    # Write QC CSV
    qc_path = out_dir / "hough_qc.csv"
    if qc_rows:
        with open(qc_path, "w", newline="") as f:
            w = csv_mod.DictWriter(f, fieldnames=list(qc_rows[0].keys()))
            w.writeheader(); w.writerows(qc_rows)

    print(f"\nDone — {len(tasks)} tasks  (ok={n_ok}  fallback/fail={n_fail})")
    print(f"  JSON    : {json_path}")
    print(f"  QC CSV  : {qc_path}")
    print(f"  Overlays: {overlay_dir}/")

    if qc_rows:
        import statistics
        scores = [r["qc_score"] for r in qc_rows if r["source"] == "hough"]
        if scores:
            print(f"\nHough QC  mean={statistics.mean(scores):.3f}  "
                  f"min={min(scores):.3f}  max={max(scores):.3f}")
            n_warn = sum(1 for s in scores if s < 0.85)
            if n_warn:
                print(f"  {n_warn}/{len(scores)} images scored < 0.85 — "
                      "check overlays")


def main():
    p = argparse.ArgumentParser(
        description="96-well plate center detection via Hough + KDE grid")
    p.add_argument("--image-dir", required=True, dest="image_dir")
    p.add_argument("--ls-folder", default=None, dest="ls_folder")
    p.add_argument("--out",       required=True)
    p.add_argument("--fallback",  default=None,
                   help="Preannotations JSON to use when Hough fails")
    p.add_argument("--pilot", action="store_true",
                   help="Run on first image only")
    args = p.parse_args()
    run(args)


if __name__ == "__main__":
    main()
