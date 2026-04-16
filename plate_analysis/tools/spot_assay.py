"""
tools/spot_assay.py
--------------------
Spot-assay application logic built on top of the YOLO colour pipeline.

Pipeline (per image)
--------------------
  1. YOLO well detection + colour sampling  (reuses yolo_color_pipeline)
  2. Filled-well classification             (HSV brightness + saturation gate)
  3. CIE Lab Delta-E 76 vs PC and NC        (Lab values already returned by sampler)
  4. Delta-E category + positivity call     (all thresholds configurable)
  5. CSV append                             (one row per well per image, idempotent)
  6. Plate-grid PNG per image
  7. latest_summary.png always updated to last processed image

Assumptions
-----------
  * Images are JPEG or PNG files in the plates directory.
  * Filename sort order = chronological order.  If the filename contains a
    token matching  \\d+min  (e.g. Oat_milk_LOD_30min.jpg), that number is
    extracted as the timepoint in minutes; otherwise the loop index is used.
  * Filled-well classification uses two independent gates (both configurable):
      Gate 1 — HSV: V > empty_v_thresh AND S < empty_s_thresh
               catches bright, near-white empty wells (transparent liquid)
      Gate 2 — Lab b*: b* < min_b_lab_filled
               catches plastic-background wells that have a cool/blue tint
               (negative b*) even when they are not exceptionally bright.
               Filled wells containing matrix or sample always have warm/
               yellow colour (positive b*); empty wells containing only
               plastic show a cool, slightly blue background.
    A well is EMPTY if it fails EITHER gate.
  * Lab values from the sampler are CIE-scale:
      L  0–100,  a  ±128,  b  ±128  (standard CIELAB).
    Delta-E 76 = sqrt((ΔL)² + (Δa)² + (Δb)²)  — no further conversion needed.
  * PC / NC means are computed from filled wells in those rows only.
    If a control row has zero filled wells the image is flagged FAILED.
  * Experiment is FAILED if ΔE(PC_mean, NC_mean) < pc_nc_min_deltaE.
    Default 5.0 — a "small difference", i.e. the controls are not distinct
    enough to call test wells against.
  * Positivity default rule:
      filled, non-control well, AND
      deltaE_to_PC < deltaE_to_NC (closer to PC in Lab space), AND
      deltaE_to_NC >= min_meaningful_deltaE (not noise)
    Both halves are separately configurable.

Usage
-----
cd /home/rami/plate_analysis

python -m tools.spot_assay \\
    --plates  /home/rami/plates \\
    --weights /home/rami/yolo_well_dataset/runs/detect/well/weights/best.pt \\
    --out     /home/rami/spot_assay_results

# Re-running is safe; already-processed images are skipped.
# Add --reprocess to force re-analysis of all images.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from tools.hough_grid_annotate import ROW_LABELS, COL_LABELS, ALL_WELLS
from tools.yolo_color_pipeline import (
    yolo_detect_and_assign,
    sample_well_color_from_bbox,
)

# ── CSV column order ──────────────────────────────────────────────────────────
CSV_COLUMNS = [
    "image_name",
    "timepoint_min",
    "well_id",
    "row",
    "column",
    "is_filled",
    "R", "G", "B",
    "L", "a", "b",
    "deltaE_to_NC",
    "deltaE_to_PC",
    "deltaE_category",
    "call",
    "experiment_status",
]

# ── Delta-E category labels ───────────────────────────────────────────────────
def deltaE_category(dE: Optional[float]) -> str:
    """Categorise a Delta-E 76 value into a human-readable tier.

    Tiers (CIEDE76 perceptual thresholds):
      < 2.3   → barely_noticeable
      2.3–5   → small
      5–12    → noticeable
      ≥ 12    → major
    """
    if dE is None or math.isnan(dE):
        return "n/a"
    if dE < 2.3:
        return "barely_noticeable"
    if dE < 5.0:
        return "small"
    if dE < 12.0:
        return "noticeable"
    return "major"


# ── Configuration ─────────────────────────────────────────────────────────────
@dataclass
class SpotAssayConfig:
    """All tunable parameters in one place.

    Change these here (or pass overrides from the CLI) to reconfigure
    the assay without touching any other logic.
    """

    # ── Control row assignments ───────────────────────────────────────────
    media_pc_row:  str = "H"   # Media positive control (reference, not used for scoring)
    matrix_pc_row: str = "G"   # Matrix positive control (PC reference for Delta-E scoring)
    matrix_nc_row: str = "F"   # Matrix negative control (NC reference for Delta-E scoring)

    # ── Filled-well detection ─────────────────────────────────────────────
    # Gate 1 — HSV (catches very bright, near-transparent empty wells):
    #   EMPTY if mean_V > empty_v_thresh AND mean_S < empty_s_thresh
    empty_v_thresh: int = 210
    empty_s_thresh: int = 25

    # Gate 2 — Lab b* (catches plastic-background wells with cool/blue tint):
    #   EMPTY if b* < min_b_lab_filled
    #   Rationale: filled wells containing assay matrix are warm/yellowish
    #   (positive b*); empty well plastic is cool/bluish (near-zero or
    #   negative b*).  On this dataset the gap between the lowest known-filled
    #   well (b*≈3.7) and the highest known-empty well (b*≈2.5) is clear.
    #   Default 3.0 sits in the middle of that gap.
    #   Tune this if the matrix colour changes (e.g. different assay).
    min_b_lab_filled: float = 3.0

    # ── NC reference sanity check ─────────────────────────────────────────
    # After building the NC reference mean, the filled NC wells are compared
    # against it.  Their mean ΔE should be close to 0 (within-group spread).
    # If it exceeds this threshold, the reference is likely contaminated or
    # the NC wells are unexpectedly heterogeneous — a warning is printed.
    nc_ref_max_internal_dE: float = 3.0

    # ── Positivity decision ───────────────────────────────────────────────
    # Rule part 1: well must have shifted away from NC by at least this much.
    #   Default = 2.3  (just-noticeable-difference boundary).
    min_meaningful_deltaE: float = 2.3

    # Rule part 2: well must be closer to PC than to NC.
    #   Set to False to disable this half (call only on min_meaningful_deltaE).
    require_closer_to_pc: bool = True

    # ── YOLO inference parameters ─────────────────────────────────────────
    conf: float = 0.20
    imgsz: int = 1280
    radius_frac: float = 0.50   # inner disk fraction for colour sampling


# ── Core maths ───────────────────────────────────────────────────────────────

def deltaE76(lab1: tuple[float, float, float],
             lab2: tuple[float, float, float]) -> float:
    """CIE Delta-E 76.  Inputs must be in CIE Lab scale (L 0–100, a/b ±128)."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2)))


def _lab_from_row(row: dict) -> tuple[float, float, float]:
    return (float(row["L"]), float(row["a"]), float(row["b"]))


def _mean_lab(rows: list[dict]) -> tuple[float, float, float]:
    """Mean Lab over a list of well-row dicts."""
    L = sum(r["L"] for r in rows) / len(rows)
    a = sum(r["a"] for r in rows) / len(rows)
    b = sum(r["b"] for r in rows) / len(rows)
    return (L, a, b)


# ── Filled-well detection ─────────────────────────────────────────────────────

def classify_filled(color_data: dict, cfg: SpotAssayConfig) -> bool:
    """Return True if the well appears to contain liquid (is 'filled').

    A well is EMPTY (returns False) if it fails EITHER of two independent gates:

    Gate 1 — HSV brightness + saturation:
        V > empty_v_thresh AND S < empty_s_thresh
        Catches very bright, near-transparent wells.

    Gate 2 — Lab b* (yellow-blue axis):
        b* < min_b_lab_filled
        Catches plastic-background wells with a cool/blue tint that are not
        captured by Gate 1 because their brightness is below the V threshold.
        Filled wells containing assay matrix are always warm/yellowish (b* > 0);
        empty wells showing only well plastic are cool/neutral (b* near 0 or
        negative).  The default threshold (3.0) sits in the gap between the
        lowest filled well observed (~3.7) and the highest empty well (~2.5).
    """
    n = color_data.get("n_pixels", 0)
    if n == 0:
        return False

    # Gate 1: very bright + low saturation → background (transparent/white)
    V = color_data.get("mean_V", 0.0)
    S = color_data.get("mean_S", 255.0)
    if V > cfg.empty_v_thresh and S < cfg.empty_s_thresh:
        return False

    # Gate 2: cool/blue tint → empty plastic well
    b_lab = color_data.get("mean_b_lab", float("nan"))
    if not math.isnan(b_lab) and b_lab < cfg.min_b_lab_filled:
        return False

    return True


# ── Timepoint extraction ──────────────────────────────────────────────────────

_TS_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})")
_TS_FORMAT  = "%Y-%m-%d_%H-%M-%S"


def _parse_ts(stem: str):
    """Return a datetime parsed from a YYYY-MM-DD_HH-MM-SS token, or None."""
    from datetime import datetime
    m = _TS_PATTERN.search(stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), _TS_FORMAT)
    except ValueError:
        return None


def infer_timepoint(stem: str, index: int, ref_stem: str | None = None) -> int:
    """Extract a timepoint in minutes from the filename stem.

    Priority:
    1. Explicit  <digits>min  token  (e.g. Oat_milk_LOD_30min.jpg → 30)
    2. Timestamp token YYYY-MM-DD_HH-MM-SS with a reference stem provided
       → elapsed minutes from the reference image (first image in the folder)
    3. Fall back to the loop index.
    """
    m = re.search(r"(\d+)min", stem, re.IGNORECASE)
    if m:
        return int(m.group(1))

    curr_dt = _parse_ts(stem)
    if curr_dt is not None and ref_stem is not None:
        ref_dt = _parse_ts(ref_stem)
        if ref_dt is not None:
            return int((curr_dt - ref_dt).total_seconds() / 60)

    return index


# ── Single-image processing ───────────────────────────────────────────────────

def process_image(
    img_path: Path,
    model,
    cfg: SpotAssayConfig,
    timepoint: int,
) -> tuple[str, list[dict]]:
    """Process one plate image.

    Returns
    -------
    (experiment_status, well_rows)
        experiment_status : "OK" | "FAILED:<reason>"
        well_rows         : list of dicts, one per well, matching CSV_COLUMNS
    """
    img = cv2.imread(str(img_path))
    if img is None:
        return "FAILED:cannot_read_image", []

    lab_img = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    hsv_img = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    assigned = yolo_detect_and_assign(img, model, conf=cfg.conf, imgsz=cfg.imgsz)
    if assigned is None:
        return "FAILED:yolo_detection_failed", []

    # ── Sample colour for every well ─────────────────────────────────────────
    color_data: dict[str, dict] = {}
    for well, det in assigned.items():
        color_data[well] = sample_well_color_from_bbox(
            img, lab_img, hsv_img, det, radius_frac=cfg.radius_frac
        )

    # ── Filled classification ─────────────────────────────────────────────────
    filled: dict[str, bool] = {w: classify_filled(color_data[w], cfg)
                               for w in ALL_WELLS if w in color_data}

    # ── Build base well rows (Lab values, filled flag) ────────────────────────
    well_rows: dict[str, dict] = {}
    for well in ALL_WELLS:
        if well not in color_data:
            continue
        cd = color_data[well]
        row_label = well[0]
        col_label = well[1:]
        well_rows[well] = {
            "image_name":     img_path.name,
            "timepoint_min":  timepoint,
            "well_id":        well,
            "row":            row_label,
            "column":         col_label,
            "is_filled":      filled.get(well, False),
            "R":              round(cd.get("mean_R", float("nan")), 2),
            "G":              round(cd.get("mean_G", float("nan")), 2),
            "B":              round(cd.get("mean_B", float("nan")), 2),
            "L":              round(cd.get("mean_L", float("nan")), 2),
            "a":              round(cd.get("mean_a", float("nan")), 2),
            "b":              round(cd.get("mean_b_lab", float("nan")), 2),
            "deltaE_to_NC":   float("nan"),
            "deltaE_to_PC":   float("nan"),
            "deltaE_category": "n/a",
            "call":           "ignored",
            "experiment_status": "",   # filled in below
        }

    # ── Compute control means from filled wells in control rows ───────────────
    _ctrl_rows = (cfg.media_pc_row, cfg.matrix_pc_row, cfg.matrix_nc_row)

    pc_filled = [well_rows[f"{cfg.matrix_pc_row}{c}"]
                 for c in COL_LABELS
                 if f"{cfg.matrix_pc_row}{c}" in well_rows
                 and well_rows[f"{cfg.matrix_pc_row}{c}"]["is_filled"]]

    nc_filled = [well_rows[f"{cfg.matrix_nc_row}{c}"]
                 for c in COL_LABELS
                 if f"{cfg.matrix_nc_row}{c}" in well_rows
                 and well_rows[f"{cfg.matrix_nc_row}{c}"]["is_filled"]]

    if not pc_filled:
        status = "FAILED:no_filled_PC_wells"
        for r in well_rows.values():
            r["experiment_status"] = status
            r["call"] = "control" if r["row"] in _ctrl_rows else r["call"]
        return status, list(well_rows.values())

    if not nc_filled:
        status = "FAILED:no_filled_NC_wells"
        for r in well_rows.values():
            r["experiment_status"] = status
            r["call"] = "control" if r["row"] in _ctrl_rows else r["call"]
        return status, list(well_rows.values())

    pc_mean_lab = _mean_lab(pc_filled)
    nc_mean_lab = _mean_lab(nc_filled)
    status = "OK"

    # ── NC reference sanity check ─────────────────────────────────────────────
    # The filled NC wells, compared against their own mean, should have a small
    # internal ΔE (within-group spread).  A high value indicates the reference
    # was built from heterogeneous wells — likely contaminated by empty wells
    # that slipped through the filled-detection gate.
    nc_internal_dEs = [deltaE76(_lab_from_row(r), nc_mean_lab) for r in nc_filled]
    nc_internal_mean_dE = sum(nc_internal_dEs) / len(nc_internal_dEs)
    if nc_internal_mean_dE > cfg.nc_ref_max_internal_dE:
        print(f"  [WARN] NC reference sanity FAILED: mean ΔE of NC wells vs "
              f"NC mean = {nc_internal_mean_dE:.2f} "
              f"(threshold {cfg.nc_ref_max_internal_dE}).  "
              f"NC reference may be contaminated by empty wells — "
              f"check min_b_lab_filled / empty thresholds.")
    else:
        print(f"  [OK]   NC reference sanity passed: mean internal ΔE = "
              f"{nc_internal_mean_dE:.2f} "
              f"(n={len(nc_filled)} filled NC wells)")

    # ── Per-well Delta-E + call ───────────────────────────────────────────────
    for well, row in well_rows.items():
        row["experiment_status"] = status
        row_label = row["row"]

        if row_label in _ctrl_rows:
            row["call"] = "control"
            # Still compute ΔE for reference
            if row["is_filled"]:
                lab = _lab_from_row(row)
                row["deltaE_to_NC"] = round(deltaE76(lab, nc_mean_lab), 3)
                row["deltaE_to_PC"] = round(deltaE76(lab, pc_mean_lab), 3)
                row["deltaE_category"] = deltaE_category(row["deltaE_to_NC"])
            continue

        if not row["is_filled"]:
            row["call"] = "ignored"
            continue

        lab = _lab_from_row(row)
        dE_nc = round(deltaE76(lab, nc_mean_lab), 3)
        dE_pc = round(deltaE76(lab, pc_mean_lab), 3)
        row["deltaE_to_NC"] = dE_nc
        row["deltaE_to_PC"] = dE_pc
        row["deltaE_category"] = deltaE_category(dE_nc)

        # Positivity rule (both parts independently configurable):
        #   Part 1: shifted away from NC by at least min_meaningful_deltaE
        #   Part 2: (if require_closer_to_pc) closer to PC than to NC
        part1 = dE_nc >= cfg.min_meaningful_deltaE
        part2 = (dE_pc < dE_nc) if cfg.require_closer_to_pc else True
        row["call"] = "positive" if (part1 and part2) else "negative"

    return status, list(well_rows.values())


# ── Plate-grid visualisation ──────────────────────────────────────────────────

# Visual constants — adjust these for display density
_CELL      = 72     # px per well cell
_MARGIN_L  = 46     # left margin (row labels)
_MARGIN_T  = 80     # top margin (column labels + title)
_MARGIN_B  = 30
_MARGIN_R  = 20
_BG        = (20, 20, 20)      # dark background BGR
_EMPTY_CLR = (55, 55, 55)      # unfilled well BGR
_MEDIA_PC_BORDER  = (80, 255, 80)    # green  — media positive control (H)
_MATRIX_PC_BORDER = (0, 230, 230)    # cyan   — matrix positive control (G)
_MATRIX_NC_BORDER = (30, 200, 255)   # amber  — matrix negative control (F)
_DEFAULT_BORDER = (120, 120, 120)
_POSITIVE_MARK_CLR = (0, 255, 80)   # bright green "+"
_FONT      = cv2.FONT_HERSHEY_SIMPLEX


def draw_plate_grid(
    well_rows: list[dict],
    cfg: SpotAssayConfig,
    title: str,
) -> np.ndarray:
    """Render an 8×12 plate-grid summary image.

    Layout
    ------
    - Title + timepoint at the top.
    - Row labels (A–H) on the left; column labels (1–12) along the top.
    - Each cell filled with the well's actual measured colour.
    - Empty wells shown in dark grey with a "·" marker.
    - Positive wells have a bright-green "+" overlay.
    - Control rows (PC / NC) have a coloured border and label.
    - If experiment_status != "OK", a red overlay + "FAILED" banner is drawn.
    """
    n_rows = len(ROW_LABELS)   # 8
    n_cols = len(COL_LABELS)   # 12

    img_w = _MARGIN_L + n_cols * _CELL + _MARGIN_R
    img_h = _MARGIN_T + n_rows * _CELL + _MARGIN_B
    canvas = np.full((img_h, img_w, 3), _BG, dtype=np.uint8)

    # ── Index well_rows by well_id ────────────────────────────────────────────
    by_well: dict[str, dict] = {r["well_id"]: r for r in well_rows}

    # ── Column labels ─────────────────────────────────────────────────────────
    for ci, cl in enumerate(COL_LABELS):
        x = _MARGIN_L + ci * _CELL + _CELL // 2 - 8
        cv2.putText(canvas, cl, (x, _MARGIN_T - 10),
                    _FONT, 0.55, (180, 180, 180), 1, cv2.LINE_AA)

    # ── Row labels + cells ────────────────────────────────────────────────────
    for ri, rl in enumerate(ROW_LABELS):
        y_top = _MARGIN_T + ri * _CELL

        # Row label
        cv2.putText(canvas, rl, (8, y_top + _CELL // 2 + 8),
                    _FONT, 0.65, (180, 180, 180), 1, cv2.LINE_AA)

        is_media_pc  = (rl == cfg.media_pc_row)
        is_matrix_pc = (rl == cfg.matrix_pc_row)
        is_matrix_nc = (rl == cfg.matrix_nc_row)
        is_ctrl = is_media_pc or is_matrix_pc or is_matrix_nc
        if is_media_pc:
            border_clr = _MEDIA_PC_BORDER
        elif is_matrix_pc:
            border_clr = _MATRIX_PC_BORDER
        elif is_matrix_nc:
            border_clr = _MATRIX_NC_BORDER
        else:
            border_clr = _DEFAULT_BORDER
        border_thick = 3 if is_ctrl else 1

        for ci, cl in enumerate(COL_LABELS):
            well = f"{rl}{cl}"
            x_left = _MARGIN_L + ci * _CELL
            x1, y1 = x_left + 2, y_top + 2
            x2, y2 = x_left + _CELL - 2, y_top + _CELL - 2
            cx, cy = (x1 + x2) // 2, (y1 + y2) // 2

            row = by_well.get(well)

            if row is None or not row["is_filled"]:
                # Empty or undetected well
                cv2.rectangle(canvas, (x1, y1), (x2, y2), _EMPTY_CLR, -1)
                cv2.rectangle(canvas, (x1, y1), (x2, y2), border_clr, border_thick)
                cv2.putText(canvas, "·", (cx - 4, cy + 5),
                            _FONT, 0.5, (80, 80, 80), 1, cv2.LINE_AA)
            else:
                R = row.get("R", 180); G = row.get("G", 180); B = row.get("B", 180)
                if any(math.isnan(v) for v in [R, G, B]):
                    cell_bgr = _EMPTY_CLR
                else:
                    cell_bgr = (int(B), int(G), int(R))
                cv2.rectangle(canvas, (x1, y1), (x2, y2), cell_bgr, -1)
                cv2.rectangle(canvas, (x1, y1), (x2, y2), border_clr, border_thick)

                call = row.get("call", "")

                if call == "positive":
                    # Draw a bright "+" centred on the cell
                    fs = 0.9
                    tw = cv2.getTextSize("+", _FONT, fs, 2)[0][0]
                    cv2.putText(canvas, "+", (cx - tw // 2, cy + 8),
                                _FONT, fs, (0, 0, 0), 4, cv2.LINE_AA)
                    cv2.putText(canvas, "+", (cx - tw // 2, cy + 8),
                                _FONT, fs, _POSITIVE_MARK_CLR, 2, cv2.LINE_AA)

                elif is_media_pc:
                    cv2.putText(canvas, "MPC", (cx - 18, cy + 6),
                                _FONT, 0.40, (0, 0, 0), 3, cv2.LINE_AA)
                    cv2.putText(canvas, "MPC", (cx - 18, cy + 6),
                                _FONT, 0.40, _MEDIA_PC_BORDER, 1, cv2.LINE_AA)

                elif is_matrix_pc:
                    cv2.putText(canvas, "GPC", (cx - 18, cy + 6),
                                _FONT, 0.40, (0, 0, 0), 3, cv2.LINE_AA)
                    cv2.putText(canvas, "GPC", (cx - 18, cy + 6),
                                _FONT, 0.40, _MATRIX_PC_BORDER, 1, cv2.LINE_AA)

                elif is_matrix_nc:
                    cv2.putText(canvas, "NC", (cx - 14, cy + 6),
                                _FONT, 0.45, (0, 0, 0), 3, cv2.LINE_AA)
                    cv2.putText(canvas, "NC", (cx - 14, cy + 6),
                                _FONT, 0.45, _MATRIX_NC_BORDER, 1, cv2.LINE_AA)

    # ── Title ─────────────────────────────────────────────────────────────────
    cv2.putText(canvas, title, (10, 30),
                _FONT, 0.70, (220, 220, 220), 1, cv2.LINE_AA)

    return canvas


# ── Folder-level processing ───────────────────────────────────────────────────

_IMG_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}


def collect_images(plates_dir: Path) -> list[Path]:
    """Return plate images sorted chronologically.

    Sort key (in priority order):
    1. Explicit <digits>min token → numeric sort   (e.g. 30min < 90min < 120min)
    2. Timestamp token YYYY-MM-DD_HH-MM-SS         → alphabetical = chronological
    3. Everything else                              → alphabetical by filename

    Supports .jpg / .jpeg / .png / .tif / .tiff.
    """
    imgs = [p for p in plates_dir.iterdir()
            if p.suffix.lower() in _IMG_EXTS]

    def _key(p: Path):
        m = re.search(r"(\d+)min", p.stem, re.IGNORECASE)
        if m:
            return (0, int(m.group(1)), p.stem)   # numeric timepoint
        if _TS_PATTERN.search(p.stem):
            return (1, 0, p.stem)                  # timestamp → alpha = chrono
        return (2, 0, p.stem)                      # other → alpha

    imgs.sort(key=_key)
    return imgs


def already_processed(csv_path: Path) -> set[str]:
    """Return the set of image_name values already written to the CSV."""
    if not csv_path.exists():
        return set()
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        return {row["image_name"] for row in reader}


def process_folder(
    plates_dir: Path,
    weights_path: Path,
    out_dir: Path,
    cfg: SpotAssayConfig,
    reprocess: bool = False,
) -> None:
    """Process all images in plates_dir as a time series.

    Idempotent: already-processed images are skipped unless reprocess=True.
    Outputs
    -------
    out_dir/well_colors.csv               — cumulative per-well data
    out_dir/plate_grids/<stem>_grid.png   — per-image plate grid
    out_dir/latest_summary.png            — copy of the last grid
    """
    from ultralytics import YOLO

    out_dir.mkdir(parents=True, exist_ok=True)
    grids_dir = out_dir / "plate_grids"
    grids_dir.mkdir(exist_ok=True)

    csv_path = out_dir / "well_colors.csv"

    images = collect_images(plates_dir)
    if not images:
        print(f"No images found in {plates_dir}")
        return

    done = set() if reprocess else already_processed(csv_path)
    to_process = [p for p in images if p.name not in done]

    if not to_process:
        print("All images already processed. Use --reprocess to reanalyse.")
        return

    print(f"Found {len(images)} image(s), {len(to_process)} to process.")

    model = YOLO(str(weights_path))
    csv_exists = csv_path.exists() and not reprocess

    with open(csv_path, "a" if csv_exists else "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        if not csv_exists:
            writer.writeheader()

        last_grid_path: Optional[Path] = None

        ref_stem = images[0].stem if images else None
        for idx, img_path in enumerate(to_process):
            # Use absolute index within full sorted list for timepoint fallback
            abs_idx = images.index(img_path)
            timepoint = infer_timepoint(img_path.stem, abs_idx, ref_stem=ref_stem)

            print(f"\n[{idx+1}/{len(to_process)}] {img_path.name}  "
                  f"(t={timepoint} min)")

            status, well_rows = process_image(img_path, model, cfg, timepoint)
            print(f"  Status: {status}  |  wells: {len(well_rows)}")

            if not well_rows:
                print("  Skipping — no wells returned.")
                continue

            # Count calls
            calls = [r["call"] for r in well_rows]
            n_pos  = calls.count("positive")
            n_neg  = calls.count("negative")
            n_ctrl = calls.count("control")
            n_ign  = calls.count("ignored")
            print(f"  positive={n_pos}  negative={n_neg}  "
                  f"control={n_ctrl}  ignored={n_ign}")

            writer.writerows(well_rows)
            fh.flush()

            # ── Grid image ───────────────────────────────────────────────────
            title = f"{img_path.stem}  |  t={timepoint}min"
            grid_img = draw_plate_grid(well_rows, cfg, title)
            grid_path = grids_dir / f"{img_path.stem}_grid.png"
            cv2.imwrite(str(grid_path), grid_img)
            print(f"  Grid → {grid_path}")
            last_grid_path = grid_path

        # ── latest_summary.png ────────────────────────────────────────────────
        if last_grid_path is not None:
            summary_path = out_dir / "latest_summary.png"
            shutil.copy2(str(last_grid_path), str(summary_path))
            print(f"\nlatest_summary.png → {summary_path}")

    print(f"\nCSV  → {csv_path}")
    print(f"Done.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Spot-assay time-series processor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--plates",   required=True,  help="Directory of plate images")
    p.add_argument("--weights",  required=True,  help="YOLO weights (.pt)")
    p.add_argument("--out",      required=True,  help="Output directory")
    p.add_argument("--reprocess", action="store_true",
                   help="Re-analyse images that are already in the CSV")

    # Configurable thresholds
    p.add_argument("--media-pc-row",  default="H",
                   help="Media positive-control row label")
    p.add_argument("--matrix-pc-row", default="G",
                   help="Matrix positive-control row label (PC reference for scoring)")
    p.add_argument("--matrix-nc-row", default="F",
                   help="Matrix negative-control row label (NC reference for scoring)")
    p.add_argument("--empty-v-thresh", type=int, default=210,
                   help="HSV V threshold above which a well is candidate empty")
    p.add_argument("--empty-s-thresh", type=int, default=25,
                   help="HSV S threshold below which a well (with high V) is empty")
    p.add_argument("--pc-nc-min-deltaE", type=float, default=5.0,
                   help="Min ΔE between PC and NC means; below this → FAILED")
    p.add_argument("--min-meaningful-deltaE", type=float, default=2.3,
                   help="Min ΔE from NC for a well change to be meaningful")
    p.add_argument("--min-b-lab-filled", type=float, default=3.0,
                   help="Min Lab b* for a well to be classified as filled "
                        "(below this → empty plastic background)")
    p.add_argument("--nc-ref-max-internal-dE", type=float, default=3.0,
                   help="Warn if mean ΔE of NC wells vs NC mean exceeds this "
                        "(indicates contaminated NC reference)")
    p.add_argument("--no-require-closer-to-pc", action="store_true",
                   help="Disable the 'closer to PC than NC' positivity requirement")
    p.add_argument("--conf",  type=float, default=0.20,
                   help="YOLO detection confidence threshold")
    p.add_argument("--imgsz", type=int,   default=1280,
                   help="YOLO inference image size")

    args = p.parse_args()

    cfg = SpotAssayConfig(
        media_pc_row            = args.media_pc_row,
        matrix_pc_row           = args.matrix_pc_row,
        matrix_nc_row           = args.matrix_nc_row,
        empty_v_thresh          = args.empty_v_thresh,
        empty_s_thresh          = args.empty_s_thresh,
        min_b_lab_filled        = args.min_b_lab_filled,
        nc_ref_max_internal_dE  = args.nc_ref_max_internal_dE,
        min_meaningful_deltaE   = args.min_meaningful_deltaE,
        require_closer_to_pc    = not args.no_require_closer_to_pc,
        conf                    = args.conf,
        imgsz                   = args.imgsz,
    )

    process_folder(
        plates_dir   = Path(args.plates),
        weights_path = Path(args.weights),
        out_dir      = Path(args.out),
        cfg          = cfg,
        reprocess    = args.reprocess,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
