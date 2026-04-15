"""
tools/yolo_color_pipeline.py
-----------------------------
End-to-end well color classification using YOLO for BOTH detection AND sampling.

Pipeline
--------
  1. YOLO detects all 96 well bounding boxes (individual center + radius per well)
  2. Sort YOLO boxes into 8×12 grid → assign well labels (A1…H12)
  3. Sample color from each well's OWN YOLO crop (inner 50% disk)
     — each well uses its own YOLO-detected radius, not a global value
  4. Name each well's color from its measured HSV/RGB (free-form: "pale amber", "vivid orange", …)
  5. Labeled overlay + stats panel

Usage
-----
cd /home/rami/plate_analysis

python -m tools.yolo_color_pipeline \
    --image   /home/rami/plates/Oat_milk_LOD_240min.jpg \
    --weights /home/rami/yolo_well_dataset/runs/detect/well/weights/best.pt \
    --out     /tmp/yolo_color_out
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import cv2
import numpy as np
from scipy.spatial.distance import cdist

from tools.hough_grid_annotate import (
    ALL_WELLS, ROW_LABELS, COL_LABELS,
    robust_linear_grid, KDE_BW_FRAC, KDE_EXTRA,
)
from scipy.signal import find_peaks

# ── Dynamic color rendering ───────────────────────────────────────────────────
# Labels are generated per-well from actual RGB values — no fixed palette.

def label_to_bgr(label: str, color_data: dict) -> tuple[int, int, int]:
    """Return a BGR display color that matches the well's actual measured color."""
    r = color_data.get("mean_R", 180)
    g = color_data.get("mean_G", 180)
    b = color_data.get("mean_B", 180)
    if any(np.isnan(v) for v in [r, g, b]):
        return (60, 60, 60)
    return (int(b), int(g), int(r))


def label_short(label: str) -> str:
    """Compact 2–4 char abbreviation for overlay text."""
    words = label.split()
    if len(words) == 1:
        return words[0][:3].capitalize()
    # First letter of each word, max 4 chars
    return "".join(w[0].upper() for w in words)[:4]


# ── Step 1+2: YOLO detect → assign grid labels ────────────────────────────────

def yolo_detect_and_assign(
    img_bgr: np.ndarray,
    model,
    conf:  float = 0.20,
    imgsz: int   = 1280,
) -> dict[str, dict] | None:
    """
    Run YOLO, sort detected boxes into an 8×12 grid, assign well labels.

    Returns dict keyed by well label (e.g. "A1"):
        cx, cy   — center in original image pixels
        r        — radius (half mean bbox dim) in pixels
        x1,y1,x2,y2 — YOLO bbox coords
        yolo_conf — detection confidence
    Returns None if fewer than 90 wells detected.
    """
    ih, iw = img_bgr.shape[:2]
    results = model(img_bgr, conf=conf, imgsz=imgsz, verbose=False)
    boxes   = results[0].boxes
    if boxes is None or len(boxes) < 90:
        return None

    xyxy  = boxes.xyxy.cpu().numpy()
    confs = boxes.conf.cpu().numpy()

    # Build array: [cx, cy, r, x1, y1, x2, y2, conf]
    detections = []
    for i, (x1, y1, x2, y2) in enumerate(xyxy):
        cx = (x1 + x2) / 2.0
        cy = (y1 + y2) / 2.0
        r  = ((x2 - x1) + (y2 - y1)) / 4.0   # mean half-dim ≈ radius
        detections.append(dict(cx=cx, cy=cy, r=r,
                               x1=x1, y1=y1, x2=x2, y2=y2,
                               yolo_conf=float(confs[i])))

    if len(detections) < 90:
        return None

    pts = np.array([[d["cx"], d["cy"]] for d in detections])

    # ── Assign rows using KDE on y-coordinates ──────────────────────────────
    # Estimate pitch from nearest-neighbour distances
    D = cdist(pts, pts); np.fill_diagonal(D, np.inf)
    nn_d = D.min(axis=1)
    pitch_est = float(np.median(nn_d[nn_d < np.percentile(nn_d, 80)]))
    bw = pitch_est * KDE_BW_FRAC

    def peaks1d(values, n_min, n_want, bw):
        lo, hi = values.min() - bw, values.max() + bw
        xs  = np.linspace(lo, hi, 3000)
        kde = np.zeros(len(xs))
        for v in values:
            kde += np.exp(-0.5 * ((xs - v) / bw) ** 2)
        min_sep = max(1, int(len(xs) * bw * 1.2 / (hi - lo)))
        pks, pr = find_peaks(kde, distance=min_sep, height=0)
        if len(pks) < n_min:
            return None
        take  = min(n_want, len(pks))
        top_n = pks[np.argsort(pr["peak_heights"])[::-1][:take]]
        return np.sort(xs[top_n])

    col_c = peaks1d(pts[:, 0], 12, 12 + KDE_EXTRA, bw)
    row_c = peaks1d(pts[:, 1],  8,  8 + KDE_EXTRA, bw)
    if col_c is None or row_c is None:
        return None

    try:
        col_centers, _ = robust_linear_grid(col_c, 12)
        row_centers, _ = robust_linear_grid(row_c,  8)
    except ValueError:
        return None

    col_centers = np.clip(col_centers, 0, iw - 1)
    row_centers = np.clip(row_centers, 0, ih - 1)

    # ── Match each YOLO box to nearest grid position ─────────────────────────
    grid_xy = np.array(
        [[col_centers[ci], row_centers[ri]]
         for ri in range(8) for ci in range(12)],
        dtype=np.float32,
    )  # shape (96, 2)

    D_match = cdist(pts, grid_xy)             # (n_det, 96)
    assigned: dict[str, dict] = {}

    for gi, well in enumerate(ALL_WELLS):
        # Find the closest unassigned detection to this grid position
        col_i = D_match[:, gi].argmin()
        assigned[well] = detections[col_i]

    return assigned


# ── Step 3: color sampling using each well's own YOLO bbox ───────────────────

def sample_well_color_from_bbox(
    bgr: np.ndarray,
    lab: np.ndarray,
    hsv: np.ndarray,
    well_det: dict,
    radius_frac: float = 0.50,
) -> dict:
    """
    Sample color from the inner disk of a YOLO-detected well.
    Uses the well's own detected radius (not a global value).
    """
    cx, cy, r = well_det["cx"], well_det["cy"], well_det["r"]
    ih, iw = bgr.shape[:2]

    x0 = max(0, int(cx - r) - 1)
    x1 = min(iw, int(cx + r) + 2)
    y0 = max(0, int(cy - r) - 1)
    y1 = min(ih, int(cy + r) + 2)

    ys, xs = np.mgrid[y0:y1, x0:x1]
    dist = np.sqrt((xs - cx) ** 2 + (ys - cy) ** 2)
    mask = dist <= (r * radius_frac)

    n_px = int(mask.sum())
    if n_px == 0:
        nan = float("nan")
        return dict(mean_R=nan, mean_G=nan, mean_B=nan,
                    mean_L=nan, mean_a=nan, mean_b_lab=nan,
                    mean_V=nan, mean_S=nan, n_pixels=0,
                    cx=cx, cy=cy, r=r)

    px_bgr = bgr[y0:y1, x0:x1][mask]
    px_lab = lab[y0:y1, x0:x1][mask]
    px_hsv = hsv[y0:y1, x0:x1][mask]

    mb, mg, mr = px_bgr.mean(axis=0)
    return dict(
        mean_R     = round(float(mr), 2),
        mean_G     = round(float(mg), 2),
        mean_B     = round(float(mb), 2),
        mean_L     = round(float(px_lab[:, 0].mean()) / 255.0 * 100.0, 2),
        mean_a     = round(float(px_lab[:, 1].mean()) - 128.0, 2),
        mean_b_lab = round(float(px_lab[:, 2].mean()) - 128.0, 2),
        mean_V     = round(float(px_hsv[:, 2].mean()), 2),
        mean_S     = round(float(px_hsv[:, 1].mean()), 2),
        n_pixels   = n_px,
        cx=round(cx, 1), cy=round(cy, 1), r=round(r, 1),
    )


# ── Step 4+5: free-form color naming from HSV ────────────────────────────────

# Hue ranges (OpenCV: H is 0–180, i.e. degrees/2)
_HUE_NAMES = [
    (0,   5,   "red"),
    (5,   12,  "red-orange"),
    (12,  18,  "orange"),
    (18,  22,  "dark orange"),
    (22,  28,  "amber"),
    (28,  38,  "golden yellow"),
    (38,  48,  "yellow"),
    (48,  58,  "yellow-green"),
    (58,  80,  "green"),
    (80,  100, "teal"),
    (100, 130, "cyan"),
    (130, 155, "blue"),
    (155, 165, "violet"),
    (165, 175, "magenta"),
    (175, 180, "red"),   # wraps back
]

def _hue_name(h_cv: float) -> str:
    """Map OpenCV hue (0–180) to a color family name."""
    for lo, hi, name in _HUE_NAMES:
        if lo <= h_cv < hi:
            return name
    return "red"   # fallback for h≈180


def name_color(color_data: dict) -> str:
    """
    Name a well's color from its measured HSV + RGB — no fixed categories.

    Uses:
      mean_H  — dominant hue angle (derived from mean R/G/B)
      mean_S  — saturation (0–255)
      mean_V  — brightness  (0–255)

    Returns a natural English color description like:
      "white", "cream", "pale amber", "vivid orange",
      "deep red-orange", "golden yellow", "muted teal", …
    """
    R = color_data.get("mean_R", float("nan"))
    G = color_data.get("mean_G", float("nan"))
    B = color_data.get("mean_B", float("nan"))
    S = color_data.get("mean_S", float("nan"))
    V = color_data.get("mean_V", float("nan"))

    if any(np.isnan(x) for x in [R, G, B, S, V]):
        return "unknown"

    # Recompute hue from mean RGB (more stable than averaging per-pixel hue)
    px = np.array([[[int(B), int(G), int(R)]]], dtype=np.uint8)
    h_cv = float(cv2.cvtColor(px, cv2.COLOR_BGR2HSV)[0, 0, 0])

    # ── Achromatic: low saturation ────────────────────────────────────────
    if S < 18:
        if V > 220:  return "white"
        if V > 190:  return "off-white"
        if V > 155:  return "light grey"
        if V > 110:  return "grey"
        if V > 70:   return "dark grey"
        return "near black"

    # ── Chromatic: name hue + add lightness/saturation modifiers ─────────
    base = _hue_name(h_cv)

    # Lightness modifier (from V)
    if V > 215:    light = "pale"
    elif V > 185:  light = "light"
    elif V > 155:  light = ""          # no prefix — "medium" brightness
    elif V > 115:  light = "dark"
    else:          light = "deep"

    # Saturation modifier (from S)
    if S > 200:    sat = "vivid"
    elif S > 140:  sat = ""            # no prefix — clearly chromatic
    elif S > 70:   sat = "muted"
    else:          sat = "pale"        # very desaturated but still hued

    # Combine: avoid doubling up modifiers
    parts = [p for p in [light, sat, base] if p]
    # Simplify common combos
    name = " ".join(parts)
    replacements = {
        "pale pale":        "pale",
        "light pale":       "pale",
        "pale vivid":       "vivid",
        "deep vivid":       "deep vivid",
        "pale muted":       "pale",
        "light muted":      "muted light",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)

    return name.strip()


# ── Step 6: overlay ───────────────────────────────────────────────────────────

def draw_labeled_overlay(
    img_bgr: np.ndarray,
    assigned: dict[str, dict],
    labels: dict[str, str],
    color_data: dict[str, dict],
) -> np.ndarray:
    ov = img_bgr.copy()
    ih, iw = ov.shape[:2]

    # Grid lines connecting YOLO centers
    ROWS, COLS = len(ROW_LABELS), len(COL_LABELS)
    grid = np.array(
        [[assigned[f"{rl}{cl}"]["cx"], assigned[f"{rl}{cl}"]["cy"]]
         for rl in ROW_LABELS for cl in COL_LABELS],
        dtype=np.float32,
    ).reshape(ROWS, COLS, 2)
    for ri in range(ROWS):
        cv2.polylines(ov, [grid[ri].astype(np.int32).reshape(-1,1,2)],
                      False, (40,40,40), 1, cv2.LINE_AA)
    for ci in range(COLS):
        cv2.polylines(ov, [grid[:,ci].astype(np.int32).reshape(-1,1,2)],
                      False, (40,40,40), 1, cv2.LINE_AA)

    font       = cv2.FONT_HERSHEY_SIMPLEX
    med_r      = float(np.median([d["r"] for d in assigned.values()]))
    font_scale = max(0.35, med_r / 120.0)
    thickness  = max(1, int(font_scale * 2))

    for well in ALL_WELLS:
        if well not in assigned:
            continue
        det   = assigned[well]
        cx, cy, r = det["cx"], det["cy"], det["r"]
        label = labels.get(well, "unknown")
        cd    = color_data.get(well, {})
        color = label_to_bgr(label, cd)
        r_int = max(3, int(r))

        # Semi-transparent fill using each well's own radius
        overlay = ov.copy()
        cv2.circle(overlay, (int(cx), int(cy)), r_int, color, -1)
        cv2.addWeighted(overlay, 0.45, ov, 0.55, 0, ov)

        # Border
        cv2.circle(ov, (int(cx), int(cy)), r_int, color, 2, cv2.LINE_AA)

        # Label text
        short = label_short(label)
        tw, th = cv2.getTextSize(short, font, font_scale, thickness)[0]
        tx = int(cx - tw / 2)
        ty = int(cy + th / 2)
        cv2.putText(ov, short, (tx, ty), font, font_scale, (20,20,20), thickness+1, cv2.LINE_AA)
        cv2.putText(ov, short, (tx, ty), font, font_scale, (255,255,255), thickness, cv2.LINE_AA)

    # Row / column axis labels
    ax_scale = max(0.5, med_r / 100.0)
    for rl in ROW_LABELS:
        d = assigned.get(f"{rl}1")
        if d:
            cv2.putText(ov, rl,
                        (max(0, int(d["cx"]) - int(d["r"]) - 40), int(d["cy"]) + 8),
                        font, ax_scale, (0, 230, 230), 2, cv2.LINE_AA)
    for cl in COL_LABELS:
        d = assigned.get(f"A{cl}")
        if d:
            cv2.putText(ov, cl,
                        (int(d["cx"]) - 12, max(0, int(d["cy"]) - int(d["r"]) - 15)),
                        font, ax_scale * 0.85, (0, 230, 230), 2, cv2.LINE_AA)

    # Legend — built from observed labels only
    from collections import Counter
    seen = Counter(labels.values())
    lx, ly = 10, ih - 10
    for lbl, _ in seen.most_common():
        if lbl in ("unknown", "—"):
            continue
        # Find first well with this label to get its display color
        cd_ex = next((color_data[w] for w in ALL_WELLS
                      if labels.get(w) == lbl and w in color_data), {})
        bgr_ex = label_to_bgr(lbl, cd_ex)
        ly -= 32
        if ly < 0:
            break
        cv2.rectangle(ov, (lx, ly), (lx + 24, ly + 24), bgr_ex, -1)
        cv2.rectangle(ov, (lx, ly), (lx + 24, ly + 24), (80,80,80), 1)
        cv2.putText(ov, lbl, (lx + 30, ly + 17), font, 0.55, (230,230,230), 1, cv2.LINE_AA)

    return ov


def draw_stats_panel(
    img_bgr: np.ndarray,
    labels: dict[str, str],
    color_data: dict[str, dict],
) -> np.ndarray:
    from collections import Counter
    counts = Counter(labels.values())
    total  = len(labels)

    panel_w = 420
    panel   = np.full((img_bgr.shape[0], panel_w, 3), 25, dtype=np.uint8)
    font    = cv2.FONT_HERSHEY_SIMPLEX

    y = 40
    cv2.putText(panel, "Color Summary", (14, y), font, 0.9, (220,220,220), 2)
    y += 35
    cv2.putText(panel, f"Wells: {total}", (14, y), font, 0.58, (100,200,255), 1)

    y += 36
    cv2.line(panel, (10, y), (panel_w-10, y), (60,60,60), 1)
    y += 25

    # Bar chart — sorted by count descending
    for lbl, n in counts.most_common():
        if n == 0:
            continue
        # Representative color: pick first well with this label
        cd_ex = next((color_data[w] for w in ALL_WELLS
                      if labels.get(w) == lbl and w in color_data), {})
        bgr = label_to_bgr(lbl, cd_ex)
        bar_w = max(4, int((n / total) * (panel_w - 140)))
        cv2.rectangle(panel, (14, y-16), (14 + bar_w, y + 4), bgr, -1)
        cv2.putText(panel, f"{lbl} ({n})", (14 + bar_w + 8, y),
                    font, 0.50, (210,210,210), 1)
        y += 30
        if y > img_bgr.shape[0] - 180:
            break

    # Plate heatmap grid
    y += 12
    cv2.putText(panel, "Plate grid", (14, y), font, 0.7, (200,200,200), 1)
    y += 18
    cell     = max(18, (panel_w - 80) // 12)
    margin_x = (panel_w - cell * 12) // 2

    for ri, rl in enumerate(ROW_LABELS):
        cv2.putText(panel, rl, (margin_x - 22, y + ri*cell + cell//2 + 5),
                    font, 0.45, (180,180,180), 1)
        for ci, cl in enumerate(COL_LABELS):
            well  = f"{rl}{cl}"
            label = labels.get(well, "unknown")
            cd_w  = color_data.get(well, {})
            bgr   = label_to_bgr(label, cd_w)
            x0    = margin_x + ci * cell
            y0    = y + ri * cell
            cv2.rectangle(panel, (x0+1, y0+1), (x0+cell-1, y0+cell-1), bgr, -1)

    for ci, cl in enumerate(COL_LABELS):
        x0 = margin_x + ci * cell + cell//2 - 5
        cv2.putText(panel, cl, (x0, y + 8*cell + 14), font, 0.35, (180,180,180), 1)

    return panel


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args):
    from ultralytics import YOLO

    img_path = Path(args.image)
    out_dir  = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Image: {img_path.name}")

    img = cv2.imread(str(img_path))
    if img is None:
        print(f"Cannot read {img_path}"); return
    ih, iw = img.shape[:2]
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    # ── 1+2: YOLO detect + assign grid labels ─────────────────────────────────
    print("Running YOLO detection …")
    model    = YOLO(args.weights)
    assigned = yolo_detect_and_assign(img, model, conf=args.conf, imgsz=args.imgsz)
    if assigned is None:
        print("YOLO detection failed"); return

    radii = [d["r"] for d in assigned.values()]
    print(f"  {len(assigned)} wells assigned  "
          f"median_r={np.median(radii):.1f}px  "
          f"r_range=[{min(radii):.0f}, {max(radii):.0f}]px")

    # ── 3: Sample color from each well's own YOLO bbox ────────────────────────
    print("Sampling well colors from YOLO crops …")
    color_data: dict[str, dict] = {}
    for well, det in assigned.items():
        color_data[well] = sample_well_color_from_bbox(
            img, lab, hsv, det, radius_frac=args.radius_frac
        )

    # ── 4: Name each well's color from its measured HSV/RGB ──────────────────
    print("Naming well colors …")
    labels: dict[str, str] = {}
    for well, d in color_data.items():
        labels[well] = name_color(d)

    from collections import Counter
    counts = Counter(labels.values())
    print("\n  Color counts:")
    for lbl, n in counts.most_common():
        print(f"    {lbl:20s}: {n}")

    # ── 5: Draw overlays ──────────────────────────────────────────────────────
    print("\nDrawing overlay …")
    ov    = draw_labeled_overlay(img, assigned, labels, color_data)
    panel = draw_stats_panel(img, labels, color_data)

    # Scale overlay to panel height for side-by-side
    scale    = panel.shape[0] / ov.shape[0]
    ov_small = cv2.resize(ov, (int(iw * scale), panel.shape[0]))
    combined = np.hstack([ov_small, panel])

    stem = img_path.stem
    overlay_path  = out_dir / f"{stem}_yolo_color.png"
    combined_path = out_dir / f"{stem}_yolo_color_combined.png"
    json_path     = out_dir / f"{stem}_labels.json"

    cv2.imwrite(str(overlay_path),  cv2.resize(ov, (iw//3, ih//3)))
    cv2.imwrite(str(combined_path), combined)

    result = {
        "image": img_path.name,
        "n_wells": len(assigned),
        "labels": labels,
        "color_data": {
            w: {k: round(float(v), 3) if isinstance(v, (float, np.floating)) else v
                for k, v in d.items()}
            for w, d in color_data.items()
        },
    }
    with open(json_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nOutputs:")
    print(f"  Overlay:  {overlay_path}")
    print(f"  Combined: {combined_path}")
    print(f"  JSON:     {json_path}")


def main():
    p = argparse.ArgumentParser(description="YOLO well detection + color classification")
    p.add_argument("--image",       required=True)
    p.add_argument("--weights",     required=True)
    p.add_argument("--out",         required=True)
    p.add_argument("--conf",        type=float, default=0.20)
    p.add_argument("--imgsz",       type=int,   default=1280)
    p.add_argument("--radius-frac", type=float, default=0.50, dest="radius_frac")
    args = p.parse_args()
    run(args)


if __name__ == "__main__":
    main()
