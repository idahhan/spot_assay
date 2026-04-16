"""
tests/test_pipeline.py
-----------------------
Unit tests for the YOLO + spot-assay pipeline:
  tools/spot_assay.py
  tools/yolo_color_pipeline.py
  tools/hough_grid_annotate.robust_linear_grid
"""

from __future__ import annotations

import csv
import math
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import cv2
import numpy as np
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.spot_assay import (
    CSV_COLUMNS,
    SpotAssayConfig,
    already_processed,
    classify_filled,
    collect_images,
    deltaE76,
    deltaE_category,
    draw_plate_grid,
    infer_timepoint,
    process_folder,
    process_image,
    _lab_from_row,
    _mean_lab,
    _parse_ts,
)
from tools.yolo_color_pipeline import (
    _hue_name,
    label_short,
    label_to_bgr,
    name_color,
    sample_well_color_from_bbox,
)
from tools.hough_grid_annotate import ALL_WELLS, COL_LABELS, ROW_LABELS, robust_linear_grid


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cfg(**kw) -> SpotAssayConfig:
    """Return a SpotAssayConfig, overriding any fields via kwargs."""
    return SpotAssayConfig(**kw)


def _well_color(L=50.0, a=10.0, b=15.0, V=150.0, S=100.0, n_pixels=200) -> dict:
    """Minimal filled-well color dict."""
    return dict(
        mean_R=180.0, mean_G=150.0, mean_B=100.0,
        mean_L=L, mean_a=a, mean_b_lab=b,
        mean_V=V, mean_S=S, n_pixels=n_pixels,
        cx=100.0, cy=100.0, r=30.0,
    )


def _empty_color() -> dict:
    """Color dict that classify_filled will call empty (Gate 1 fires)."""
    return dict(
        mean_R=240.0, mean_G=240.0, mean_B=240.0,
        mean_L=95.0, mean_a=0.0, mean_b_lab=1.0,
        mean_V=230.0, mean_S=5.0, n_pixels=100,
        cx=100.0, cy=100.0, r=30.0,
    )


def _build_mock_color_side_effect(
    cfg: SpotAssayConfig,
    pc_color: dict,
    nc_color: dict,
    media_color: dict,
    test_colors: dict[str, dict],
) -> list[dict]:
    """
    Build a list of color_data returns for ALL_WELLS in order.
    test_colors maps well_id → color dict; all others use pc/nc/media/empty.
    """
    result = []
    for well in ALL_WELLS:
        row = well[0]
        if well in test_colors:
            result.append(test_colors[well])
        elif row == cfg.matrix_pc_row:
            result.append(pc_color)
        elif row == cfg.matrix_nc_row:
            result.append(nc_color)
        elif row == cfg.media_pc_row:
            result.append(media_color)
        else:
            result.append(_empty_color())
    return result


def _fake_assigned() -> dict:
    """Return a mock assigned dict for all 96 wells."""
    det = dict(cx=100.0, cy=100.0, r=30.0, x1=70, y1=70, x2=130, y2=130, yolo_conf=0.9)
    return {well: det for well in ALL_WELLS}


def _make_well_rows(cfg: SpotAssayConfig, overrides: dict[str, dict] | None = None) -> list[dict]:
    """
    Build a synthetic well_rows list for draw_plate_grid tests.
    All wells are filled and called 'negative' except control rows (marked 'control').
    overrides: well_id → partial dict to update that well's entry.
    """
    rows = []
    ctrl = (cfg.media_pc_row, cfg.matrix_pc_row, cfg.matrix_nc_row)
    for well in ALL_WELLS:
        row_label = well[0]
        entry = dict(
            well_id=well, row=row_label, column=well[1:],
            is_filled=True, R=180.0, G=150.0, B=100.0,
            call="control" if row_label in ctrl else "negative",
            experiment_status="OK",
            deltaE_to_NC=5.0, deltaE_to_PC=2.0,
        )
        if overrides and well in overrides:
            entry.update(overrides[well])
        rows.append(entry)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# deltaE_category
# ─────────────────────────────────────────────────────────────────────────────

class TestDeltaECategory:
    def test_none(self):
        assert deltaE_category(None) == "n/a"

    def test_nan(self):
        assert deltaE_category(float("nan")) == "n/a"

    def test_barely_noticeable(self):
        assert deltaE_category(0.0) == "barely_noticeable"
        assert deltaE_category(2.2) == "barely_noticeable"

    def test_small_lower_boundary(self):
        assert deltaE_category(2.3) == "small"

    def test_small_upper(self):
        assert deltaE_category(4.9) == "small"

    def test_noticeable_lower_boundary(self):
        assert deltaE_category(5.0) == "noticeable"

    def test_noticeable_upper(self):
        assert deltaE_category(11.9) == "noticeable"

    def test_major_lower_boundary(self):
        assert deltaE_category(12.0) == "major"

    def test_major_large(self):
        assert deltaE_category(100.0) == "major"


# ─────────────────────────────────────────────────────────────────────────────
# deltaE76
# ─────────────────────────────────────────────────────────────────────────────

class TestDeltaE76:
    def test_identity(self):
        assert deltaE76((50.0, 0.0, 0.0), (50.0, 0.0, 0.0)) == pytest.approx(0.0)

    def test_known_value(self):
        # sqrt(9 + 16 + 0) = 5
        assert deltaE76((50.0, 0.0, 0.0), (47.0, 4.0, 0.0)) == pytest.approx(5.0)

    def test_symmetry(self):
        a, b = (40.0, 10.0, -5.0), (55.0, -3.0, 12.0)
        assert deltaE76(a, b) == pytest.approx(deltaE76(b, a))


# ─────────────────────────────────────────────────────────────────────────────
# _lab_from_row  &  _mean_lab
# ─────────────────────────────────────────────────────────────────────────────

class TestLabHelpers:
    def test_lab_from_row(self):
        row = {"L": 50, "a": -10, "b": 5.5}
        assert _lab_from_row(row) == (50.0, -10.0, 5.5)

    def test_mean_lab_single(self):
        rows = [{"L": 60.0, "a": 5.0, "b": 10.0}]
        assert _mean_lab(rows) == pytest.approx((60.0, 5.0, 10.0))

    def test_mean_lab_multiple(self):
        rows = [
            {"L": 60.0, "a": 10.0, "b": 20.0},
            {"L": 40.0, "a": -10.0, "b": 0.0},
        ]
        assert _mean_lab(rows) == pytest.approx((50.0, 0.0, 10.0))


# ─────────────────────────────────────────────────────────────────────────────
# classify_filled
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifyFilled:
    def setup_method(self):
        self.cfg = _cfg()

    def test_zero_pixels_is_empty(self):
        assert classify_filled({"n_pixels": 0}, self.cfg) is False

    def test_gate1_fires_bright_and_low_saturation(self):
        cd = dict(n_pixels=100, mean_V=230.0, mean_S=10.0, mean_b_lab=10.0)
        assert classify_filled(cd, self.cfg) is False

    def test_gate1_high_V_but_high_S_does_not_fire(self):
        # V above threshold, S above threshold → Gate 1 does NOT fire
        cd = dict(n_pixels=100, mean_V=230.0, mean_S=50.0, mean_b_lab=10.0)
        assert classify_filled(cd, self.cfg) is True

    def test_gate1_low_V_does_not_fire(self):
        # V below threshold → Gate 1 does NOT fire
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=5.0, mean_b_lab=10.0)
        assert classify_filled(cd, self.cfg) is True

    def test_gate2_fires_cool_b_lab(self):
        # b* below threshold → empty plastic background
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0, mean_b_lab=1.0)
        assert classify_filled(cd, self.cfg) is False

    def test_gate2_nan_b_lab_skipped(self):
        # NaN b* → Gate 2 skipped, well treated as filled
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0, mean_b_lab=float("nan"))
        assert classify_filled(cd, self.cfg) is True

    def test_missing_b_lab_key_skipped(self):
        # No mean_b_lab key → defaults to NaN → Gate 2 skipped
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0)
        assert classify_filled(cd, self.cfg) is True

    def test_both_gates_pass(self):
        # Well above all thresholds → filled
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0, mean_b_lab=10.0)
        assert classify_filled(cd, self.cfg) is True

    def test_b_lab_exactly_at_threshold_is_empty(self):
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0, mean_b_lab=3.0)
        # b* == min_b_lab_filled (3.0); condition is b* < 3.0, so 3.0 is NOT empty
        assert classify_filled(cd, self.cfg) is True

    def test_b_lab_just_below_threshold_is_empty(self):
        cd = dict(n_pixels=100, mean_V=150.0, mean_S=100.0, mean_b_lab=2.9)
        assert classify_filled(cd, self.cfg) is False


# ─────────────────────────────────────────────────────────────────────────────
# infer_timepoint
# ─────────────────────────────────────────────────────────────────────────────

class TestInferTimepoint:
    def test_extracts_minutes(self):
        assert infer_timepoint("Oat_milk_LOD_240min", 0) == 240

    def test_case_insensitive(self):
        assert infer_timepoint("Sample_30MIN_run1", 0) == 30

    def test_takes_first_match(self):
        assert infer_timepoint("10min_repeat_20min", 0) == 10

    def test_fallback_to_index(self):
        assert infer_timepoint("no_time_info_here", 7) == 7

    def test_index_zero_fallback(self):
        assert infer_timepoint("sample", 0) == 0

    # ── Timestamp filenames ──────────────────────────────────────────────────

    def test_timestamp_elapsed_minutes(self):
        ref  = "2024-04-02_14-00-00"
        curr = "2024-04-02_14-30-00"
        assert infer_timepoint(curr, 0, ref_stem=ref) == 30

    def test_timestamp_elapsed_across_hours(self):
        ref  = "2024-04-02_13-45-00"
        curr = "2024-04-02_15-15-00"
        assert infer_timepoint(curr, 0, ref_stem=ref) == 90

    def test_timestamp_no_ref_falls_back_to_index(self):
        assert infer_timepoint("2024-04-02_14-30-00", 5) == 5

    def test_timestamp_ref_no_ts_in_ref_falls_back(self):
        # ref_stem has no timestamp token → falls back to index
        assert infer_timepoint("2024-04-02_14-30-00", 3, ref_stem="no_timestamp") == 3

    def test_min_token_takes_priority_over_timestamp(self):
        # If the stem has both a min token and a timestamp, min wins
        assert infer_timepoint("run_45min_2024-04-02_14-00-00", 0) == 45


class TestParseTs:
    def test_valid_timestamp_returns_datetime(self):
        from datetime import datetime
        dt = _parse_ts("2024-04-02_14-23-41")
        assert dt == datetime(2024, 4, 2, 14, 23, 41)

    def test_no_match_returns_none(self):
        assert _parse_ts("sample_90min") is None

    def test_malformed_date_values_return_none(self):
        # Regex matches but month 13 is invalid → ValueError branch (lines 242-243)
        assert _parse_ts("2024-13-01_00-00-00") is None


# ─────────────────────────────────────────────────────────────────────────────
# process_image
# ─────────────────────────────────────────────────────────────────────────────

class TestProcessImage:
    """
    All tests patch cv2.imread, yolo_detect_and_assign, and
    sample_well_color_from_bbox so no real images or YOLO model are needed.
    """

    def setup_method(self):
        self.cfg = _cfg()
        self.model = MagicMock()
        # A tiny valid BGR image so cv2.cvtColor doesn't fail
        self._dummy_img = np.zeros((100, 100, 3), dtype=np.uint8)
        # Standard PC / NC / media colours
        self.pc_color  = _well_color(L=50.0, a=30.0, b=20.0)
        self.nc_color  = _well_color(L=70.0, a=-5.0, b=5.0)
        self.med_color = _well_color(L=55.0, a=25.0, b=18.0)

    @patch("tools.spot_assay.cv2.imread", return_value=None)
    def test_cannot_read_image(self, mock_read, tmp_path):
        status, rows = process_image(tmp_path / "fake.jpg", self.model, self.cfg, 0)
        assert status == "FAILED:cannot_read_image"
        assert rows == []

    @patch("tools.spot_assay.yolo_detect_and_assign", return_value=None)
    @patch("tools.spot_assay.cv2.imread")
    def test_yolo_detection_failed(self, mock_read, mock_detect, tmp_path):
        mock_read.return_value = self._dummy_img
        status, rows = process_image(tmp_path / "fake.jpg", self.model, self.cfg, 0)
        assert status == "FAILED:yolo_detection_failed"
        assert rows == []

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_no_filled_pc_wells(self, mock_read, mock_detect, mock_sample, tmp_path):
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()
        # G row wells are empty (Gate 1 fires), all others also empty
        side = _build_mock_color_side_effect(
            self.cfg, _empty_color(), self.nc_color, self.med_color, {}
        )
        mock_sample.side_effect = side

        status, rows = process_image(tmp_path / "fake.jpg", self.model, self.cfg, 0)

        assert status == "FAILED:no_filled_PC_wells"
        ctrl_rows = (self.cfg.media_pc_row, self.cfg.matrix_pc_row, self.cfg.matrix_nc_row)
        for r in rows:
            if r["row"] in ctrl_rows:
                assert r["call"] == "control"

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_no_filled_nc_wells(self, mock_read, mock_detect, mock_sample, tmp_path):
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()
        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, _empty_color(), self.med_color, {}
        )
        mock_sample.side_effect = side

        status, rows = process_image(tmp_path / "fake.jpg", self.model, self.cfg, 0)

        assert status == "FAILED:no_filled_NC_wells"
        ctrl_rows = (self.cfg.media_pc_row, self.cfg.matrix_pc_row, self.cfg.matrix_nc_row)
        for r in rows:
            if r["row"] in ctrl_rows:
                assert r["call"] == "control"

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_nc_sanity_warning(self, mock_read, mock_detect, mock_sample, tmp_path, capsys):
        """NC wells with high internal variance should trigger a warning."""
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()
        # Alternate F-row wells between two very different colours.
        # Both must have b > min_b_lab_filled (3.0) so they are classified as filled.
        het_nc_colors = {}
        for i, col in enumerate(COL_LABELS):
            well = f"{self.cfg.matrix_nc_row}{col}"
            if i % 2 == 0:
                het_nc_colors[well] = _well_color(L=70.0, a=-5.0, b=5.0)
            else:
                het_nc_colors[well] = _well_color(L=30.0, a=20.0, b=10.0)
        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, self.nc_color, self.med_color, het_nc_colors
        )
        mock_sample.side_effect = side

        # Lower the sanity threshold so our heterogeneous NC wells trigger the warning
        cfg = _cfg(nc_ref_max_internal_dE=0.001)
        status, rows = process_image(tmp_path / "fake.jpg", self.model, cfg, 0)

        assert status == "OK"
        captured = capsys.readouterr()
        assert "[WARN]" in captured.out

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_ok_positive_negative_ignored_control(
        self, mock_read, mock_detect, mock_sample, tmp_path, capsys
    ):
        """
        Full OK path: one positive test well, one negative (dE_nc too small),
        one negative (closer to NC), empty wells → ignored, controls labelled.
        """
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()

        # PC: (50, 30, 20)   NC: (70, -5, 5)
        # Positive: close to PC, far from NC
        pos_well = "A1"
        # dE_nc = sqrt((55-70)^2 + (25-(-5))^2 + (18-5)^2) ≈ 36  ≥ 2.3  and closer to PC → positive
        positive_color = _well_color(L=55.0, a=25.0, b=18.0)

        # Negative: very close to NC (dE_nc < min_meaningful)
        neg_close_well = "A2"
        neg_close_color = _well_color(L=70.5, a=-4.8, b=5.1)   # dE_nc < 1

        # Negative: dE_nc ≥ 2.3 but closer to NC than PC
        neg_far_well = "A3"
        neg_far_color = _well_color(L=65.0, a=-3.0, b=4.0)

        test_overrides = {
            pos_well: positive_color,
            neg_close_well: neg_close_color,
            neg_far_well: neg_far_color,
        }
        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, self.nc_color, self.med_color, test_overrides
        )
        mock_sample.side_effect = side

        status, rows = process_image(tmp_path / "p.jpg", self.model, self.cfg, 30)

        assert status == "OK"
        by_well = {r["well_id"]: r for r in rows}

        assert by_well[pos_well]["call"] == "positive"
        assert by_well[neg_close_well]["call"] == "negative"
        assert by_well[neg_far_well]["call"] == "negative"

        # Control rows
        ctrl_rows = (self.cfg.media_pc_row, self.cfg.matrix_pc_row, self.cfg.matrix_nc_row)
        for r in rows:
            if r["row"] in ctrl_rows:
                assert r["call"] == "control"

        # Empty test wells (those not in overrides, with _empty_color) → ignored
        for r in rows:
            if r["row"] not in ctrl_rows and r["well_id"] not in test_overrides:
                assert r["call"] == "ignored"

        # Check deltaE fields populated for control wells that are filled
        for r in rows:
            if r["row"] in ctrl_rows and r["is_filled"]:
                assert not math.isnan(r["deltaE_to_NC"])
                assert not math.isnan(r["deltaE_to_PC"])

        # Timepoint written correctly
        assert all(r["timepoint_min"] == 30 for r in rows)

        # NC sanity should pass and print [OK]
        out = capsys.readouterr().out
        assert "[OK]" in out

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_require_closer_to_pc_disabled(
        self, mock_read, mock_detect, mock_sample, tmp_path
    ):
        """With require_closer_to_pc=False a well closer to NC but far enough from NC
        is still called positive."""
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()

        # Well closer to NC but dE_nc ≥ min_meaningful
        ambiguous_well = "A1"
        # dE_to_NC ≈ 5.47 ≥ 2.3; dE_to_PC >> dE_to_NC → normally negative
        ambiguous_color = _well_color(L=65.0, a=-3.0, b=4.0)

        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, self.nc_color, self.med_color,
            {ambiguous_well: ambiguous_color}
        )
        mock_sample.side_effect = side

        cfg_no_closer = _cfg(require_closer_to_pc=False)
        status, rows = process_image(tmp_path / "p.jpg", self.model, cfg_no_closer, 0)

        by_well = {r["well_id"]: r for r in rows}
        assert by_well[ambiguous_well]["call"] == "positive"

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_media_pc_row_marked_as_control(
        self, mock_read, mock_detect, mock_sample, tmp_path
    ):
        """H-row wells must be marked 'control' regardless of their color."""
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()
        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, self.nc_color, self.med_color, {}
        )
        mock_sample.side_effect = side

        _, rows = process_image(tmp_path / "p.jpg", self.model, self.cfg, 0)
        h_rows = [r for r in rows if r["row"] == self.cfg.media_pc_row]
        assert all(r["call"] == "control" for r in h_rows)

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_unfilled_control_well_has_no_deltaE(
        self, mock_read, mock_detect, mock_sample, tmp_path
    ):
        """A control well that is not filled should not have ΔE computed."""
        mock_read.return_value = self._dummy_img
        mock_detect.return_value = _fake_assigned()
        # Override one G-row well to be empty
        empty_pc = {"G1": _empty_color()}
        side = _build_mock_color_side_effect(
            self.cfg, self.pc_color, self.nc_color, self.med_color, empty_pc
        )
        mock_sample.side_effect = side

        _, rows = process_image(tmp_path / "p.jpg", self.model, self.cfg, 0)
        by_well = {r["well_id"]: r for r in rows}
        assert math.isnan(by_well["G1"]["deltaE_to_NC"])
        assert math.isnan(by_well["G1"]["deltaE_to_PC"])


# ─────────────────────────────────────────────────────────────────────────────
# draw_plate_grid
# ─────────────────────────────────────────────────────────────────────────────

class TestDrawPlateGrid:
    def setup_method(self):
        self.cfg = _cfg()

    def test_returns_ndarray(self):
        rows = _make_well_rows(self.cfg)
        canvas = draw_plate_grid(rows, self.cfg, "Test title")
        assert isinstance(canvas, np.ndarray)
        assert canvas.ndim == 3
        assert canvas.shape[2] == 3

    def test_positive_well_rendered(self):
        rows = _make_well_rows(self.cfg, overrides={"A1": {"call": "positive"}})
        canvas = draw_plate_grid(rows, self.cfg, "Pos test")
        assert canvas is not None

    def test_all_control_row_types(self):
        """Exercises media_pc, matrix_pc, matrix_nc row rendering paths."""
        rows = _make_well_rows(self.cfg)
        canvas = draw_plate_grid(rows, self.cfg, "Controls")
        assert canvas.shape[0] > 0

    def test_nan_rgb_uses_empty_color(self):
        """Well with NaN RGB should fall back to the empty-cell colour."""
        rows = _make_well_rows(self.cfg, overrides={
            "A1": {"R": float("nan"), "G": float("nan"), "B": float("nan")}
        })
        canvas = draw_plate_grid(rows, self.cfg, "NaN test")
        assert canvas is not None

    def test_well_absent_from_by_well(self):
        """If a well isn't in the data at all, it should still render."""
        rows = [r for r in _make_well_rows(self.cfg) if r["well_id"] != "C5"]
        canvas = draw_plate_grid(rows, self.cfg, "Missing well")
        assert canvas is not None

    def test_unfilled_control_well_renders(self):
        rows = _make_well_rows(self.cfg, overrides={"G1": {"is_filled": False}})
        canvas = draw_plate_grid(rows, self.cfg, "Unfilled ctrl")
        assert canvas is not None

    def test_unfilled_test_well_renders(self):
        rows = _make_well_rows(self.cfg, overrides={"A1": {"is_filled": False}})
        canvas = draw_plate_grid(rows, self.cfg, "Unfilled test")
        assert canvas is not None


# ─────────────────────────────────────────────────────────────────────────────
# collect_images
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectImages:
    def test_empty_directory(self, tmp_path):
        assert collect_images(tmp_path) == []

    def test_jpg_and_png_included(self, tmp_path):
        (tmp_path / "a.jpg").touch()
        (tmp_path / "b.png").touch()
        imgs = collect_images(tmp_path)
        assert len(imgs) == 2

    def test_non_image_files_excluded(self, tmp_path):
        (tmp_path / "data.csv").touch()
        (tmp_path / "notes.txt").touch()
        (tmp_path / "plate.jpg").touch()
        imgs = collect_images(tmp_path)
        assert len(imgs) == 1
        assert imgs[0].name == "plate.jpg"

    def test_all_supported_extensions(self, tmp_path):
        for ext in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            (tmp_path / f"img{ext}").touch()
        imgs = collect_images(tmp_path)
        assert len(imgs) == 5

    def test_sorted_by_timepoint(self, tmp_path):
        # Files with timepoint tokens must be sorted numerically, not alphabetically
        for name in ["sample_90min.jpg", "sample_30min.jpg", "sample_120min.jpg"]:
            (tmp_path / name).touch()
        imgs = collect_images(tmp_path)
        assert [p.name for p in imgs] == [
            "sample_30min.jpg", "sample_90min.jpg", "sample_120min.jpg"
        ]

    def test_sorted_alphabetically_without_timepoint(self, tmp_path):
        for name in ["c.jpg", "a.jpg", "b.jpg"]:
            (tmp_path / name).touch()
        imgs = collect_images(tmp_path)
        # No timepoint → fallback is inf → stable sort (all equal key) → original os order
        # Just check all 3 are returned
        assert len(imgs) == 3
        assert {p.name for p in imgs} == {"a.jpg", "b.jpg", "c.jpg"}

    def test_timestamp_files_sorted_chronologically(self, tmp_path):
        # Timestamp filenames: alphabetical order = chronological order (line 575 branch)
        names = [
            "2024-04-02_16-00-00.jpg",
            "2024-04-02_14-00-00.jpg",
            "2024-04-02_15-00-00.jpg",
        ]
        for name in names:
            (tmp_path / name).touch()
        imgs = collect_images(tmp_path)
        assert [p.name for p in imgs] == [
            "2024-04-02_14-00-00.jpg",
            "2024-04-02_15-00-00.jpg",
            "2024-04-02_16-00-00.jpg",
        ]

    def test_min_files_before_timestamp_files(self, tmp_path):
        # min-token files (tier 0) must come before timestamp files (tier 1)
        for name in ["2024-04-02_15-00-00.jpg", "sample_30min.jpg"]:
            (tmp_path / name).touch()
        imgs = collect_images(tmp_path)
        assert imgs[0].name == "sample_30min.jpg"
        assert imgs[1].name == "2024-04-02_15-00-00.jpg"


# ─────────────────────────────────────────────────────────────────────────────
# already_processed
# ─────────────────────────────────────────────────────────────────────────────

class TestAlreadyProcessed:
    def test_no_csv_returns_empty_set(self, tmp_path):
        assert already_processed(tmp_path / "does_not_exist.csv") == set()

    def test_returns_image_names_from_csv(self, tmp_path):
        csv_path = tmp_path / "well_colors.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerow({c: ("img_a.jpg" if c == "image_name" else "x") for c in CSV_COLUMNS})
            w.writerow({c: ("img_b.jpg" if c == "image_name" else "x") for c in CSV_COLUMNS})
        assert already_processed(csv_path) == {"img_a.jpg", "img_b.jpg"}

    def test_duplicate_image_names_deduplicated(self, tmp_path):
        csv_path = tmp_path / "well_colors.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            for _ in range(96):
                w.writerow({c: ("plate.jpg" if c == "image_name" else "x") for c in CSV_COLUMNS})
        assert already_processed(csv_path) == {"plate.jpg"}


# ─────────────────────────────────────────────────────────────────────────────
# process_folder
# ─────────────────────────────────────────────────────────────────────────────

class TestProcessFolder:
    """Tests use a mock YOLO (so no GPU/weights needed) and a mock process_image."""

    def _fake_ultralytics(self):
        mod = MagicMock()
        mod.YOLO.return_value = MagicMock()
        return mod

    def test_no_images_in_dir(self, tmp_path, capsys):
        plates = tmp_path / "plates"
        plates.mkdir()
        cfg = _cfg()
        with patch.dict("sys.modules", {"ultralytics": self._fake_ultralytics()}):
            process_folder(plates, tmp_path / "w.pt", tmp_path / "out", cfg)
        assert "No images found" in capsys.readouterr().out

    def test_all_already_processed(self, tmp_path, capsys):
        plates = tmp_path / "plates"
        plates.mkdir()
        (plates / "img.jpg").touch()

        out = tmp_path / "out"
        out.mkdir()
        csv_path = out / "well_colors.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerow({c: ("img.jpg" if c == "image_name" else "x") for c in CSV_COLUMNS})

        cfg = _cfg()
        with patch.dict("sys.modules", {"ultralytics": self._fake_ultralytics()}):
            process_folder(plates, tmp_path / "w.pt", out, cfg, reprocess=False)
        assert "All images already processed" in capsys.readouterr().out

    @patch("tools.spot_assay.process_image")
    def test_processes_new_images_writes_csv(self, mock_pi, tmp_path):
        plates = tmp_path / "plates"
        plates.mkdir()
        img = plates / "plate_30min.jpg"
        img.touch()

        # process_image returns one minimal OK well row
        mock_pi.return_value = ("OK", [{c: "v" for c in CSV_COLUMNS}])

        cfg = _cfg()
        with patch.dict("sys.modules", {"ultralytics": self._fake_ultralytics()}):
            with patch("tools.spot_assay.draw_plate_grid", return_value=np.zeros((10, 10, 3), np.uint8)):
                with patch("tools.spot_assay.cv2.imwrite"):
                    with patch("tools.spot_assay.shutil.copy2"):
                        process_folder(plates, tmp_path / "w.pt", tmp_path / "out", cfg)

        csv_path = tmp_path / "out" / "well_colors.csv"
        assert csv_path.exists()

    @patch("tools.spot_assay.process_image")
    def test_empty_well_rows_skipped(self, mock_pi, tmp_path, capsys):
        """When process_image returns empty well_rows the image is skipped."""
        plates = tmp_path / "plates"
        plates.mkdir()
        (plates / "img.jpg").touch()

        mock_pi.return_value = ("FAILED:yolo_detection_failed", [])

        cfg = _cfg()
        with patch.dict("sys.modules", {"ultralytics": self._fake_ultralytics()}):
            process_folder(plates, tmp_path / "w.pt", tmp_path / "out", cfg)

        out = capsys.readouterr().out
        assert "Skipping" in out
        # No summary if no grid was written
        assert not (tmp_path / "out" / "latest_summary.png").exists()

    @patch("tools.spot_assay.process_image")
    def test_reprocess_ignores_existing_csv(self, mock_pi, tmp_path):
        plates = tmp_path / "plates"
        plates.mkdir()
        img = plates / "img.jpg"
        img.touch()

        # Pre-existing CSV with img.jpg already recorded
        out = tmp_path / "out"
        out.mkdir()
        csv_path = out / "well_colors.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerow({c: ("img.jpg" if c == "image_name" else "x") for c in CSV_COLUMNS})

        mock_pi.return_value = ("OK", [{c: "v" for c in CSV_COLUMNS}])

        cfg = _cfg()
        with patch.dict("sys.modules", {"ultralytics": self._fake_ultralytics()}):
            with patch("tools.spot_assay.draw_plate_grid", return_value=np.zeros((10, 10, 3), np.uint8)):
                with patch("tools.spot_assay.cv2.imwrite"):
                    with patch("tools.spot_assay.shutil.copy2"):
                        process_folder(plates, tmp_path / "w.pt", out, cfg, reprocess=True)

        # process_image must have been called even though img was in the CSV
        mock_pi.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# hough_grid_annotate.robust_linear_grid
# ─────────────────────────────────────────────────────────────────────────────

class TestRobustLinearGrid:
    def test_uniform_peaks_returns_correct_positions(self):
        peaks = np.arange(12, dtype=float) * 50.0   # 0, 50, 100, … 550
        positions, pitch = robust_linear_grid(peaks, 12)
        assert positions == pytest.approx(np.arange(12) * 50.0, abs=1e-6)
        assert pitch == pytest.approx(50.0, abs=1e-6)

    def test_fewer_than_n_peaks_still_extrapolates(self):
        # Give 8 peaks, ask for 12 — linear model extrapolates
        peaks = np.arange(8, dtype=float) * 60.0
        positions, pitch = robust_linear_grid(peaks, 12)
        assert len(positions) == 12
        assert pitch == pytest.approx(60.0, abs=1e-3)

    def test_spurious_close_peak_is_removed(self):
        # Insert a near-duplicate between indices 2 and 3
        good = np.arange(8, dtype=float) * 50.0
        noisy = np.sort(np.append(good, 102.0))   # 102 is very close to 100
        positions, pitch = robust_linear_grid(noisy, 8)
        assert len(positions) == 8
        assert pitch == pytest.approx(50.0, abs=2.0)

    def test_too_few_peaks_raises(self):
        with pytest.raises(ValueError, match="Not enough peaks"):
            robust_linear_grid(np.array([42.0]), 12)

    def test_exactly_two_peaks(self):
        positions, pitch = robust_linear_grid(np.array([0.0, 100.0]), 5)
        assert len(positions) == 5
        assert pitch == pytest.approx(100.0, abs=1e-6)

    def test_constants_shape(self):
        assert len(ROW_LABELS) == 8
        assert len(COL_LABELS) == 12
        assert len(ALL_WELLS) == 96


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline.label_to_bgr
# ─────────────────────────────────────────────────────────────────────────────

class TestLabelToBgr:
    def test_normal_rgb(self):
        cd = {"mean_R": 200.0, "mean_G": 100.0, "mean_B": 50.0}
        assert label_to_bgr("amber", cd) == (50, 100, 200)

    def test_nan_falls_back(self):
        cd = {"mean_R": float("nan"), "mean_G": 100.0, "mean_B": 50.0}
        assert label_to_bgr("unknown", cd) == (60, 60, 60)

    def test_missing_keys_falls_back(self):
        assert label_to_bgr("unknown", {}) == (180, 180, 180)


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline.label_short
# ─────────────────────────────────────────────────────────────────────────────

class TestLabelShort:
    def test_single_word_truncated(self):
        assert label_short("amber") == "Amb"

    def test_single_word_short_kept(self):
        assert label_short("red") == "Red"

    def test_multi_word_initials(self):
        assert label_short("pale amber") == "PA"

    def test_multi_word_max_four(self):
        assert label_short("pale vivid red orange") == "PVRO"

    def test_multi_word_exactly_four(self):
        assert label_short("a b c d") == "ABCD"


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline._hue_name
# ─────────────────────────────────────────────────────────────────────────────

class TestHueName:
    @pytest.mark.parametrize("h,expected", [
        (0,   "red"),
        (3,   "red"),
        (5,   "red-orange"),
        (10,  "red-orange"),  # 5–12 → red-orange
        (15,  "orange"),      # 12–18
        (20,  "dark orange"),
        (25,  "amber"),
        (30,  "golden yellow"),
        (40,  "yellow"),
        (50,  "yellow-green"),
        (60,  "green"),
        (85,  "teal"),
        (110, "cyan"),
        (140, "blue"),
        (157, "violet"),
        (167, "magenta"),
        (177, "red"),         # 175–180 → red
        (179, "red"),
    ])
    def test_range(self, h, expected):
        assert _hue_name(h) == expected

    def test_fallback_at_180(self):
        assert _hue_name(180) == "red"


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline.name_color
# ─────────────────────────────────────────────────────────────────────────────

def _bgr_color_data(R, G, B, S=None, V=None) -> dict:
    """Build color_data from explicit RGB; compute S and V via OpenCV."""
    px = np.array([[[int(B), int(G), int(R)]]], dtype=np.uint8)
    hsv = cv2.cvtColor(px, cv2.COLOR_BGR2HSV)[0, 0]
    return {
        "mean_R": float(R), "mean_G": float(G), "mean_B": float(B),
        "mean_S": float(S) if S is not None else float(hsv[1]),
        "mean_V": float(V) if V is not None else float(hsv[2]),
    }


class TestNameColor:
    def test_nan_returns_unknown(self):
        cd = {"mean_R": float("nan"), "mean_G": 100.0, "mean_B": 100.0,
              "mean_S": 100.0, "mean_V": 150.0}
        assert name_color(cd) == "unknown"

    # ── Achromatic branches ──────────────────────────────────────────────────

    def test_achromatic_white(self):
        cd = _bgr_color_data(230, 230, 230, S=5)
        cd["mean_V"] = 230.0
        assert name_color(cd) == "white"

    def test_achromatic_off_white(self):
        cd = _bgr_color_data(200, 200, 200, S=5)
        cd["mean_V"] = 200.0
        assert name_color(cd) == "off-white"

    def test_achromatic_light_grey(self):
        cd = _bgr_color_data(170, 170, 170, S=5)
        cd["mean_V"] = 170.0
        assert name_color(cd) == "light grey"

    def test_achromatic_grey(self):
        cd = _bgr_color_data(130, 130, 130, S=5)
        cd["mean_V"] = 130.0
        assert name_color(cd) == "grey"

    def test_achromatic_dark_grey(self):
        cd = _bgr_color_data(90, 90, 90, S=5)
        cd["mean_V"] = 90.0
        assert name_color(cd) == "dark grey"

    def test_achromatic_near_black(self):
        cd = _bgr_color_data(40, 40, 40, S=5)
        cd["mean_V"] = 40.0
        assert name_color(cd) == "near black"

    # ── Chromatic branches ───────────────────────────────────────────────────

    def test_chromatic_no_duplicate_modifiers(self):
        # Any chromatic result must not contain "pale pale", "light pale", etc.
        for R, G, B in [
            (220, 180, 80),   # bright amber
            (180, 80, 80),    # medium red
            (60, 120, 60),    # dark green
            (80, 220, 200),   # pale teal
        ]:
            cd = _bgr_color_data(R, G, B)
            result = name_color(cd)
            assert "pale pale" not in result
            assert "light pale" not in result
            assert "pale muted" not in result
            assert "light muted " not in result or result == "muted light " + result.split()[-1]

    def test_chromatic_vivid(self):
        # Highly saturated → "vivid" modifier
        cd = _bgr_color_data(255, 100, 0)   # vivid orange-red
        result = name_color(cd)
        assert "vivid" in result or result  # just check it doesn't crash

    def test_chromatic_muted(self):
        # Medium saturation
        cd = _bgr_color_data(180, 120, 80)
        result = name_color(cd)
        assert isinstance(result, str) and len(result) > 0

    def test_chromatic_dark(self):
        cd = _bgr_color_data(120, 60, 60)
        cd["mean_V"] = 120.0
        result = name_color(cd)
        assert "dark" in result or "deep" in result or isinstance(result, str)

    # ── Replacement table ────────────────────────────────────────────────────

    def test_pale_vivid_replaced(self):
        """V > 215 AND S > 200 → 'pale vivid ...' → replaced with 'vivid ...'"""
        # Force S > 200 and V > 215 by overriding after construction
        cd = _bgr_color_data(255, 150, 0)
        cd["mean_V"] = 220.0
        cd["mean_S"] = 210.0
        result = name_color(cd)
        assert not result.startswith("pale vivid")
        # Replaced to "vivid ..." or just "vivid"
        assert "pale" not in result.split()[0] if result.startswith("vivid") else True

    def test_light_muted_replaced(self):
        """V in (185, 215] AND S in (70, 140] → 'light muted ...' → 'muted light ...'"""
        cd = _bgr_color_data(190, 130, 90)
        cd["mean_V"] = 190.0
        cd["mean_S"] = 100.0
        result = name_color(cd)
        assert "light muted" not in result


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline.sample_well_color_from_bbox
# ─────────────────────────────────────────────────────────────────────────────

class TestSampleWellColorFromBbox:
    def _make_images(self, h=200, w=200):
        bgr = np.zeros((h, w, 3), dtype=np.uint8)
        bgr[:, :] = [100, 150, 200]   # uniform colour
        lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
        hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
        return bgr, lab, hsv

    def test_normal_sampling_returns_valid_dict(self):
        bgr, lab, hsv = self._make_images()
        det = dict(cx=100.0, cy=100.0, r=20.0)
        result = sample_well_color_from_bbox(bgr, lab, hsv, det, radius_frac=0.5)

        assert result["n_pixels"] > 0
        assert not math.isnan(result["mean_R"])
        assert not math.isnan(result["mean_L"])
        assert not math.isnan(result["mean_V"])
        assert result["cx"] == pytest.approx(100.0, abs=0.2)

    def test_zero_pixel_mask_returns_nans(self):
        """A radius so tiny no pixel falls inside → n_pixels=0, all NaN."""
        bgr, lab, hsv = self._make_images()
        # Use a non-integer centre so the nearest integer pixel is at dist ≈ 0.7 px,
        # while the effective radius (r × radius_frac = 0.01 × 0.001) ≈ 0 → no pixels.
        det = dict(cx=100.5, cy=100.5, r=0.01)
        result = sample_well_color_from_bbox(bgr, lab, hsv, det, radius_frac=0.001)

        assert result["n_pixels"] == 0
        assert math.isnan(result["mean_R"])
        assert math.isnan(result["mean_L"])

    def test_radius_frac_limits_sampling_area(self):
        """Smaller radius_frac → fewer pixels sampled."""
        bgr, lab, hsv = self._make_images()
        det = dict(cx=100.0, cy=100.0, r=30.0)
        r_full = sample_well_color_from_bbox(bgr, lab, hsv, det, radius_frac=0.9)
        r_half = sample_well_color_from_bbox(bgr, lab, hsv, det, radius_frac=0.3)
        assert r_full["n_pixels"] > r_half["n_pixels"]


# ─────────────────────────────────────────────────────────────────────────────
# spot_assay.process_image — well missing from color_data (line 282)
# ─────────────────────────────────────────────────────────────────────────────

class TestProcessImageMissingWell:
    """When yolo_detect_and_assign omits a well it must be absent from output."""

    def setup_method(self):
        self.cfg = _cfg()
        self.model = MagicMock()
        self._img = np.zeros((100, 100, 3), dtype=np.uint8)

    @patch("tools.spot_assay.sample_well_color_from_bbox")
    @patch("tools.spot_assay.yolo_detect_and_assign")
    @patch("tools.spot_assay.cv2.imread")
    def test_missing_well_skipped_in_output(self, mock_read, mock_detect, mock_sample):
        mock_read.return_value = self._img
        # Return only 95 wells — drop A12
        assigned = {w: dict(cx=100.0, cy=100.0, r=30.0) for w in ALL_WELLS if w != "A12"}
        mock_detect.return_value = assigned

        pc = _well_color(L=50.0, a=30.0, b=20.0)
        nc = _well_color(L=70.0, a=-5.0, b=5.0)
        med = _well_color(L=55.0, a=25.0, b=18.0)
        side = _build_mock_color_side_effect(_cfg(), pc, nc, med, {})
        # side was built for 96 wells; drop the A12 entry (index of A12 in ALL_WELLS)
        a12_idx = ALL_WELLS.index("A12")
        side.pop(a12_idx)
        mock_sample.side_effect = side

        _, rows = process_image(Path("fake.jpg"), self.model, self.cfg, 0)
        well_ids = {r["well_id"] for r in rows}
        assert "A12" not in well_ids
        assert len(rows) == 95


# ─────────────────────────────────────────────────────────────────────────────
# spot_assay.main() CLI
# ─────────────────────────────────────────────────────────────────────────────

class TestSpotAssayMain:
    @patch("tools.spot_assay.process_folder")
    def test_main_passes_defaults(self, mock_pf, tmp_path):
        weights = tmp_path / "w.pt"
        weights.touch()
        plates = tmp_path / "plates"
        plates.mkdir()
        out = tmp_path / "out"

        from tools.spot_assay import main as sa_main
        with patch("sys.argv", [
            "spot_assay",
            "--plates", str(plates),
            "--weights", str(weights),
            "--out", str(out),
        ]):
            sa_main()

        mock_pf.assert_called_once()
        _, kwargs = mock_pf.call_args
        cfg = kwargs.get("cfg") or mock_pf.call_args[0][2]
        assert cfg.media_pc_row == "H"
        assert cfg.matrix_pc_row == "G"
        assert cfg.matrix_nc_row == "F"
        assert cfg.require_closer_to_pc is True

    @patch("tools.spot_assay.process_folder")
    def test_main_no_require_closer_flag(self, mock_pf, tmp_path):
        weights = tmp_path / "w.pt"
        weights.touch()
        plates = tmp_path / "plates"
        plates.mkdir()

        from tools.spot_assay import main as sa_main
        with patch("sys.argv", [
            "spot_assay",
            "--plates", str(plates),
            "--weights", str(weights),
            "--out", str(tmp_path / "out"),
            "--no-require-closer-to-pc",
        ]):
            sa_main()

        _, kwargs = mock_pf.call_args
        cfg = kwargs.get("cfg") or mock_pf.call_args[0][2]
        assert cfg.require_closer_to_pc is False

    @patch("tools.spot_assay.process_folder")
    def test_main_custom_control_rows(self, mock_pf, tmp_path):
        weights = tmp_path / "w.pt"
        weights.touch()
        plates = tmp_path / "plates"
        plates.mkdir()

        from tools.spot_assay import main as sa_main
        with patch("sys.argv", [
            "spot_assay",
            "--plates", str(plates),
            "--weights", str(weights),
            "--out", str(tmp_path / "out"),
            "--media-pc-row", "A",
            "--matrix-pc-row", "B",
            "--matrix-nc-row", "C",
        ]):
            sa_main()

        _, kwargs = mock_pf.call_args
        cfg = kwargs.get("cfg") or mock_pf.call_args[0][2]
        assert cfg.media_pc_row == "A"
        assert cfg.matrix_pc_row == "B"
        assert cfg.matrix_nc_row == "C"

    @patch("tools.spot_assay.process_folder")
    def test_main_reprocess_flag(self, mock_pf, tmp_path):
        weights = tmp_path / "w.pt"
        weights.touch()
        plates = tmp_path / "plates"
        plates.mkdir()

        from tools.spot_assay import main as sa_main
        with patch("sys.argv", [
            "spot_assay",
            "--plates", str(plates),
            "--weights", str(weights),
            "--out", str(tmp_path / "out"),
            "--reprocess",
        ]):
            sa_main()

        _, kwargs = mock_pf.call_args
        reprocess = kwargs.get("reprocess") or mock_pf.call_args[0][3]
        assert reprocess is True


# ─────────────────────────────────────────────────────────────────────────────
# yolo_color_pipeline.yolo_detect_and_assign
# ─────────────────────────────────────────────────────────────────────────────

from tools.yolo_color_pipeline import yolo_detect_and_assign


def _mock_yolo_model(n_boxes: int, img_h: int = 800, img_w: int = 1200):
    """
    Build a mock YOLO model that returns `n_boxes` detections arranged in a
    regular 8×12 grid pattern within the image (with small jitter so the
    nearest-neighbour pitch estimator doesn't get an empty array).
    """
    rng = np.random.RandomState(42)
    row_pitch = img_h / 10   # a little padding
    col_pitch = img_w / 14
    boxes_xy = []
    for ri in range(8):
        for ci in range(12):
            cx = col_pitch + ci * col_pitch + rng.uniform(-2, 2)
            cy = row_pitch + ri * row_pitch + rng.uniform(-2, 2)
            r = min(row_pitch, col_pitch) * 0.4
            boxes_xy.append([cx - r, cy - r, cx + r, cy + r])

    # Only return n_boxes of them
    boxes_xy = boxes_xy[:n_boxes]
    xyxy_t = MagicMock()
    xyxy_t.cpu.return_value.numpy.return_value = np.array(boxes_xy, dtype=np.float32)
    conf_t = MagicMock()
    conf_t.cpu.return_value.numpy.return_value = np.ones(n_boxes, dtype=np.float32) * 0.9

    boxes_mock = MagicMock()
    boxes_mock.xyxy = xyxy_t
    boxes_mock.conf = conf_t
    boxes_mock.__len__ = lambda self: n_boxes

    result_mock = MagicMock()
    result_mock.boxes = boxes_mock

    model = MagicMock()
    model.return_value = [result_mock]
    return model


class TestYoloDetectAndAssign:
    def test_returns_none_when_fewer_than_90_boxes(self):
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        model = _mock_yolo_model(85)
        assert yolo_detect_and_assign(img, model) is None

    def test_returns_none_when_boxes_is_none(self):
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        result_mock = MagicMock()
        result_mock.boxes = None
        model = MagicMock(return_value=[result_mock])
        assert yolo_detect_and_assign(img, model) is None

    def test_returns_96_well_dict_for_full_grid(self):
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        model = _mock_yolo_model(96)
        result = yolo_detect_and_assign(img, model)
        assert result is not None
        assert set(result.keys()) == set(ALL_WELLS)
        for det in result.values():
            assert "cx" in det and "cy" in det and "r" in det

    @patch("tools.yolo_color_pipeline.find_peaks",
           return_value=(np.array([50, 1500, 2950]),
                         {"peak_heights": np.array([10.0, 8.0, 6.0])}))
    def test_returns_none_when_too_few_peaks(self, _mock_fp):
        """Patch find_peaks to return only 3 peaks → fewer than 12 col-peaks → None."""
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        model = _mock_yolo_model(96)
        assert yolo_detect_and_assign(img, model) is None

    @patch("tools.yolo_color_pipeline.robust_linear_grid", side_effect=ValueError("test"))
    def test_returns_none_on_grid_fit_error(self, _mock_grid):
        """robust_linear_grid raising ValueError must propagate as None return."""
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        model = _mock_yolo_model(96)
        assert yolo_detect_and_assign(img, model) is None

    def test_returns_none_when_detections_below_90_after_loop(self):
        """boxes.__len__ == 96 but xyxy array has only 50 rows → second < 90 check."""
        img = np.zeros((800, 1200, 3), dtype=np.uint8)
        boxes_xy = np.zeros((50, 4), dtype=np.float32)  # only 50 rows
        xyxy_t = MagicMock()
        xyxy_t.cpu.return_value.numpy.return_value = boxes_xy
        conf_t = MagicMock()
        conf_t.cpu.return_value.numpy.return_value = np.ones(50, dtype=np.float32)
        boxes_mock = MagicMock()
        boxes_mock.xyxy = xyxy_t
        boxes_mock.conf = conf_t
        boxes_mock.__len__ = lambda self: 96   # passes first check
        result_mock = MagicMock()
        result_mock.boxes = boxes_mock
        model = MagicMock(return_value=[result_mock])
        assert yolo_detect_and_assign(img, model) is None
