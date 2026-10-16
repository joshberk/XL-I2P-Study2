"""Export-pending: closed epochs get exported no matter who rolled them.

Regression: `epoch rollover --export-closed` only exported the epoch that
*this invocation* had just closed. The crawler's in-loop backstop rolls the
epoch within one cycle of it turning due, so the daily VM2 timer found a
fresh (age-0) epoch, printed "not due", and exported nothing — closed
epochs were never exported.
"""
from __future__ import annotations

import dataclasses
import json
from datetime import timedelta

import pytest
from typer.testing import CliRunner

import xl_i2p.epochs as epochs_mod
from xl_i2p.cli import app
from xl_i2p.config import Settings
from xl_i2p.epochs import close_epoch, open_epoch, rollover_if_due
from xl_i2p.export import exported_epoch_labels, export_pending_epochs
from xl_i2p.models import now


@pytest.fixture()
def auto_settings(monkeypatch):
    s = dataclasses.replace(Settings(), epoch_duration_days=30, epoch_auto_rollover=True)
    monkeypatch.setattr(epochs_mod, "settings", s)
    return s


def _manifest_labels(root):
    return sorted(
        json.loads(p.read_text())["epoch_label"]
        for p in root.glob("epoch_*/manifest.json")
    )


def test_exports_closed_epochs_only_once(db_session, tmp_path):
    s = db_session
    open_epoch(s, "e1")
    close_epoch(s, "e1")
    open_epoch(s, "e2")  # still OPEN -> must not be exported

    written, failed = export_pending_epochs(s, tmp_path)
    assert failed == []
    assert len(written) == 1
    assert _manifest_labels(tmp_path) == ["e1"]

    # Idempotent: the second run finds e1 already exported.
    written, failed = export_pending_epochs(s, tmp_path)
    assert written == [] and failed == []
    assert _manifest_labels(tmp_path) == ["e1"]


def test_crashed_export_without_manifest_is_retried(db_session, tmp_path):
    s = db_session
    open_epoch(s, "e1")
    close_epoch(s, "e1")
    (tmp_path / "epoch_e1_20261101T000000Z").mkdir()  # partial dir, no manifest

    assert exported_epoch_labels(tmp_path) == set()
    written, _ = export_pending_epochs(s, tmp_path)
    assert len(written) == 1
    assert _manifest_labels(tmp_path) == ["e1"]


def test_labels_come_from_manifest_not_dir_name(db_session, tmp_path):
    """Label 'a' must not be satisfied by an export of label 'a_b'."""
    s = db_session
    for label in ("a_b", "a"):
        open_epoch(s, label)
        close_epoch(s, label)
    export_pending_epochs(s, tmp_path)  # exports both
    assert _manifest_labels(tmp_path) == ["a", "a_b"]

    # Remove only a's export; a_b's dir (epoch_a_b_...) must not hide it.
    for mf in tmp_path.glob("epoch_*/manifest.json"):
        if json.loads(mf.read_text())["epoch_label"] == "a":
            mf.unlink()
    assert exported_epoch_labels(tmp_path) == {"a_b"}


def test_one_failed_export_does_not_block_others(db_session, tmp_path, monkeypatch):
    import xl_i2p.export as export_mod

    s = db_session
    for label in ("e1", "e2"):
        open_epoch(s, label)
        close_epoch(s, label)
    real = export_mod.export_epoch

    def flaky(session, label, export_dir=None):
        if label == "e1":
            raise OSError("disk full")
        return real(session, label, export_dir)

    monkeypatch.setattr(export_mod, "export_epoch", flaky)
    written, failed = export_pending_epochs(s, tmp_path)
    assert failed == ["e1"]
    assert _manifest_labels(tmp_path) == ["e2"]


def test_rollover_cli_exports_epoch_rolled_by_crawler_backstop(
        db_session, tmp_path, auto_settings):
    """The exact production sequence: crawler rolls first, timer runs later."""
    s = db_session
    old = open_epoch(s, "2026-Q4")
    old.started_at = now() - timedelta(days=31)
    s.commit()
    # Crawler backstop wins the race (rolls without exporting).
    rollover_if_due(s)
    s.expire_all()

    result = CliRunner().invoke(
        app, ["epoch", "rollover", "--export-closed", "--out-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "not due" in result.output  # the new epoch is age 0
    assert _manifest_labels(tmp_path) == ["2026-Q4"]


def test_rollover_cli_exit_nonzero_when_export_fails(
        db_session, tmp_path, monkeypatch):
    import xl_i2p.export as export_mod

    s = db_session
    open_epoch(s, "e1")
    close_epoch(s, "e1")
    monkeypatch.setattr(
        export_mod, "export_epoch",
        lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))

    result = CliRunner().invoke(
        app, ["epoch", "rollover", "--export-closed", "--out-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "e1" in result.output
