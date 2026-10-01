"""Tests for local_netdb_path(): I2P_NETDB_DIR override and unreadable candidates.

No real router needed: uses tmp dirs, a stubbed Path, and monkeypatched settings.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import xl_i2p.cross_layer as xl_mod


def _settings_with(netdb_dir: str, monkeypatch):
    monkeypatch.setattr(xl_mod, "settings", replace(xl_mod.settings, i2p_netdb_dir=netdb_dir))


def test_netdb_dir_override_wins(monkeypatch, tmp_path):
    override = tmp_path / "custom" / "netDb"
    override.mkdir(parents=True)
    _settings_with(str(override), monkeypatch)
    assert xl_mod.local_netdb_path() == override


def test_netdb_dir_override_blank_falls_back(monkeypatch, tmp_path):
    home_netdb = tmp_path / "home" / ".i2p" / "netDb"
    home_netdb.mkdir(parents=True)
    _settings_with("   ", monkeypatch)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    assert xl_mod.local_netdb_path() == home_netdb


def test_unreadable_candidate_skipped_not_raised(monkeypatch):
    seen = []

    class Cand:
        def __init__(self, s):
            self.s = s

        def exists(self):
            seen.append(self.s)
            raise PermissionError(13, "Permission denied")

        def __truediv__(self, other):
            return Cand(self.s + "/" + other)

    class FakePath:
        def __call__(self, s):
            return Cand(s)

        def home(self):
            return Cand("/fake/home")

    monkeypatch.setattr(xl_mod, "Path", FakePath())
    _settings_with("", monkeypatch)
    assert xl_mod.local_netdb_path() is None
    assert seen  # candidates were tried and skipped, not raised
