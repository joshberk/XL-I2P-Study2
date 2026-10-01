"""Standalone netDb binary parsing for the VPS floodfill harvester.

Vendored from ``xl_i2p/cross_layer.py::parse_routerinfo_bytes`` plus
LeaseSet parsing; kept dependency-free (stdlib only) because the VPS sensor
does NOT have the xl_i2p package installed. Do not import xl_i2p here.

Byte-layout assumptions (documented for the dissertation appendix):
- Router identity (classic 2048-bit ElGamal/DSA): 256-byte public key,
  128-byte signing public key, 1-byte cert type, 2-byte big-endian cert
  length, then the cert payload. identity_end = 387 + cert_len.
  (Newer EdDSA identities have different key sizes; the parser does not
  assume them and falls back to metadata-only extraction.)
- Router published timestamp: 8-byte big-endian integer immediately after
  the identity, milliseconds since Unix epoch (Java Date.getTime()).
  Accepted only within [2020-01-01, now+7d]; otherwise None.
- Router options (``router.version=``, ``caps=``) appear as ASCII ``k=v``
  pairs later in the blob; extracted with regexes on a best-effort basis.
  A ``caps`` value containing ``f``/``F`` marks a floodfill router.
- LeaseSet files (``leaseSet-<i2p-b64>.dat``): the destination hash is
  ground truth from the filename (it IS the SHA-256 of the destination in
  I2P base64). Expiry is heuristic: the maximum 8-byte big-endian
  millisecond timestamp found in the trailing 1 KiB that falls within
  (now-2h, now+24h]; leases in I2P expire on the order of minutes, so a
  timestamp far outside that window is not an expiry. May be None.

The filename hash is always ground truth for identity; everything parsed
from the binary is auxiliary metadata. All functions are defensive and
never raise on malformed input.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import re
import struct
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ROUTERINFO_RE = re.compile(r"routerInfo-(.+)\.dat$")
LEASESET_RE = re.compile(r"leaseSet-(.+)\.dat$")


def decode_i2p_b64(value: str) -> bytes:
    """Decode I2P base64 (uses - and ~ instead of + and /)."""
    try:
        value = value.replace("-", "+").replace("~", "/")
        return base64.b64decode(value + "=" * ((4 - len(value) % 4) % 4))
    except Exception:
        return b""


def i2p_b64_to_b32_host(b64hash: str) -> str | None:
    """Convert an I2P-base64 destination hash to its .b32.i2p hostname."""
    raw = decode_i2p_b64(b64hash)
    if len(raw) != 32:
        return None
    return base64.b32encode(raw).decode().lower().rstrip("=") + ".b32.i2p"


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def parse_routerinfo(data: bytes) -> dict:
    """Best-effort parse of a routerInfo blob. Never raises."""
    info: dict = {
        "version": None, "caps": None, "floodfill": False, "published_at": None,
    }
    try:
        if not data or len(data) < 400:
            return info
        text = data.decode("latin-1")
        match = re.search(r"router\.version=([0-9][0-9A-Za-z.\-]*)", text)
        if match:
            info["version"] = match.group(1)[:32]
        match = re.search(r"caps=([A-Za-z]+)", text)
        if match:
            caps = match.group(1)[:16]
            info["caps"] = caps
            info["floodfill"] = "f" in caps or "F" in caps
        cert_len = struct.unpack(">H", data[385:387])[0]
        ident_end = 387 + cert_len
        if ident_end + 8 <= len(data):
            ts_ms = struct.unpack(">Q", data[ident_end:ident_end + 8])[0]
            lo = 1577836800000  # 2020-01-01 UTC
            hi = _now_ms() + 7 * 86400 * 1000
            if lo <= ts_ms <= hi:
                info["published_at"] = datetime.fromtimestamp(
                    ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        logger.debug("routerInfo parse failed defensively", exc_info=True)
    return info


def parse_leaseset(data: bytes) -> dict:
    """Best-effort parse of a LeaseSet blob. Never raises.

    Returns the heuristic expiry (ISO UTC) or None. Lease entries end with
    an 8-byte millisecond Date; we scan the trailing 1 KiB for the largest
    plausible timestamp.
    """
    info: dict = {"lease_expiry": None}
    try:
        if not data or len(data) < 16:
            return info
        tail = data[-1024:]
        best: int | None = None
        now_ms = _now_ms()
        lo, hi = now_ms - 2 * 3600 * 1000, now_ms + 24 * 3600 * 1000
        for offset in range(0, len(tail) - 7):
            ts_ms = struct.unpack(">Q", tail[offset:offset + 8])[0]
            if lo <= ts_ms <= hi and (best is None or ts_ms > best):
                best = ts_ms
        if best is not None:
            info["lease_expiry"] = datetime.fromtimestamp(
                best / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        logger.debug("leaseSet parse failed defensively", exc_info=True)
    return info


def fingerprint(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]
