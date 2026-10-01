from __future__ import annotations

import base64
import csv
import hashlib
import json
import logging
import re
import socket
import struct
import subprocess
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .models import CrossLayerObservation, now
from .seeds import record_seed_event, upsert_site
from .states import SiteState
from .utils import extract_i2p_host, normalize_host

CONSOLE_URL = "http://127.0.0.1:7657"
SAM_HOST = "127.0.0.1"
SAM_PORT = 7656

logger = logging.getLogger(__name__)

# I2P base32 destination hashes are 32-byte SHA-256 digests encoded as
# unpadded base32: exactly 52 characters, followed optionally by .b32.i2p.
B32_RE = re.compile(r"^[a-z2-7]{52}(?:\.b32\.i2p)?$", re.IGNORECASE)
B32_IN_TEXT_RE = re.compile(r"([a-z2-7]{52}\.b32\.i2p)", re.IGNORECASE)


@dataclass(frozen=True)
class CrossLayerResult:
    input_value: str
    host: str | None
    canonical_b32: str | None
    lookup_method: str
    leaseset_found: bool
    leaseset_hash: str | None = None
    leaseset_type: str | None = None
    routing_key: str | None = None
    published: str | None = None
    expires: str | None = None
    gateway_count: int = 0
    floodfill_count: int = 0
    confidence: str | None = None
    raw_error: str | None = None
    raw_summary: str | None = None
    console_template_version: str | None = None
    lease_parser_status: str | None = None


def clean_html(html: str) -> str:
    h = re.sub(r"&nbsp;", " ", html)
    h = re.sub(r"&#\d+;", "", h)
    h = re.sub(r"<[^>]+>", " ", h)
    return re.sub(r"\s{2,}", " ", h).strip()


def http_get_console(path: str, timeout: int = 10) -> str:
    try:
        req = urllib.request.Request(CONSOLE_URL + path, headers={"Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def decode_i2p_b64(value: str) -> bytes:
    try:
        value = value.replace("-", "+").replace("~", "/")
        return base64.b64decode(value + "=" * ((4 - len(value) % 4) % 4))
    except Exception:
        return b""


def derive_b32_from_destination(destination: str) -> str | None:
    data = decode_i2p_b64(destination)
    if len(data) < 387:
        return None
    try:
        cert_type = data[384]
        cert_len = struct.unpack(">H", data[385:387])[0]
        # Common/modern destinations are cert types 0 and 5. Types 1 and 2 are
        # deprecated but still parseable because the certificate length is explicit.
        if cert_type in {0, 1, 2, 5}:
            dest_size = 387 + cert_len
        else:
            logger.warning("Unsupported I2P destination certificate type: %s", cert_type)
            return None
        if dest_size > len(data):
            return None
        return base64.b32encode(hashlib.sha256(data[:dest_size]).digest()).decode().lower().rstrip("=") + ".b32.i2p"
    except Exception:
        return None


def canonicalize_input(value: str) -> tuple[str | None, str | None]:
    raw = value.strip()
    raw = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE).strip().rstrip("/")
    if B32_RE.match(raw):
        host = raw.lower()
        if not host.endswith(".b32.i2p"):
            host += ".b32.i2p"
        return host, host
    host = extract_i2p_host(raw)
    if host:
        return host, host if host.endswith(".b32.i2p") else None
    return None, None


def sam_alive() -> bool:
    try:
        with socket.create_connection((SAM_HOST, SAM_PORT), timeout=3) as sock:
            sock.sendall(b"HELLO VERSION MIN=3.0 MAX=3.3\n")
            reply = sock.recv(256).decode("utf-8", errors="ignore")
            return "RESULT=OK" in reply
    except Exception:
        return False


def sam_naming_lookup(name: str) -> str:
    try:
        with socket.create_connection((SAM_HOST, SAM_PORT), timeout=5) as sock:
            sock.sendall(b"HELLO VERSION MIN=3.0 MAX=3.3\n")
            buf = b""
            sock.settimeout(5)
            while b"\n" not in buf:
                buf += sock.recv(256)
            if b"RESULT=OK" not in buf:
                return ""
            sock.sendall(f"NAMING LOOKUP NAME={name}\n".encode())
            buf = b""
            sock.settimeout(15)
            while b"\n" not in buf:
                chunk = sock.recv(512)
                if not chunk:
                    break
                buf += chunk
        match = re.search(r"VALUE=(\S+)", buf.decode("utf-8", errors="ignore"))
        return match.group(1) if match else ""
    except Exception:
        return ""


def infer_console_template_version(html: str) -> str:
    """Return a stable parser fingerprint for detecting I2P console template drift.

    I2P console pages do not expose a formal template version, so this records
    either a detected router version or a short content fingerprint. If a later
    I2P upgrade changes the NetDB page and lease parsing drops to zero, this
    value helps identify that the HTML template changed rather than the target
    having no leases.
    """
    text = clean_html(html)
    match = re.search(r"router\.version\s*=\s*([0-9]+(?:\.[0-9]+){1,3})", text, flags=re.IGNORECASE)
    if match:
        return f"i2p-console-router-{match.group(1)}"
    digest = hashlib.sha256(html[:4096].encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"i2p-console-template-sha256-{digest}"


def parse_leaseset_html(html: str, query: str = "") -> dict[str, object]:
    text = clean_html(html)
    ls: dict[str, object] = {
        "b32": "",
        "destination": "",
        "leaseset_hash": "",
        "published": "",
        "expires": "",
        "leaseset_type": "",
        "routing_key": "",
        "leases": [],
        "console_template_version": infer_console_template_version(html),
        "lease_parser_status": "not_evaluated",
    }
    patterns = [
        (r"LeaseSet:\s*([A-Za-z0-9~=+/\-]{20,})", "leaseset_hash"),
        (r"Destination:\s*([A-Za-z0-9~=+/\-]{6,})", "destination"),
        (r"([a-z2-7]{52}\.b32\.i2p)", "b32"),
        (r"Published\s+(\d+\s+\w+\s+ago)", "published"),
        (r"Expires\s+in\s+([\d]+\s+\w+)", "expires"),
        (r"Type:\s*(\d+)", "leaseset_type"),
        (r"Routing Key:\s*([A-Za-z0-9~=+/\-]{20,})", "routing_key"),
    ]
    for pattern, key in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            ls[key] = match.group(1).strip()
    if not ls["b32"] and query.endswith(".b32.i2p"):
        ls["b32"] = query
    leases: list[dict[str, str]] = []
    for match in re.finditer(r"Lease\s+(\d+):\s*(?:[^A-Za-z0-9~=+/\-]*)([A-Za-z0-9~=+/\-]{4,8})\s+Tunnel\s+(\d+)(?:\s+Expires\s+in\s+([\d]+\s+\w+))?", text):
        leases.append({
            "num": match.group(1),
            "gateway_prefix": match.group(2),
            "tunnel_id": match.group(3),
            "expires": match.group(4) or "unknown",
        })
    ls["leases"] = leases
    has_leaseset_evidence = bool(ls.get("leaseset_hash") or ls.get("b32") or ls.get("destination"))
    if leases:
        ls["lease_parser_status"] = "leases_parsed"
    elif has_leaseset_evidence:
        ls["lease_parser_status"] = "no_leases_parsed_possible_template_drift"
    else:
        ls["lease_parser_status"] = "no_leaseset_evidence"
    return ls


def local_netdb_path() -> Path | None:
    override = settings.i2p_netdb_dir.strip()
    candidates = ([Path(override)] if override else []) + [
        Path("/var/lib/i2p/i2p-config/netDb"),
        Path.home() / ".i2p" / "netDb",
        Path("/root/.i2p/netDb"),
    ]
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except OSError:
            # Unreadable (e.g. another user's 0700 dir): skip, don't crash.
            continue
    return None


def load_floodfill_hashes(limit: int | None = None) -> list[str]:
    path = local_netdb_path()
    if not path:
        return []
    floodfills: list[str] = []
    for fp in path.rglob("routerInfo-*.dat"):
        try:
            result = subprocess.run(["strings", str(fp)], capture_output=True, text=True, timeout=5)
            if re.search(r"(^|\n)caps=\s*\n?[A-Za-z0-9]*f[A-Za-z0-9]*", result.stdout, flags=re.IGNORECASE):
                match = re.match(r"routerInfo-(.+)\.dat$", fp.name)
                if match:
                    floodfills.append(match.group(1))
        except Exception:
            continue
        if limit and len(floodfills) >= limit:
            break
    return floodfills


def compute_daily_mod_key() -> bytes:
    return hashlib.sha256(datetime.now(timezone.utc).strftime("%Y%m%d").encode()).digest()


def compute_routing_key_from_b32(b32: str) -> str | None:
    try:
        b32_part = b32.replace(".b32.i2p", "").upper()
        padding = (8 - len(b32_part) % 8) % 8
        dest_hash = base64.b32decode(b32_part + "=" * padding)
        if len(dest_hash) != 32:
            return None
        mod_key = compute_daily_mod_key()
        xorred = bytes(a ^ b for a, b in zip(dest_hash, mod_key))
        return base64.b64encode(hashlib.sha256(xorred).digest()).decode()
    except Exception:
        return None


def fetch_leaseset(canonical_b32: str) -> tuple[str, str]:
    encoded = urllib.parse.quote(canonical_b32)
    html = http_get_console(f"/netdb?ls={encoded}", timeout=10)
    if html and ("LeaseSet" in html or canonical_b32 in html):
        return html, "console_cache"
    if sam_alive():
        _ = sam_naming_lookup(canonical_b32)
        html = http_get_console(f"/netdb?ls={encoded}", timeout=10)
        if html and ("LeaseSet" in html or canonical_b32 in html):
            return html, "sam_triggered_console_cache"
    return "", "not_found"


def lookup_candidate(value: str, floodfill_hashes: list[str] | None = None) -> CrossLayerResult:
    host, b32 = canonicalize_input(value)
    if not host:
        return CrossLayerResult(value, None, None, "normalize", False, raw_error="no .i2p or .b32.i2p host found")

    method = "canonical_b32" if b32 else "sam_naming_lookup"
    if not b32 and host.endswith(".i2p"):
        dest = sam_naming_lookup(host)
        if dest:
            b32 = derive_b32_from_destination(dest)
            method = "sam_naming_lookup"
        else:
            method = "sam_lookup_failed"

    if not b32:
        return CrossLayerResult(value, host, None, method, False, confidence="candidate_only", raw_error="could not derive canonical b32")

    routing_key = compute_routing_key_from_b32(b32)
    html, lookup_method = fetch_leaseset(b32)
    # Floodfill census is expensive (one `strings` subprocess per routerInfo
    # file); callers doing bulk lookups precompute once and pass it in.
    floodfills = floodfill_hashes if floodfill_hashes is not None else load_floodfill_hashes(limit=500)
    if not html:
        return CrossLayerResult(value, host, b32, lookup_method, False, routing_key=routing_key, floodfill_count=len(floodfills), confidence="candidate_only", raw_error="LeaseSet not found in console/SAM path")

    ls = parse_leaseset_html(html, query=b32)
    leases = ls.get("leases") if isinstance(ls.get("leases"), list) else []
    found = bool(ls.get("leaseset_hash") or leases or ls.get("b32"))
    confidence = "validated_by_leaseset" if found else "weak_console_evidence"
    return CrossLayerResult(
        input_value=value,
        host=normalize_host(str(ls.get("b32") or b32 or host)),
        canonical_b32=str(ls.get("b32") or b32),
        lookup_method=lookup_method,
        leaseset_found=found,
        leaseset_hash=str(ls.get("leaseset_hash") or "") or None,
        leaseset_type=str(ls.get("leaseset_type") or "") or None,
        routing_key=str(ls.get("routing_key") or routing_key or "") or None,
        published=str(ls.get("published") or "") or None,
        expires=str(ls.get("expires") or "") or None,
        gateway_count=len(leases),
        floodfill_count=len(floodfills),
        confidence=confidence,
        raw_summary=json.dumps({k: v for k, v in ls.items() if k != "raw_html"}, default=str)[:4000],
        console_template_version=str(ls.get("console_template_version") or "") or None,
        lease_parser_status=str(ls.get("lease_parser_status") or "") or None,
    )


def _candidate_values_from_file(path: Path) -> Iterable[str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if path.suffix.lower() in {".json", ".jsonl"}:
        def walk(obj: object) -> Iterable[str]:
            if isinstance(obj, dict):
                for v in obj.values():
                    yield from walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    yield from walk(v)
            elif isinstance(obj, str):
                yield obj
        for line in text.splitlines() or [text]:
            if not line.strip():
                continue
            try:
                yield from walk(json.loads(line))
            except json.JSONDecodeError:
                yield line
    elif path.suffix.lower() in {".csv", ".tsv"}:
        dialect = "excel-tab" if path.suffix.lower() == ".tsv" else "excel"
        for row in csv.reader(text.splitlines(), dialect=dialect):
            yield from row
    else:
        yield from text.splitlines()


def persist_cross_layer_result(session: Session, result: CrossLayerResult, source_detail: str | None = None, enqueue_all: bool = False, epoch_id: int | None = None) -> bool:
    if not result.host:
        return False
    should_enqueue = result.leaseset_found or enqueue_all
    site = upsert_site(
        session,
        result.host,
        source="cross_layer",
        state_if_new=SiteState.DISCOVERED.value if should_enqueue else SiteState.NEW.value,
        network_source=result.lookup_method,
        discovery_method=result.lookup_method,
        is_cross_layer=True,
    )
    if result.leaseset_found:
        site.state = SiteState.DISCOVERED.value
        site.is_cross_layer = True
        site.discovery_method = result.lookup_method
        site.network_source = result.lookup_method
        site.cross_layer_validated_at = now()
        site.last_network_observed_at = now()
    record_seed_event(session, result.host, "cross_layer", source_detail or result.lookup_method, epoch_id=epoch_id)
    session.add(CrossLayerObservation(
        site_id=site.id,
        epoch_id=epoch_id,
        host=result.host,
        input_value=result.input_value[:2048],
        canonical_b32=result.canonical_b32,
        lookup_method=result.lookup_method,
        leaseset_found=result.leaseset_found,
        leaseset_hash=result.leaseset_hash,
        leaseset_type=result.leaseset_type,
        routing_key=result.routing_key,
        published=result.published,
        expires=result.expires,
        gateway_count=result.gateway_count,
        floodfill_count=result.floodfill_count,
        confidence=result.confidence,
        raw_error=result.raw_error,
        raw_summary=result.raw_summary,
        console_template_version=result.console_template_version,
        lease_parser_status=result.lease_parser_status,
    ))
    return result.leaseset_found


def import_cross_layer_candidates(session: Session, path: str | Path, enqueue_all: bool = False) -> dict[str, int]:
    path = Path(path)
    counts = {"inputs": 0, "normalized": 0, "validated": 0, "observations": 0, "queued": 0}
    for value in _candidate_values_from_file(path):
        if not str(value).strip():
            continue
        counts["inputs"] += 1
        result = lookup_candidate(str(value))
        if result.host:
            counts["normalized"] += 1
        validated = persist_cross_layer_result(session, result, source_detail=str(path), enqueue_all=enqueue_all)
        if result.host:
            counts["observations"] += 1
            counts["queued"] += 1 if (validated or enqueue_all) else 0
        if validated:
            counts["validated"] += 1
    return counts


# ---------------------------------------------------------------------------
# Study 2: netDB harvest entry point (floodfill-gated)
# ---------------------------------------------------------------------------

def harvest_netdb(session: Session, epoch_id: int | None = None) -> dict[str, int]:
    """Harvest router infos from the local netDB directory into network_observations.

    DISABLED unless FLOODFILL_MODE=true. Floodfill participation requires a
    public IP and inbound UDP/TCP reachability; our VM1 had no public IP, so
    the router reports Network: Testing/Firewalled and harvesting would only
    see the local client's partial netDB view — kept off by default.
    """
    from .models import NetworkObservation, now

    if not settings.floodfill_mode:
        logger.warning(
            "netDB harvest skipped: FLOODFILL_MODE=false "
            "(floodfill needs public IP + inbound UDP/TCP; not provisioned)"
        )
        return {"harvested": 0, "disabled": True}

    path = local_netdb_path()
    if not path:
        logger.warning("netDB harvest skipped: no local netDb directory found")
        return {"harvested": 0, "disabled": True, "no_netdb": True}

    harvested = 0
    for fp in path.rglob("routerInfo-*.dat"):
        match = re.match(r"routerInfo-(.+)\.dat$", fp.name)
        if not match:
            continue
        session.add(
            NetworkObservation(
                router_hash=match.group(1),
                epoch_id=epoch_id,
                source_type="floodfill_netdb_harvest",
                source_detail=str(fp),
                observed_at=now(),
            )
        )
        harvested += 1
        if harvested % 500 == 0:
            session.flush()
    session.commit()
    logger.info("netDB harvest: recorded %d router infos", harvested)
    return {"harvested": harvested, "disabled": False}


# ---------------------------------------------------------------------------
# Study 2 Tier 1: client-mode local netDb census (no floodfill needed)
# ---------------------------------------------------------------------------

LOCAL_NETDB_SOURCE_TYPE = "local_netdb"
LOCAL_NETDB_DISCLOSURE = (
    "client-sampled netDb view from the vantage router's local store, "
    "not the full floodfill DHT"
)


def parse_routerinfo_bytes(data: bytes) -> dict[str, object]:
    """Best-effort parse of one routerInfo binary blob. Never raises.

    Byte-layout assumptions (classic 2048-bit identity):
    - bytes 0..255: ElGamal public key; 256..383: DSA signing public key;
      byte 384: cert type; bytes 385..386: big-endian cert length.
      identity_end = 387 + cert_len.
    - Published timestamp: 8-byte big-endian integer right after the
      identity, milliseconds since Unix epoch (Java Date.getTime()).
      Accepted only within [2020-01-01, now+7d]; otherwise None.
    - Router options (``router.version=``, ``caps=``) appear as ASCII
      ``k=v`` pairs later in the blob; extracted with regexes.
    The filename hash is always the ground truth for router identity;
    everything here is auxiliary metadata.
    """
    info: dict[str, object] = {
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
            hi = int(now().timestamp() * 1000) + 7 * 86400 * 1000
            if lo <= ts_ms <= hi:
                info["published_at"] = datetime.fromtimestamp(
                    ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None).isoformat()
    except Exception:
        logger.debug("routerInfo parse failed defensively", exc_info=True)
    return info


def census_local_netdb(session: Session, epoch_id: int | None) -> dict[str, int]:
    """Record one NetworkObservation per router in the LOCAL netDb.

    This is the Tier 1 client-mode census: it reads the vantage router's own
    ``netDb/`` store (populated through normal client operation) rather than
    participating in the floodfill DHT. Each row is tagged
    ``source_type='local_netdb'`` and the sampled-view disclosure, so the
    dissertation can distinguish it from the VPS floodfill sensor's full-DHT
    view (``source_type='vps_floodfill_netdb'``). Deduplicates on
    (router_hash, epoch_id, source_type); malformed files never crash it.
    """
    from .models import NetworkObservation

    counts = {"files": 0, "recorded": 0, "skipped_dup": 0, "errors": 0}
    path = local_netdb_path()
    if not path:
        logger.warning("local netDb census skipped: no netDb directory found")
        return counts
    for fp in sorted(path.rglob("routerInfo-*.dat")):
        match = re.match(r"routerInfo-(.+)\.dat$", fp.name)
        if not match:
            continue
        router_hash = match.group(1)
        counts["files"] += 1
        try:
            dup = session.scalar(
                select(NetworkObservation.id).where(
                    NetworkObservation.router_hash == router_hash,
                    NetworkObservation.epoch_id == epoch_id,
                    NetworkObservation.source_type == LOCAL_NETDB_SOURCE_TYPE,
                )
            )
            if dup is not None:
                counts["skipped_dup"] += 1
                continue
            try:
                data = fp.read_bytes()
            except OSError:
                counts["errors"] += 1
                continue
            info = parse_routerinfo_bytes(data)
            try:
                observed = datetime.fromtimestamp(fp.stat().st_mtime)
            except OSError:
                observed = now()
            session.add(NetworkObservation(
                router_hash=router_hash,
                epoch_id=epoch_id,
                source_type=LOCAL_NETDB_SOURCE_TYPE,
                source_detail=f"{LOCAL_NETDB_DISCLOSURE}; file={fp.name}",
                raw_value=json.dumps(
                    {k: v for k, v in info.items() if v is not None},
                    default=str)[:2000],
                observed_at=observed,
            ))
            counts["recorded"] += 1
            if counts["recorded"] % 500 == 0:
                session.flush()
        except Exception:
            logger.exception("local netDb census failed on %s", fp)
            counts["errors"] += 1
    session.commit()
    logger.info(
        "local netDb census: %d files, %d recorded, %d dups skipped, %d errors",
        counts["files"], counts["recorded"], counts["skipped_dup"], counts["errors"])
    return counts
