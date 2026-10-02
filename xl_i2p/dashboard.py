"""Read-only monitoring dashboard for XL-I2P Study 2.

Runs on VM2 (where MariaDB lives) and exposes the campaign stats for
on-the-go checks from a phone. The app NEVER writes to the database —
it only issues SELECTs through the existing engine/session factory.

Usage:
    python -m xl_i2p.dashboard

Environment:
    DASHBOARD_TOKEN  bearer/?token= auth; unset = open (trusted LAN only)
    DASHBOARD_HOST   bind address, default 0.0.0.0 (VM2 has no public IP,
                     so this is private-LAN only)
    DASHBOARD_PORT   default 8080
"""
from __future__ import annotations

import hmac
import json
import logging
import urllib.parse
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request
from sqlalchemy import func, select, text

from . import db
from .config import settings
from .models import (
    CrawlAttempt,
    CrossLayerObservation,
    Epoch,
    Heartbeat,
    Link,
    NetworkObservation,
    Page,
    Site,
)
from .states import AttemptStatus, AttemptType, EpochStatus, SiteState

log = logging.getLogger(__name__)

# Heartbeat considered fresh while younger than this multiple of HEARTBEAT_SECONDS.
ALIVE_MULTIPLE = 3
STALE_MULTIPLE = 30

STATE_ORDER = [
    SiteState.NEW.value,
    SiteState.DISCOVERED.value,
    SiteState.VERIFYING.value,
    SiteState.REACHABLE.value,
    SiteState.CRAWLING.value,
    SiteState.CRAWLED.value,
    SiteState.RETRY_READY.value,
    SiteState.UNREACHABLE.value,
    SiteState.ERROR.value,
    SiteState.PAUSED.value,
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _age_str(ts: datetime | None) -> str:
    if ts is None:
        return "never"
    secs = int((_utcnow() - ts).total_seconds())
    if secs < 0:
        secs = 0
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _rate(success: int, total: int) -> float | None:
    return round(success / total, 3) if total else None


def collect_stats() -> dict:
    """Gather every dashboard number with cheap, indexed SELECTs only."""
    session = db.SessionLocal()
    try:
        return _collect(session)
    finally:
        session.close()


def _collect(session) -> dict:
    now = _utcnow()
    epoch = session.execute(
        select(Epoch).where(Epoch.status == EpochStatus.OPEN.value)
    ).scalar_one_or_none()
    epoch_id = epoch.id if epoch else None

    # --- Liveness ---------------------------------------------------------
    beats = (
        session.execute(select(Heartbeat).order_by(Heartbeat.updated_at.desc()).limit(10))
        .scalars()
        .all()
    )
    latest = beats[0] if beats else None
    if latest is None:
        liveness = {"status": "UNKNOWN", "last_seen": None, "seconds_ago": None, "phase": None}
    else:
        age_s = (now - latest.updated_at).total_seconds()
        if age_s < settings.heartbeat_seconds * ALIVE_MULTIPLE:
            status = "ALIVE"
        elif age_s < settings.heartbeat_seconds * STALE_MULTIPLE:
            status = "STALE"
        else:
            status = "DEAD"
        counters = {}
        try:
            counters = json.loads(latest.counters_json or "{}")
        except (TypeError, ValueError):
            pass
        liveness = {
            "status": status,
            "last_seen": latest.updated_at.isoformat(),
            "seconds_ago": int(age_s),
            "phase": latest.phase,
            "cycles": counters.get("cycles"),
        }

    heartbeats = []
    for b in beats:
        try:
            cycles = json.loads(b.counters_json or "{}").get("cycles")
        except (TypeError, ValueError):
            cycles = None
        heartbeats.append(
            {
                "at": b.updated_at.isoformat() if b.updated_at else None,
                "phase": b.phase,
                "epoch": b.epoch_label,
                "cycles": cycles,
            }
        )

    # --- Cohort -----------------------------------------------------------
    state_rows = session.execute(
        select(Site.state, func.count()).group_by(Site.state)
    ).all()
    by_state = {row[0]: row[1] for row in state_rows}
    cohort_total = sum(by_state.values())

    # --- This epoch -------------------------------------------------------
    epoch_block: dict = {"label": epoch.label if epoch else None}
    if epoch is not None:
        age_days = (now - epoch.started_at).days if epoch.started_at else 0
        epoch_block.update(
            {
                "age_days": age_days,
                "days_until_rollover": settings.epoch_duration_days - age_days,
                "epoch_duration_days": settings.epoch_duration_days,
            }
        )
        for atype, key in ((AttemptType.VERIFY.value, "verify"), (AttemptType.CRAWL.value, "crawl")):
            total = session.execute(
                select(func.count())
                .select_from(CrawlAttempt)
                .where(CrawlAttempt.epoch_id == epoch_id, CrawlAttempt.attempt_type == atype)
            ).scalar() or 0
            success = session.execute(
                select(func.count())
                .select_from(CrawlAttempt)
                .where(
                    CrawlAttempt.epoch_id == epoch_id,
                    CrawlAttempt.attempt_type == atype,
                    CrawlAttempt.status == AttemptStatus.SUCCESS.value,
                )
            ).scalar() or 0
            epoch_block[f"{key}_attempts"] = total
            epoch_block[f"{key}_success"] = success
            epoch_block[f"{key}_success_rate"] = _rate(success, total)

        epoch_block["pages_fetched"] = (
            session.execute(
                select(func.count()).select_from(Page).where(Page.epoch_id == epoch_id)
            ).scalar()
            or 0
        )
        epoch_block["links_found"] = (
            session.execute(
                select(func.count()).select_from(Link).where(Link.epoch_id == epoch_id)
            ).scalar()
            or 0
        )
        epoch_block["new_sites"] = (
            session.execute(
                select(func.count())
                .select_from(Site)
                .where(Site.first_seen_at >= epoch.started_at)
            ).scalar()
            or 0
        )
        err_rows = session.execute(
            select(CrawlAttempt.error_type, func.count())
            .where(CrawlAttempt.epoch_id == epoch_id, CrawlAttempt.error_type.isnot(None))
            .group_by(CrawlAttempt.error_type)
            .order_by(func.count().desc())
            .limit(5)
        ).all()
        epoch_block["error_top5"] = [[r[0], r[1]] for r in err_rows]

    # --- Cross-layer ------------------------------------------------------
    def _group(model, field, epoch_filter: bool) -> dict:
        q = select(field, func.count()).group_by(field)
        if epoch_filter and epoch_id is not None:
            q = q.where(model.epoch_id == epoch_id)
        return {r[0]: r[1] for r in session.execute(q).all()}

    cross_layer = {
        "this_epoch": {
            "cross_layer_observations": _group(CrossLayerObservation, CrossLayerObservation.lookup_method, True) if epoch_id else {},
            "network_observations": _group(NetworkObservation, NetworkObservation.source_type, True) if epoch_id else {},
        },
        "cumulative": {
            "cross_layer_observations": sum(_group(CrossLayerObservation, CrossLayerObservation.lookup_method, False).values()),
            "network_observations": sum(_group(NetworkObservation, NetworkObservation.source_type, False).values()),
        },
    }

    # --- Churn (needs a previous epoch) -----------------------------------
    churn: dict = {"available": False}
    if epoch is not None:
        prev = session.execute(
            select(Epoch)
            .where(Epoch.id != epoch.id)
            .order_by(Epoch.started_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if prev is not None:
            good_types = (AttemptType.VERIFY.value, AttemptType.CRAWL.value)
            cur_ok = set(
                session.execute(
                    select(CrawlAttempt.site_id)
                    .where(
                        CrawlAttempt.epoch_id == epoch_id,
                        CrawlAttempt.status == AttemptStatus.SUCCESS.value,
                        CrawlAttempt.attempt_type.in_(good_types),
                    )
                    .distinct()
                ).scalars()
            )
            prev_ok = set(
                session.execute(
                    select(CrawlAttempt.site_id)
                    .where(
                        CrawlAttempt.epoch_id == prev.id,
                        CrawlAttempt.status == AttemptStatus.SUCCESS.value,
                        CrawlAttempt.attempt_type.in_(good_types),
                    )
                    .distinct()
                ).scalars()
            )
            cur_attempted = set(
                session.execute(
                    select(CrawlAttempt.site_id)
                    .where(CrawlAttempt.epoch_id == epoch_id)
                    .distinct()
                ).scalars()
            )
            churn = {
                "available": True,
                "prev_epoch_label": prev.label,
                "newly_reachable": len(cur_ok - prev_ok),
                "lost": len((prev_ok - cur_ok) & cur_attempted),
            }

    # --- Stuck sites (janitor signal) --------------------------------------
    cutoff = now - timedelta(minutes=settings.stale_minutes)
    stuck_states = (SiteState.VERIFYING.value, SiteState.CRAWLING.value)
    stuck_count = (
        session.execute(
            select(func.count())
            .select_from(Site)
            .where(Site.state.in_(stuck_states), Site.last_checked_at < cutoff)
        ).scalar()
        or 0
    )
    stuck_hosts = list(
        session.execute(
            select(Site.host)
            .where(Site.state.in_(stuck_states), Site.last_checked_at < cutoff)
            .limit(20)
        ).scalars()
    )

    return {
        "generated_at": now.isoformat(),
        "epoch": epoch_block,
        "liveness": liveness,
        "cohort": {"total": cohort_total, "by_state": by_state, "state_order": STATE_ORDER},
        "this_epoch": epoch_block,
        "cross_layer": cross_layer,
        "churn": churn,
        "health": {
            "heartbeats": heartbeats,
            "stuck_sites": {
                "count": stuck_count,
                "hosts": stuck_hosts,
                "stale_minutes": settings.stale_minutes,
            },
        },
    }


def create_app(token: str | None = None) -> Flask:
    """Build the Flask app. ``token=None`` falls back to settings.dashboard_token."""
    dash_token = token if token is not None else (settings.dashboard_token or None)
    app = Flask(__name__)

    @app.before_request
    def _auth():
        if request.path == "/healthz":
            return None
        if not dash_token:
            return None
        supplied = request.args.get("token", "")
        authz = request.headers.get("Authorization", "")
        if authz.lower().startswith("bearer "):
            supplied = authz[7:].strip()
        if supplied and hmac.compare_digest(supplied, dash_token):
            return None
        return jsonify({"error": "unauthorized"}), 401

    @app.get("/healthz")
    def healthz():
        try:
            session = db.SessionLocal()
            try:
                session.execute(text("SELECT 1"))
            finally:
                session.close()
            return jsonify({"status": "ok", "db": "ok"}), 200
        except Exception as exc:  # noqa: BLE001 - surfaced as 503 detail
            log.warning("healthz DB check failed: %s", exc)
            return jsonify({"status": "degraded", "db": "unreachable"}), 503

    @app.get("/api/stats")
    def api_stats():
        try:
            return jsonify(collect_stats())
        except Exception as exc:  # noqa: BLE001 - DB down etc.
            log.warning("stats collection failed: %s", exc)
            return jsonify({"error": "stats unavailable", "detail": str(exc)}), 503

    @app.get("/")
    def index():
        try:
            stats = collect_stats()
        except Exception as exc:  # noqa: BLE001
            log.warning("dashboard render failed: %s", exc)
            stats = None
        return _render(stats, token_param=request.args.get("token", ""))

    @app.get("/api/research")
    def api_research():
        try:
            return jsonify(collect_research())
        except Exception as exc:  # noqa: BLE001
            log.warning("research stats collection failed: %s", exc)
            return jsonify({"error": "research stats unavailable", "detail": str(exc)}), 503

    @app.get("/api/research/site")
    def api_research_site():
        host = (request.args.get("host") or "").strip().lower()
        if not host:
            return jsonify({"error": "missing host"}), 400
        try:
            session = db.SessionLocal()
            try:
                hist = collect_site_history(session, host)
            finally:
                session.close()
        except Exception as exc:  # noqa: BLE001
            log.warning("site history lookup failed: %s", exc)
            return jsonify({"error": "lookup unavailable", "detail": str(exc)}), 503
        if hist is None:
            return jsonify({"error": "unknown host"}), 404
        return jsonify(hist)

    @app.get("/research")
    def research():
        try:
            stats = collect_research()
        except Exception as exc:  # noqa: BLE001
            log.warning("research render failed: %s", exc)
            stats = None
        return _render_research(stats, token_param=request.args.get("token", ""))

    return app


def _render(stats: dict | None, token_param: str) -> str:
    """Server-render the dashboard; JS re-fetches /api/stats every 60s."""
    s = stats or {}
    epoch = s.get("epoch") or {}
    live = s.get("liveness") or {}
    cohort = s.get("cohort") or {}
    by_state = cohort.get("by_state") or {}
    order = cohort.get("state_order") or list(by_state)
    health = s.get("health") or {}
    stuck = health.get("stuck_sites") or {}
    churn = s.get("churn") or {}
    xl = s.get("cross_layer") or {}
    xl_epoch = xl.get("this_epoch") or {}
    xl_cum = xl.get("cumulative") or {}

    def _state_cells() -> str:
        cells = []
        for st in order:
            cells.append(
                f'<div class="cell"><span class="k">{st}</span>'
                f'<span class="v" id="st-{st}">{by_state.get(st, 0)}</span></div>'
            )
        return "".join(cells)

    def _err_bars() -> str:
        rows = (s.get("this_epoch") or {}).get("error_top5") or []
        if not rows:
            return '<p class="muted">no errors this epoch</p>'
        mx = max(r[1] for r in rows) or 1
        out = []
        for name, cnt in rows:
            w = max(2, int(cnt / mx * 100))
            out.append(
                f'<div class="stat"><span class="mono">{name}</span><b>{cnt}</b></div>'
                f'<div class="bar"><i style="width:{w}%"></i></div>'
            )
        return "".join(out)

    def _xl_clo_rows(mapping: dict) -> str:
        if not mapping:
            return '<div class="stat"><span class="muted">—</span><span></span></div>'
        return "".join(
            f'<div class="stat"><span class="mono">{k}</span><b>{v}</b></div>'
            for k, v in sorted(mapping.items())
        )

    def _xl_net_bars(mapping: dict) -> str:
        if not mapping:
            return '<p class="muted">—</p>'
        mx = max(mapping.values()) or 1
        out = []
        for k, v in sorted(mapping.items()):
            w = max(2, int(v / mx * 100))
            out.append(
                f'<div class="stat"><span class="mono">{k}</span><b>{v}</b></div>'
                f'<div class="bar teal"><i style="width:{w}%"></i></div>'
            )
        return "".join(out)

    def _beats() -> str:
        rows = health.get("heartbeats") or []
        if not rows:
            return '<tr><td colspan="3" class="muted">no heartbeats yet</td></tr>'
        out = []
        for b in rows:
            cyc = b.get("cycles")
            out.append(
                f'<tr><td class="mono">{b.get("at") or "—"}</td>'
                f'<td>{b.get("phase") or "—"}</td>'
                f'<td class="num">{cyc if cyc is not None else "—"}</td></tr>'
            )
        return "".join(out)

    live_class = {"ALIVE": "ok", "STALE": "warn", "DEAD": "bad"}.get(live.get("status"), "muted")
    last_seen_txt = (
        _age_str(datetime.fromisoformat(live["last_seen"])) if live.get("last_seen") else "never"
    )

    churn_html = (
        f'<div class="row"><span>Newly reachable vs {churn.get("prev_epoch_label")}</span>'
        f'<b id="churn-new">{churn.get("newly_reachable")}</b></div>'
        f'<div class="row"><span>Lost (attempted, no success yet)</span>'
        f'<b id="churn-lost">{churn.get("lost")}</b></div>'
        if churn.get("available")
        else '<p class="muted">— (needs a second epoch)</p>'
    )

    stats_json = json.dumps(s)
    html = _PAGE_TEMPLATE
    html = html.replace("__EPOCH_LABEL__", epoch.get("label") or "none")
    html = html.replace("__AGE_DAYS__", str(epoch.get("age_days", "—")))
    html = html.replace("__ROLLOVER_DAYS__", str(epoch.get("days_until_rollover", "—")))
    html = html.replace("__LIVE_CLASS__", live_class)
    html = html.replace("__LIVE_STATUS__", str(live.get("status", "UNKNOWN")))
    html = html.replace("__LIVE_SEEN__", last_seen_txt)
    html = html.replace("__COHORT_TOTAL__", str(cohort.get("total", 0)))
    html = html.replace("__STATE_CELLS__", _state_cells())
    html = html.replace("__VERIFY__", str((s.get("this_epoch") or {}).get("verify_attempts", "—")))
    html = html.replace("__VERIFY_RATE__", _pct((s.get("this_epoch") or {}).get("verify_success_rate")))
    html = html.replace("__CRAWL__", str((s.get("this_epoch") or {}).get("crawl_attempts", "—")))
    html = html.replace("__CRAWL_RATE__", _pct((s.get("this_epoch") or {}).get("crawl_success_rate")))
    html = html.replace("__PAGES__", str((s.get("this_epoch") or {}).get("pages_fetched", "—")))
    html = html.replace("__LINKS__", str((s.get("this_epoch") or {}).get("links_found", "—")))
    html = html.replace("__NEW_SITES__", str((s.get("this_epoch") or {}).get("new_sites", "—")))
    html = html.replace("__ERROR_BARS__", _err_bars())
    html = html.replace("__XL_CLO_ROWS__", _xl_clo_rows(xl_epoch.get("cross_layer_observations") or {}))
    html = html.replace("__XL_NET_BARS__", _xl_net_bars(xl_epoch.get("network_observations") or {}))
    html = html.replace("__XL_CLO_CUM__", str(xl_cum.get("cross_layer_observations", 0)))
    html = html.replace("__XL_NET_CUM__", str(xl_cum.get("network_observations", 0)))
    html = html.replace("__REACHABLE_N__", str(by_state.get(SiteState.REACHABLE.value, 0)))
    html = html.replace("__CRAWLED_N__", str(by_state.get(SiteState.CRAWLED.value, 0)))
    html = html.replace("__CHURN_HTML__", churn_html)
    html = html.replace("__BEAT_ROWS__", _beats())
    html = html.replace("__STUCK_COUNT__", str(stuck.get("count", 0)))
    html = html.replace(
        "__STUCK_HOSTS__",
        ", ".join(stuck.get("hosts") or []) or "none",
    )
    html = html.replace("__STATS_JSON__", stats_json.replace("</", "<\\/"))
    safe_token = token_param.replace("\\", "\\\\").replace('"', '\\"')
    html = html.replace("__TOKEN_QS__", safe_token)
    html = html.replace(
        "__R_TOKEN__",
        "?token=" + urllib.parse.quote(token_param, safe="") if token_param else "",
    )
    return html


def _render_research(stats: dict | None, token_param: str) -> str:
    """Server-render the research archive page; JS soft-refreshes every 5 min."""
    s = stats or {}
    k = s.get("kpis") or {}
    surv = s.get("survival") or {}
    ledger = s.get("ledger") or []
    flaps = s.get("flap_leaders") or []

    def _ledger_rows() -> str:
        if not ledger:
            return '<tr><td colspan="6" class="muted">no epochs yet</td></tr>'
        out = []
        for row in ledger:
            lost = "—" if row["lost"] is None else str(row["lost"])
            newly = "—" if row["newly_reachable"] is None else str(row["newly_reachable"])
            out.append(
                f'<tr><td class="mono">{row["label"]} <span class="muted">({row["status"]})</span></td>'
                f'<td class="num">{row["ever_reachable"]}</td>'
                f'<td class="num">{row["ever_crawled"]}</td>'
                f'<td class="num">{lost}</td>'
                f'<td class="num">{newly}</td></tr>'
            )
        return "".join(out)

    def _flap_rows() -> str:
        if not flaps:
            return '<tr><td colspan="3" class="muted">no transitions recorded yet</td></tr>'
        out = []
        for f in flaps:
            pill = (
                '<span class="pill ok">alive</span>'
                if f["now"] == "alive"
                else '<span class="pill bad">dead</span>'
            )
            out.append(
                f'<tr><td class="mono">{f["host"]}</td>'
                f'<td class="num">{f["transitions"]}</td>'
                f"<td>{pill}</td></tr>"
            )
        return "".join(out)

    def _funnel() -> str:
        total = k.get("cohort_total") or 0
        rows = [
            ("Seeded (Study 1)", total, "#2dd4bf"),
            ("Discovered via links", k.get("discovered_via_links") or 0, "#f5a623"),
            ("Ever reachable", k.get("ever_reachable") or 0, "#2dd4bf"),
            ("Ever crawled", k.get("ever_crawled") or 0, "#2ecc71"),
        ]
        mx = max([v for _, v, _ in rows] + [1])
        out = []
        for label, v, color in rows:
            w = max(2, int(v / mx * 100))
            out.append(
                f'<div class="frow"><span>{label}</span>'
                f'<div class="fbar"><i style="width:{w}%;background:{color}"></i></div>'
                f'<span class="n">{v}</span></div>'
            )
        return "".join(out)

    lifetime = k.get("cohort_lifetime_pct")
    html = _RESEARCH_TEMPLATE
    html = html.replace("__EPOCH_LABEL__", s.get("epoch_label") or "none")
    html = html.replace("__EVER_REACH__", str(k.get("ever_reachable", "—")))
    html = html.replace("__EVER_CRAWL__", str(k.get("ever_crawled", "—")))
    html = html.replace("__EVER_REACH_E__", str(k.get("ever_reachable_this_epoch", "—")))
    html = html.replace("__EVER_CRAWL_E__", str(k.get("ever_crawled_this_epoch", "—")))
    html = html.replace(
        "__LIFETIME__", f"{lifetime:.2f}%" if lifetime is not None else "—"
    )
    html = html.replace("__COHORT_TOTAL__", str(k.get("cohort_total", "—")))
    html = html.replace("__XLAYER_N__", str(k.get("xlayer_validated", "—")))
    html = html.replace("__DISC_N__", str(k.get("discovered_via_links", "—")))
    html = html.replace("__SURV_SVG__", surv.get("svg") or "")
    html = html.replace("__SURV_R__", str(surv.get("final_reachable", 0)))
    html = html.replace("__SURV_C__", str(surv.get("final_crawled", 0)))
    html = html.replace("__SURV_DAYS__", str(surv.get("days", 0)))
    html = html.replace("__LEDGER_ROWS__", _ledger_rows())
    html = html.replace("__FUNNEL__", _funnel())
    html = html.replace("__FLAP_ROWS__", _flap_rows())
    safe_token = token_param.replace("\\", "\\\\").replace('"', '\\"')
    html = html.replace("__TOKEN_QS__", safe_token)
    html = html.replace("__REFRESH_MS__", str(RESEARCH_REFRESH_MS))
    return html


_RESEARCH_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XL-I2P Study 2 — Research Archive</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:0;padding:14px;background:#0d1424;color:#e6ecf5;max-width:1100px;margin-inline:auto}
h2{font-size:.72rem;margin:0 0 10px;color:#9fb0c9;text-transform:uppercase;letter-spacing:.09em}
.topbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.title{font-size:1.35rem;font-weight:700;margin-right:auto}
.chip{font-size:.78rem;color:#9fb0c9;background:#16203a;padding:6px 14px;border-radius:20px}
a.chip{color:#2dd4bf;text-decoration:none;border:1px solid #2dd4bf}
.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:12px}
.kpi{background:#16203a;border-radius:12px;padding:14px}
.kpi .v{font-size:1.7rem;font-weight:700;font-variant-numeric:tabular-nums}
.kpi .l{font-size:.75rem;color:#9fb0c9;margin-top:3px}
.card{background:#16203a;border-radius:12px;padding:16px;margin-bottom:12px}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.stat{display:flex;justify-content:space-between;font-size:.85rem;padding:3px 0}
.stat b{font-variant-numeric:tabular-nums}
.mono{font-family:ui-monospace,monospace;font-size:.8rem;word-break:break-all}
.muted{color:#8a99a8}
table{width:100%;border-collapse:collapse;font-size:.85rem}
td,th{padding:5px 6px;border-bottom:1px solid #22304f;text-align:left}
th{color:#9fb0c9;font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.05em}
.num{text-align:right;font-variant-numeric:tabular-nums}
.frow{display:grid;grid-template-columns:170px 1fr 80px;gap:10px;align-items:center;font-size:.85rem;margin:7px 0}
.fbar{height:20px;border-radius:5px;background:#0d1424;overflow:hidden}
.fbar i{display:block;height:100%;border-radius:5px}
.frow .n{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
svg.chart{width:100%;height:auto;background:#0d1424;border-radius:8px}
.legend{display:flex;gap:16px;font-size:.78rem;color:#9fb0c9;margin-top:8px}
.sw{display:inline-block;width:11px;height:11px;border-radius:3px;margin-right:5px;vertical-align:-1px}
.pill{display:inline-block;font-size:.72rem;font-weight:700;padding:2px 10px;border-radius:12px;border:1px solid}
.pill.ok{color:#2ecc71;border-color:#2ecc71;background:rgba(46,204,113,.12)}
.pill.bad{color:#e74c3c;border-color:#e74c3c;background:rgba(231,76,60,.12)}
.search{display:flex;gap:8px;margin-bottom:10px}
.search input{flex:1;background:#0d1424;border:1px solid #22304f;border-radius:8px;color:#e6ecf5;padding:10px 12px;font-size:.9rem;font-family:ui-monospace,monospace}
.btn{background:#2dd4bf;color:#06231f;border:none;border-radius:8px;padding:10px 18px;font-weight:700;cursor:pointer}
.timeline{display:flex;gap:4px;flex-wrap:wrap;margin:10px 0;align-items:center}
.dot{width:12px;height:12px;border-radius:50%;flex:none}
.dot.ok{background:#2ecc71}.dot.no{background:#e74c3c;opacity:.75}.dot.warn{background:#f5a623}
.statgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:10px 0}
.statbox{background:#0d1424;border-radius:8px;padding:8px 10px}
.statbox .v{font-weight:700;font-size:1rem}.statbox .l{font-size:.7rem;color:#9fb0c9}
.foot{font-size:.75rem;color:#8a99a8;text-align:center;margin-top:2px}
a.navlink{font-size:.78rem;color:#2dd4bf;background:#16203a;padding:6px 14px;border-radius:20px;text-decoration:none;border:1px solid #2dd4bf}
@media (max-width:700px){.kpis{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}.frow{grid-template-columns:120px 1fr 60px}}
</style>
</head>
<body>
<div class="topbar">
  <div class="title">Research Archive</div>
  <span class="chip">epoch <b class="mono" id="r-epoch">__EPOCH_LABEL__</b></span>
  <a class="chip" id="r-back" href="/">&larr; Mission Control</a>
</div>

<div class="kpis">
  <div class="kpi"><div class="v" id="r-ever-reach">__EVER_REACH__</div><div class="l">ever reachable (all time &middot; <span id="r-ever-reach-e">__EVER_REACH_E__</span> this epoch)</div></div>
  <div class="kpi"><div class="v" id="r-ever-crawl">__EVER_CRAWL__</div><div class="l">ever crawled (all time &middot; <span id="r-ever-crawl-e">__EVER_CRAWL_E__</span> this epoch)</div></div>
  <div class="kpi"><div class="v" id="r-lifetime">__LIFETIME__</div><div class="l">cohort lifetime reach &middot; <span id="r-total">__COHORT_TOTAL__</span> sites</div></div>
  <div class="kpi"><div class="v" id="r-xlayer">__XLAYER_N__</div><div class="l">sites with validated router association</div></div>
  <div class="kpi"><div class="v" id="r-disc">__DISC_N__</div><div class="l">sites discovered via links</div></div>
</div>

<div class="card">
  <h2>Survival &mdash; cumulative distinct sites ever seen alive, by day</h2>
  <div id="r-svg">__SURV_SVG__</div>
  <div class="legend"><span><span class="sw" style="background:#2dd4bf"></span>ever reachable (<span id="r-sr">__SURV_R__</span>)</span><span><span class="sw" style="background:#2ecc71"></span>ever crawled (<span id="r-sc">__SURV_C__</span>)</span><span class="muted">day 0 = epoch start &middot; <span id="r-days">__SURV_DAYS__</span> days</span></div>
</div>

<div class="card">
  <h2>Per-epoch ledger</h2>
  <table><thead><tr><th>Epoch</th><th class="num">Ever reachable</th><th class="num">Ever crawled</th><th class="num">Lost</th><th class="num">Newly reachable</th></tr></thead>
  <tbody id="r-ledger">__LEDGER_ROWS__</tbody></table>
  <div class="foot">lost / newly-reachable are computed from the immutable attempts table at rollover &mdash; never from live states.</div>
</div>

<div class="cols">
  <div class="card">
    <h2>Discovery funnel</h2>
    <div id="r-funnel">__FUNNEL__</div>
  </div>
  <div class="card">
    <h2>Flap leaders &mdash; most alive&harr;dead transitions</h2>
    <table><thead><tr><th>Host</th><th class="num">Transitions</th><th>Now</th></tr></thead>
    <tbody id="r-flaps">__FLAP_ROWS__</tbody></table>
    <div class="foot">High flappers are the interesting cases: intermittent hosting, not stable death.</div>
  </div>
</div>

<div class="card">
  <h2>Site timeline lookup</h2>
  <div class="search"><input id="r-host" placeholder="paste a .i2p host, e.g. identiguy.i2p"><button class="btn" id="r-go">Trace</button></div>
  <div id="r-result"><p class="muted">Every probe, every outcome, its pages and its router association &mdash; no SQL needed.</p></div>
</div>

<div class="foot" id="r-updated"></div>

<script>
const TOKEN_QS = "__TOKEN_QS__";
const REFRESH_MS = __REFRESH_MS__;
function qs(){ return TOKEN_QS ? "?token=" + encodeURIComponent(TOKEN_QS) : ""; }
(function(){ const b = document.getElementById("r-back"); if (b) b.href = "/" + qs(); })();
function set(id, v){ const el = document.getElementById(id); if (el && v !== undefined && v !== null) el.textContent = v; }
async function refresh(){
  try {
    const r = await fetch("/api/research" + qs());
    if (!r.ok) return;
    const s = await r.json(), k = s.kpis || {}, sv = s.survival || {};
    set("r-epoch", s.epoch_label || "none");
    set("r-ever-reach", k.ever_reachable); set("r-ever-reach-e", k.ever_reachable_this_epoch);
    set("r-ever-crawl", k.ever_crawled); set("r-ever-crawl-e", k.ever_crawled_this_epoch);
    set("r-lifetime", k.cohort_lifetime_pct == null ? "—" : k.cohort_lifetime_pct.toFixed(2) + "%");
    set("r-total", k.cohort_total); set("r-xlayer", k.xlayer_validated); set("r-disc", k.discovered_via_links);
    const svg = document.getElementById("r-svg"); if (svg && sv.svg) svg.innerHTML = sv.svg;
    set("r-sr", sv.final_reachable); set("r-sc", sv.final_crawled); set("r-days", sv.days);
    const up = document.getElementById("r-updated");
    if (up && s.generated_at) up.textContent = "updated " + new Date(s.generated_at + "Z").toLocaleTimeString();
  } catch (e) { /* keep last good values */ }
}
setInterval(refresh, REFRESH_MS);
document.getElementById("r-go").addEventListener("click", trace);
document.getElementById("r-host").addEventListener("keydown", e => { if (e.key === "Enter") trace(); });
async function trace(){
  const host = document.getElementById("r-host").value.trim().toLowerCase();
  const box = document.getElementById("r-result");
  if (!host){ box.innerHTML = '<p class="muted">enter a host first.</p>'; return; }
  box.innerHTML = '<p class="muted">tracing…</p>';
  try {
    const r = await fetch("/api/research/site" + qs() + (qs() ? "&" : "?") + "host=" + encodeURIComponent(host));
    if (r.status === 404){ box.innerHTML = '<p class="muted">unknown host — not in the cohort.</p>'; return; }
    if (!r.ok){ box.innerHTML = '<p class="muted">lookup failed.</p>'; return; }
    box.innerHTML = renderSite(await r.json());
  } catch (e){ box.innerHTML = '<p class="muted">lookup failed.</p>'; }
}
function dotCls(a){
  if (a.status === "SUCCESS") return "ok";
  if (a.status === "FAILED") return "no";
  return "warn";
}
function esc(s){ return String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c])); }
function renderSite(h){
  const atts = (h.attempts || []).slice().reverse();
  const dots = atts.slice(0, 160).map(a =>
    `<span class="dot ${dotCls(a)}" title="${esc(a.at)} ${esc(a.type)} ${esc(a.status)}${a.error_type ? " · " + esc(a.error_type) : ""}"></span>`
  ).join("");
  const rows = (h.attempts || []).slice(0, 12).map(a =>
    `<tr><td class="mono">${esc((a.at || "").replace("T", " ").slice(0, 16))}</td><td>${esc(a.type)}</td><td>${esc(a.status)}</td><td class="mono">${esc(a.error_type || "—")}</td></tr>`
  ).join("");
  const xl = (h.cross_layer || []).map(o =>
    `<tr><td class="mono">${esc((o.at || "").replace("T", " ").slice(0, 16))}</td><td class="mono">${esc(o.method || "")}</td><td>${o.leaseset_found ? "validated" : "not found"}</td><td class="mono">${esc((o.canonical_b32 || "").slice(0, 12))}…</td></tr>`
  ).join("");
  return `<h2 class="mono" style="color:#e6ecf5;font-size:1rem">${esc(h.host)} <span class="muted">(${esc(h.state)})</span></h2>
  <div class="timeline">${dots || '<span class="muted">no attempts recorded</span>'}</div>
  <div class="legend"><span><span class="sw" style="background:#2ecc71"></span>success</span><span><span class="sw" style="background:#e74c3c"></span>failed</span><span><span class="sw" style="background:#f5a623"></span>other</span><span class="muted">oldest → newest · hover a dot for detail${atts.length > 160 ? " · showing latest 160" : ""}</span></div>
  <div class="statgrid">
    <div class="statbox"><div class="v">${h.attempts.length}</div><div class="l">probes shown</div></div>
    <div class="statbox"><div class="v">${h.success_count}</div><div class="l">site successes</div></div>
    <div class="statbox"><div class="v">${h.failure_count}</div><div class="l">site failures</div></div>
    <div class="statbox"><div class="v">${h.pages_fetched}</div><div class="l">pages fetched</div></div>
    <div class="statbox"><div class="v">${h.links_found}</div><div class="l">links out</div></div>
    <div class="statbox"><div class="v">${esc(h.discovery_method || "seed")}</div><div class="l">discovered via</div></div>
  </div>
  <h2>Recent attempts</h2><table><thead><tr><th>At</th><th>Type</th><th>Status</th><th>Error</th></tr></thead><tbody>${rows}</tbody></table>
  <h2 style="margin-top:10px">Cross-layer identity</h2>${xl ? `<table><thead><tr><th>At</th><th>Method</th><th>LeaseSet</th><th>Canonical b32</th></tr></thead><tbody>${xl}</tbody></table>` : '<p class="muted">no cross-layer observations</p>'}`;
}
</script>
</body>
</html>
"""


def _pct(rate: float | None) -> str:
    return f"{rate * 100:.1f}%" if rate is not None else "—"


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XL-I2P Study 2 — Dashboard</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:0;padding:14px;background:#0d1424;color:#e6ecf5;max-width:1100px;margin-inline:auto}
h2{font-size:.72rem;margin:0 0 10px;color:#9fb0c9;text-transform:uppercase;letter-spacing:.09em}
.topbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.title{font-size:1.35rem;font-weight:700;margin-right:auto}
.pill{display:inline-flex;align-items:center;gap:7px;font-size:.8rem;font-weight:700;padding:6px 14px;border-radius:20px;border:1px solid}
.pill .dot{width:8px;height:8px;border-radius:50%;background:currentColor}
.pill.ok{color:#2ecc71;border-color:#2ecc71;background:rgba(46,204,113,.12)}
.pill.ok .dot{animation:pulse 2s infinite}
.pill.warn{color:#f5a623;border-color:#f5a623;background:rgba(245,166,35,.12)}
.pill.bad{color:#e74c3c;border-color:#e74c3c;background:rgba(231,76,60,.12)}
.pill.muted{color:#8a99a8;border-color:#8a99a8;background:rgba(138,153,168,.12)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.chip{font-size:.78rem;color:#9fb0c9;background:#16203a;padding:6px 14px;border-radius:20px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
.kpi{background:#16203a;border-radius:12px;padding:14px}
.kpi .v{font-size:1.7rem;font-weight:700;font-variant-numeric:tabular-nums}
.kpi .l{font-size:.75rem;color:#9fb0c9;margin-top:3px}
.card{background:#16203a;border-radius:12px;padding:16px;margin-bottom:12px}
.cohort-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px}
.cell{background:#0d1424;border-radius:8px;padding:10px;text-align:center}
.cell .k{font-size:.66rem;color:#9fb0c9;display:block}
.cell .v{font-size:1.15rem;font-weight:700;font-variant-numeric:tabular-nums}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.stat{display:flex;justify-content:space-between;font-size:.85rem;padding:3px 0}
.stat b{font-variant-numeric:tabular-nums}
.bar{height:8px;border-radius:4px;background:#0d1424;margin:4px 0 10px;overflow:hidden}
.bar i{display:block;height:100%;border-radius:4px;background:#e74c3c}
.bar.teal i{background:#2dd4bf}
.mono{font-family:ui-monospace,monospace;font-size:.8rem;word-break:break-all}
.muted{color:#8a99a8}
table{width:100%;border-collapse:collapse;font-size:.85rem}
td,th{padding:5px 6px;border-bottom:1px solid #22304f;text-align:left}
th{color:#9fb0c9;font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.05em}
.num{text-align:right;font-variant-numeric:tabular-nums}
.foot{font-size:.75rem;color:#8a99a8;text-align:center;margin-top:2px}
@media (max-width:700px){.kpis{grid-template-columns:repeat(2,1fr)}.cohort-grid{grid-template-columns:repeat(3,1fr)}.cols{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="topbar">
  <div class="title">XL-I2P Study 2</div>
  <span class="pill __LIVE_CLASS__" id="live-pill"><span class="dot"></span><b id="live-status">__LIVE_STATUS__</b></span>
  <span class="chip">epoch <b class="mono" id="epoch-label">__EPOCH_LABEL__</b> &middot; <span id="epoch-age">__AGE_DAYS__ days</span> old &middot; rollover in <span id="epoch-roll">__ROLLOVER_DAYS__</span>d</span>
  <span class="chip">last beat <span id="live-seen">__LIVE_SEEN__</span></span>
  <a class="navlink" href="/research__R_TOKEN__">Research archive &rarr;</a>
</div>

<div class="kpis">
  <div class="kpi"><div class="v" id="verify-n">__VERIFY__</div><div class="l">verify attempts &middot; <span id="verify-rate">__VERIFY_RATE__</span> ok</div></div>
  <div class="kpi"><div class="v" id="crawl-n">__CRAWL__</div><div class="l">crawl attempts &middot; <span id="crawl-rate">__CRAWL_RATE__</span> ok</div></div>
  <div class="kpi"><div class="v" id="pages-n">__PAGES__</div><div class="l">pages fetched &middot; <span id="links-n">__LINKS__</span> links</div></div>
  <div class="kpi"><div class="v"><span id="kpi-reachable">__REACHABLE_N__</span> / <span id="kpi-crawled">__CRAWLED_N__</span></div><div class="l">reachable / crawled sites</div></div>
</div>

<div class="card">
  <h2>Cohort &mdash; <span id="cohort-total">__COHORT_TOTAL__</span> sites &middot; <span id="new-sites">__NEW_SITES__</span> discovered this epoch</h2>
  <div class="cohort-grid">__STATE_CELLS__</div>
</div>

<div class="cols">
  <div class="card">
    <h2>Error taxonomy (top 5)</h2>
    __ERROR_BARS__
  </div>
  <div class="card">
    <h2>Cross-layer</h2>
    <div class="stat"><span>LeaseSet obs (epoch)</span><span></span></div>
    __XL_CLO_ROWS__
    <div class="stat" style="margin-top:8px"><span>Network obs by source (epoch)</span><span></span></div>
    __XL_NET_BARS__
    <div class="stat"><span>Cumulative cross-layer obs</span><b id="xl-clo-cum">__XL_CLO_CUM__</b></div>
    <div class="stat"><span>Cumulative network obs</span><b id="xl-net-cum">__XL_NET_CUM__</b></div>
  </div>
</div>

<div class="card">
  <h2>Churn</h2>
  __CHURN_HTML__
</div>

<div class="card">
  <h2>Health</h2>
  <div class="stat"><span>Stuck sites (VERIFYING/CRAWLING &gt; stale)</span><b id="stuck-n">__STUCK_COUNT__</b></div>
  <div class="stat"><span class="mono muted" id="stuck-hosts">__STUCK_HOSTS__</span><span></span></div>
  <h2 style="margin-top:12px">Recent heartbeats</h2>
  <table><thead><tr><th>At</th><th>Phase</th><th class="num">Cycles</th></tr></thead>
  <tbody id="beat-body">__BEAT_ROWS__</tbody></table>
</div>

<div class="foot" id="updated"></div>

<script>
const TOKEN_QS = "__TOKEN_QS__";
let boot = __STATS_JSON__;
function qs(){ return TOKEN_QS ? "?token=" + encodeURIComponent(TOKEN_QS) : ""; }
function set(id, v){ const el = document.getElementById(id); if (el && v !== undefined && v !== null) el.textContent = v; }
function pct(r){ return r == null ? "—" : (r*100).toFixed(1) + "%"; }
function pillClass(s){ return "pill " + ({ALIVE:"ok",STALE:"warn",DEAD:"bad"}[s] || "muted"); }
function apply(s){
  const e = s.epoch || {}, t = s.this_epoch || {}, c = s.cohort || {},
        bs = c.by_state || {}, l = s.liveness || {}, xl = s.cross_layer || {},
        xlc = xl.cumulative || {}, ch = s.churn || {}, h = s.health || {}, st = h.stuck_sites || {};
  const pill = document.getElementById("live-pill");
  if (pill) pill.className = pillClass(l.status);
  set("live-status", l.status);
  set("live-seen", l.last_seen ? new Date(l.last_seen + "Z").toLocaleString() : "never");
  set("epoch-label", e.label || "none");
  set("epoch-age", (e.age_days ?? "—") + " days");
  set("epoch-roll", e.days_until_rollover ?? "—");
  set("cohort-total", c.total ?? 0);
  for (const k in bs) set("st-" + k, bs[k]);
  set("kpi-reachable", bs["REACHABLE"] ?? 0);
  set("kpi-crawled", bs["CRAWLED"] ?? 0);
  set("verify-n", t.verify_attempts ?? "—");
  set("verify-rate", pct(t.verify_success_rate));
  set("crawl-n", t.crawl_attempts ?? "—");
  set("crawl-rate", pct(t.crawl_success_rate));
  set("pages-n", t.pages_fetched ?? "—");
  set("links-n", t.links_found ?? "—");
  set("new-sites", t.new_sites ?? "—");
  set("xl-clo-cum", xlc.cross_layer_observations ?? 0);
  set("xl-net-cum", xlc.network_observations ?? 0);
  set("stuck-n", st.count ?? 0);
  if (ch.available){ set("churn-new", ch.newly_reachable); set("churn-lost", ch.lost); }
  const up = document.getElementById("updated");
  if (up && s.generated_at) up.textContent = "updated " + new Date(s.generated_at + "Z").toLocaleTimeString();
}
if (boot && boot.generated_at) apply(boot);
async function refresh(){
  try {
    const r = await fetch("/api/stats" + qs());
    if (!r.ok) return;
    apply(await r.json());
  } catch (e) { /* keep last good values on a flaky link */ }
}
setInterval(refresh, 60000);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Research archive (/research): cumulative, history-oriented stats.
#
# The ops page (/) answers "is the crawler alive right now". This page
# answers "what has the study ever observed". It reads the same database
# (SELECT only) and is additive: nothing on / changes.
# ---------------------------------------------------------------------------

RESEARCH_FLAP_LIMIT = 10
RESEARCH_SITE_ATTEMPT_LIMIT = 200
RESEARCH_REFRESH_MS = 300_000  # 5 minutes; history moves slowly


def _ever_distinct(session, attempt_type: str, epoch_id: int | None) -> int:
    q = select(func.count(func.distinct(CrawlAttempt.site_id))).where(
        CrawlAttempt.attempt_type == attempt_type,
        CrawlAttempt.status == AttemptStatus.SUCCESS.value,
    )
    if epoch_id is not None:
        q = q.where(CrawlAttempt.epoch_id == epoch_id)
    return session.scalar(q) or 0


def _first_success_dates(session, attempt_type: str) -> list:
    rows = session.execute(
        select(func.min(CrawlAttempt.started_at))
        .where(
            CrawlAttempt.attempt_type == attempt_type,
            CrawlAttempt.status == AttemptStatus.SUCCESS.value,
        )
        .group_by(CrawlAttempt.site_id)
    ).all()
    return [r[0] for r in rows if r[0] is not None]


def _survival_series(dates: list, day0, days: int) -> list[int]:
    per_day = [0] * (days + 1)
    for d in dates:
        idx = (d - day0).days
        if 0 <= idx <= days:
            per_day[idx] += 1
    cum, out = 0, []
    for n in per_day:
        cum += n
        out.append(cum)
    return out


def _survival_svg(series: list[tuple], days: int) -> str:
    """Render cumulative survival curves as inline SVG. No JS chart lib needed."""
    W, H, PL, PB, PT = 600, 220, 46, 26, 12
    mx = max([v for _, s, _ in series for v in s] + [1])

    def x(i: int) -> float:
        return PL + (i / max(days, 1)) * (W - PL - 10)

    def y(v: int) -> float:
        return (H - PB) - (v / mx) * (H - PB - PT)

    grid = "".join(
        f'<line x1="{PL}" y1="{y(g):.1f}" x2="{W - 10}" y2="{y(g):.1f}" '
        f'stroke="#22304f" stroke-width="1"'
        + (' stroke-dasharray="4 4" opacity=".6"' if g else "")
        + f'/><text x="8" y="{y(g) + 4:.1f}" fill="#8a99a8" font-size="10" '
        f'font-family="monospace">{g}</text>'
        for g in (0, mx // 2, mx)
    )
    lines = []
    for _label, vals, color in series:
        pts = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(vals))
        lines.append(
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2.5"/>'
        )
        if vals:
            lines.append(
                f'<circle cx="{x(len(vals) - 1):.1f}" cy="{y(vals[-1]):.1f}" '
                f'r="4" fill="{color}"/>'
            )
    xlabels = (
        f'<text x="{PL}" y="{H - 8}" fill="#8a99a8" font-size="10" '
        f'font-family="monospace">day 0</text>'
        f'<text x="{W - 60}" y="{H - 8}" fill="#8a99a8" font-size="10" '
        f'font-family="monospace">day {days}</text>'
    )
    return (
        f'<svg class="chart" viewBox="0 0 {W} {H}" role="img">'
        f"{grid}{''.join(lines)}{xlabels}</svg>"
    )


def _flap_leaders(session, limit: int = RESEARCH_FLAP_LIMIT) -> list[dict]:
    """Sites with the most alive<->dead transitions, via a LAG window query."""
    rows = session.execute(
        text(
            """
            SELECT s.host AS host, SUM(t.changed) AS transitions, COUNT(*) AS n
            FROM (
                SELECT site_id,
                       CASE
                         WHEN status <> LAG(status) OVER (
                                PARTITION BY site_id ORDER BY started_at, id)
                         THEN 1 ELSE 0
                       END AS changed
                FROM crawl_attempts
                WHERE attempt_type IN ('VERIFY', 'CRAWL')
                  AND status IN ('SUCCESS', 'FAILED')
            ) t
            JOIN sites s ON s.id = t.site_id
            GROUP BY s.host
            HAVING transitions > 0
            ORDER BY transitions DESC, n DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).all()
    leaders = []
    for host, transitions, _n in rows:
        last = session.execute(
            select(CrawlAttempt.status)
            .where(
                CrawlAttempt.site_id == select(Site.id).where(Site.host == host).scalar_subquery(),
                CrawlAttempt.attempt_type.in_(
                    (AttemptType.VERIFY.value, AttemptType.CRAWL.value)
                ),
                CrawlAttempt.status.in_(
                    (AttemptStatus.SUCCESS.value, AttemptStatus.FAILED.value)
                ),
            )
            .order_by(CrawlAttempt.started_at.desc(), CrawlAttempt.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        leaders.append(
            {
                "host": host,
                "transitions": int(transitions or 0),
                "now": "alive" if last == AttemptStatus.SUCCESS.value else "dead",
            }
        )
    return leaders


def _epoch_ledger(session) -> list[dict]:
    """Per-epoch ever-counts plus lost/newly-reachable vs the previous epoch."""
    epochs = (
        session.execute(select(Epoch).order_by(Epoch.started_at.asc())).scalars().all()
    )
    good = (AttemptType.VERIFY.value, AttemptType.CRAWL.value)
    prev_ok: set | None = None
    rows = []
    for ep in epochs:
        cur_ok = set(
            session.execute(
                select(CrawlAttempt.site_id)
                .where(
                    CrawlAttempt.epoch_id == ep.id,
                    CrawlAttempt.status == AttemptStatus.SUCCESS.value,
                    CrawlAttempt.attempt_type.in_(good),
                )
                .distinct()
            ).scalars()
        )
        cur_attempted = set(
            session.execute(
                select(CrawlAttempt.site_id)
                .where(CrawlAttempt.epoch_id == ep.id)
                .distinct()
            ).scalars()
        )
        if prev_ok is None:
            lost, newly = None, None
        else:
            newly = len(cur_ok - prev_ok)
            lost = len((prev_ok - cur_ok) & cur_attempted)
        rows.append(
            {
                "label": ep.label,
                "status": ep.status,
                "ever_reachable": _ever_distinct(session, AttemptType.VERIFY.value, ep.id),
                "ever_crawled": _ever_distinct(session, AttemptType.CRAWL.value, ep.id),
                "lost": lost,
                "newly_reachable": newly,
            }
        )
        prev_ok = cur_ok
    return rows


def _collect_research(session) -> dict:
    now = _utcnow()
    epoch = session.execute(
        select(Epoch).where(Epoch.status == EpochStatus.OPEN.value)
    ).scalar_one_or_none()
    epoch_id = epoch.id if epoch else None

    cohort_total = session.scalar(select(func.count()).select_from(Site)) or 0
    ever_reachable = _ever_distinct(session, AttemptType.VERIFY.value, None)
    ever_crawled = _ever_distinct(session, AttemptType.CRAWL.value, None)
    xlayer_validated = (
        session.scalar(
            select(func.count(func.distinct(CrossLayerObservation.site_id))).where(
                CrossLayerObservation.leaseset_found.is_(True),
                CrossLayerObservation.site_id.is_not(None),
            )
        )
        or 0
    )
    discovered_via_links = (
        session.scalar(
            select(func.count())
            .select_from(Site)
            .where(Site.source == "crawl_discovery")
        )
        or 0
    )

    # Survival curves: day 0 = epoch start if known, else first success.
    reach_dates = _first_success_dates(session, AttemptType.VERIFY.value)
    crawl_dates = _first_success_dates(session, AttemptType.CRAWL.value)
    all_dates = reach_dates + crawl_dates
    if epoch and epoch.started_at:
        day0 = epoch.started_at
    elif all_dates:
        day0 = min(all_dates)
    else:
        day0 = now
    days = max(0, (now - day0).days)
    reach_series = _survival_series(reach_dates, day0, days)
    crawl_series = _survival_series(crawl_dates, day0, days)

    return {
        "generated_at": now.isoformat(),
        "epoch_label": epoch.label if epoch else None,
        "kpis": {
            "ever_reachable": ever_reachable,
            "ever_crawled": ever_crawled,
            "ever_reachable_this_epoch": _ever_distinct(
                session, AttemptType.VERIFY.value, epoch_id
            ),
            "ever_crawled_this_epoch": _ever_distinct(
                session, AttemptType.CRAWL.value, epoch_id
            ),
            "cohort_lifetime_pct": round(ever_reachable / cohort_total * 100, 2)
            if cohort_total
            else None,
            "cohort_total": cohort_total,
            "xlayer_validated": xlayer_validated,
            "discovered_via_links": discovered_via_links,
        },
        "survival": {
            "days": days,
            "svg": _survival_svg(
                [
                    ("ever reachable", reach_series, "#2dd4bf"),
                    ("ever crawled", crawl_series, "#2ecc71"),
                ],
                days,
            ),
            "final_reachable": reach_series[-1] if reach_series else 0,
            "final_crawled": crawl_series[-1] if crawl_series else 0,
        },
        "ledger": _epoch_ledger(session),
        "flap_leaders": _flap_leaders(session),
    }


def collect_research() -> dict:
    """Gather research-archive numbers with cheap, indexed SELECTs only."""
    session = db.SessionLocal()
    try:
        return _collect_research(session)
    finally:
        session.close()


def collect_site_history(session, host: str) -> dict | None:
    """Full per-site history for the research lookup box."""
    site = session.execute(select(Site).where(Site.host == host)).scalar_one_or_none()
    if site is None:
        return None
    attempts = (
        session.execute(
            select(CrawlAttempt)
            .where(CrawlAttempt.site_id == site.id)
            .order_by(CrawlAttempt.started_at.desc(), CrawlAttempt.id.desc())
            .limit(RESEARCH_SITE_ATTEMPT_LIMIT)
        )
        .scalars()
        .all()
    )
    pages = (
        session.scalar(
            select(func.count()).select_from(Page).where(Page.site_id == site.id)
        )
        or 0
    )
    links = (
        session.scalar(
            select(func.count()).select_from(Link).where(Link.source_site_id == site.id)
        )
        or 0
    )
    xlayer = (
        session.execute(
            select(CrossLayerObservation)
            .where(CrossLayerObservation.site_id == site.id)
            .order_by(CrossLayerObservation.observed_at.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
    return {
        "host": site.host,
        "state": site.state,
        "first_seen_at": site.first_seen_at.isoformat() if site.first_seen_at else None,
        "last_checked_at": site.last_checked_at.isoformat()
        if site.last_checked_at
        else None,
        "last_crawled_at": site.last_crawled_at.isoformat()
        if site.last_crawled_at
        else None,
        "success_count": site.success_count,
        "failure_count": site.failure_count,
        "discovery_method": site.discovery_method,
        "is_cross_layer": site.is_cross_layer,
        "pages_fetched": pages,
        "links_found": links,
        "attempts": [
            {
                "at": a.started_at.isoformat() if a.started_at else None,
                "type": a.attempt_type,
                "status": a.status,
                "error_type": a.error_type,
                "pages": a.pages_fetched,
                "links": a.links_found,
            }
            for a in attempts
        ],
        "cross_layer": [
            {
                "at": o.observed_at.isoformat() if o.observed_at else None,
                "method": o.lookup_method,
                "leaseset_found": o.leaseset_found,
                "canonical_b32": o.canonical_b32,
                "routing_key": o.routing_key,
            }
            for o in xlayer
        ],
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not settings.dashboard_token:
        log.warning(
            "DASHBOARD_TOKEN is not set — dashboard is open to anyone who can reach it. "
            "Intended for trusted internal networks only."
        )
    app = create_app()
    log.info("dashboard listening on %s:%d", settings.dashboard_host, settings.dashboard_port)
    app.run(host=settings.dashboard_host, port=settings.dashboard_port, use_reloader=False)


if __name__ == "__main__":
    main()
