"""Read-only monitoring dashboard for XL-I2P Study 2.

Runs on VM2 (where MariaDB lives) and exposes the campaign stats for
on-the-go checks from a phone. The app NEVER writes to the database —
it only issues SELECTs through the existing engine/session factory.

Usage:
    python -m xl_i2p.dashboard

Environment:
    DASHBOARD_TOKEN  bearer/?token= auth; unset = open (trusted LAN only)
    DASHBOARD_HOST   bind address, default 0.0.0.0 (VM2 has no public IP,
                     so this is range-LAN only)
    DASHBOARD_PORT   default 8080
"""
from __future__ import annotations

import hmac
import json
import logging
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

    def _err_rows() -> str:
        rows = (s.get("this_epoch") or {}).get("error_top5") or []
        if not rows:
            return '<tr><td colspan="2" class="muted">no errors this epoch</td></tr>'
        return "".join(
            f'<tr><td class="mono">{r[0]}</td><td class="num" id="err-{r[0]}">{r[1]}</td></tr>'
            for r in rows
        )

    def _xl_rows(mapping: dict) -> str:
        if not mapping:
            return '<tr><td colspan="2" class="muted">—</td></tr>'
        return "".join(
            f'<tr><td class="mono">{k}</td><td class="num">{v}</td></tr>'
            for k, v in sorted(mapping.items())
        )

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
    html = html.replace("__ERROR_ROWS__", _err_rows())
    html = html.replace("__XL_CLO_EPOCH__", _xl_rows(xl_epoch.get("cross_layer_observations") or {}))
    html = html.replace("__XL_NET_EPOCH__", _xl_rows(xl_epoch.get("network_observations") or {}))
    html = html.replace("__XL_CLO_CUM__", str(xl_cum.get("cross_layer_observations", 0)))
    html = html.replace("__XL_NET_CUM__", str(xl_cum.get("network_observations", 0)))
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
    return html


def _pct(rate: float | None) -> str:
    return f"{rate * 100:.1f}%" if rate is not None else "—"


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XL-I2P Study 2 — Dashboard</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:12px;background:#101418;color:#e8edf2;max-width:720px;margin-inline:auto}
h1{font-size:1.15rem;margin:.2rem 0}
h2{font-size:.95rem;margin:1.1rem 0 .4rem;color:#9fb3c8;text-transform:uppercase;letter-spacing:.04em}
.card{background:#1a2129;border-radius:10px;padding:12px;margin-bottom:10px}
.row{display:flex;justify-content:space-between;padding:3px 0;font-size:.9rem}
.muted{color:#8a99a8}
.mono{font-family:ui-monospace,monospace;font-size:.82rem;word-break:break-all}
.num{text-align:right;font-variant-numeric:tabular-nums}
.ok{color:#4ade80}.warn{color:#fbbf24}.bad{color:#f87171}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(108px,1fr));gap:6px}
.cell{background:#232c36;border-radius:8px;padding:8px;display:flex;flex-direction:column}
.cell .k{font-size:.68rem;color:#9fb3c8}
.cell .v{font-size:1.05rem;font-weight:600}
table{width:100%;border-collapse:collapse;font-size:.85rem}
td,th{padding:4px 6px;border-bottom:1px solid #2a3440;text-align:left}
#updated{font-size:.75rem;color:#8a99a8}
</style>
</head>
<body>
<h1>XL-I2P Study 2 <span id="updated"></span></h1>

<div class="card">
<h2>Crawler</h2>
<div class="row"><span>Liveness</span><b class="__LIVE_CLASS__" id="live-status">__LIVE_STATUS__</b></div>
<div class="row"><span>Last heartbeat</span><span id="live-seen">__LIVE_SEEN__</span></div>
<div class="row"><span>Open epoch</span><b class="mono" id="epoch-label">__EPOCH_LABEL__</b></div>
<div class="row"><span>Epoch age</span><span id="epoch-age">__AGE_DAYS__ days</span></div>
<div class="row"><span>Days until auto-rollover</span><span id="epoch-roll">__ROLLOVER_DAYS__</span></div>
</div>

<div class="card">
<h2>Cohort (<span id="cohort-total">__COHORT_TOTAL__</span> sites)</h2>
<div class="grid">__STATE_CELLS__</div>
</div>

<div class="card">
<h2>This epoch</h2>
<div class="row"><span>Verify attempts</span><b id="verify-n">__VERIFY__</b></div>
<div class="row"><span>Verify success rate</span><b id="verify-rate">__VERIFY_RATE__</b></div>
<div class="row"><span>Crawl attempts</span><b id="crawl-n">__CRAWL__</b></div>
<div class="row"><span>Crawl success rate</span><b id="crawl-rate">__CRAWL_RATE__</b></div>
<div class="row"><span>Pages fetched</span><b id="pages-n">__PAGES__</b></div>
<div class="row"><span>Links found</span><b id="links-n">__LINKS__</b></div>
<div class="row"><span>New sites discovered</span><b id="new-sites">__NEW_SITES__</b></div>
<h2>Error taxonomy (top 5)</h2>
<table><tbody id="err-body">__ERROR_ROWS__</tbody></table>
</div>

<div class="card">
<h2>Cross-layer</h2>
<div class="row"><span>LeaseSet observations (epoch)</span><b id="xl-clo">see below</b></div>
<table><tbody id="xl-clo-body">__XL_CLO_EPOCH__</tbody></table>
<div class="row"><span>Network observations (epoch)</span><b></b></div>
<table><tbody id="xl-net-body">__XL_NET_EPOCH__</tbody></table>
<div class="row"><span>Cumulative cross-layer obs</span><b id="xl-clo-cum">__XL_CLO_CUM__</b></div>
<div class="row"><span>Cumulative network obs</span><b id="xl-net-cum">__XL_NET_CUM__</b></div>
</div>

<div class="card">
<h2>Churn</h2>
__CHURN_HTML__
</div>

<div class="card">
<h2>Health</h2>
<div class="row"><span>Stuck sites (VERIFYING/CRAWLING &gt; stale)</span><b id="stuck-n">__STUCK_COUNT__</b></div>
<div class="row"><span class="mono muted" id="stuck-hosts">__STUCK_HOSTS__</span><span></span></div>
<h2>Recent heartbeats</h2>
<table><thead><tr><th>At</th><th>Phase</th><th class="num">Cycles</th></tr></thead>
<tbody id="beat-body">__BEAT_ROWS__</tbody></table>
</div>

<script>
const TOKEN_QS = "__TOKEN_QS__";
let boot = __STATS_JSON__;
function qs(){ return TOKEN_QS ? "?token=" + encodeURIComponent(TOKEN_QS) : ""; }
function set(id, v){ const el = document.getElementById(id); if (el && v !== undefined && v !== null) el.textContent = v; }
function pct(r){ return r == null ? "—" : (r*100).toFixed(1) + "%"; }
function apply(s){
  const e = s.epoch || {}, t = s.this_epoch || {}, c = s.cohort || {},
        bs = c.by_state || {}, l = s.liveness || {}, xl = s.cross_layer || {},
        xle = xl.this_epoch || {}, xlc = xl.cumulative || {},
        ch = s.churn || {}, h = s.health || {}, st = h.stuck_sites || {};
  set("live-status", l.status);
  set("live-seen", l.last_seen ? new Date(l.last_seen + "Z").toLocaleString() : "never");
  set("epoch-label", e.label || "none");
  set("epoch-age", (e.age_days ?? "—") + " days");
  set("epoch-roll", e.days_until_rollover ?? "—");
  set("cohort-total", c.total ?? 0);
  for (const k in bs) set("st-" + k, bs[k]);
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
  if (up && s.generated_at) up.textContent = "· " + new Date(s.generated_at + "Z").toLocaleTimeString();
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
