"""XL-I2P Study 2 command line interface."""
from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import settings
from .crawler import crawl_batch
from .db import init_db, session_scope
from .epochs import EpochError, close_epoch, get_open_epoch, list_epochs, open_epoch
from .export import export_epoch, export_graph_csv
from .janitor import run_janitor
from .logging_setup import setup_logging
from .proxy import fetch_test_eepsite, tcp_proxy_available, wait_for_proxy
from .scheduler import run_loop, run_once
from .seeds import admit_leaseset_discoveries, import_cross_layer_file, import_seed_file
from .stats import collect_stats
from .verifier import verify_batch

app = typer.Typer(help="XL-I2P Study 2 longitudinal I2P measurement crawler")
db_app = typer.Typer(help="Database commands")
epoch_app = typer.Typer(help="Epoch lifecycle commands")
seeds_app = typer.Typer(help="Seed commands")
cross_layer_app = typer.Typer(help="Cross-layer network-source commands")
proxy_app = typer.Typer(help="I2P proxy commands")
export_app = typer.Typer(help="Export commands")

app.add_typer(db_app, name="db")
app.add_typer(epoch_app, name="epoch")
app.add_typer(seeds_app, name="seeds")
app.add_typer(cross_layer_app, name="cross-layer")
app.add_typer(proxy_app, name="proxy")
app.add_typer(export_app, name="export")

console = Console()


@db_app.command("init")
def db_init() -> None:
    """Create database tables (fresh database)."""
    init_db()
    console.print("[green]Database tables initialized.[/green]")


@app.command("janitor")
def janitor_cmd(
    stale_minutes: int = typer.Option(settings.stale_minutes, help="Staleness threshold in minutes."),
) -> None:
    """Run the startup janitor once (recover stuck rows / orphaned attempts)."""
    setup_logging()
    with session_scope() as session:
        counts = run_janitor(session, stale_minutes=stale_minutes)
    console.print(counts)


@epoch_app.command("open")
def epoch_open(
    label: str = typer.Argument(..., help="Epoch label, e.g. 2026-Q4."),
    note: str = typer.Option("", help="Optional note recorded on the epoch."),
) -> None:
    """Open a new epoch (resets the cohort retry schedule for re-probing)."""
    setup_logging()
    with session_scope() as session:
        try:
            epoch = open_epoch(session, label, note=note or None)
        except EpochError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1)
    console.print(f"[green]Opened epoch '{epoch.label}' (id={epoch.id}).[/green]")


@epoch_app.command("close")
def epoch_close(label: str = typer.Argument(..., help="Epoch label to close.")) -> None:
    """Close an open epoch (do this before exporting)."""
    with session_scope() as session:
        try:
            close_epoch(session, label)
        except EpochError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1)
    console.print(f"[green]Closed epoch '{label}'.[/green]")


@epoch_app.command("list")
def epoch_list() -> None:
    """List epochs."""
    with session_scope() as session:
        epochs = list_epochs(session)
    table = Table(title="Epochs")
    table.add_column("ID")
    table.add_column("Label")
    table.add_column("Status")
    table.add_column("Started")
    table.add_column("Ended")
    for epoch in epochs:
        table.add_row(str(epoch.id), epoch.label, epoch.status,
                      str(epoch.started_at), str(epoch.ended_at))
    console.print(table)


@epoch_app.command("rollover")
def epoch_rollover(
    export_closed: bool = typer.Option(
        False, help="Export the closed epoch (CSVs + SQL dump + SHA-256 manifest)."),
    out_dir: Path = typer.Option(Path(settings.export_dir), help="Export root directory."),
) -> None:
    """Close the open epoch and open the next one if it reached EPOCH_DURATION_DAYS.

    Intended to run daily from the xl-i2p-epoch-rollover systemd timer; it is
    a no-op when the epoch is not due yet. The running crawler also calls this
    as a backstop, and switches to the new epoch on its next cycle.
    """
    from .epochs import rollover_if_due

    setup_logging()
    with session_scope() as session:
        before = get_open_epoch(session)
        before_label = before.label if before else None
        result = rollover_if_due(session)
    if result is None:
        console.print("[yellow]No open epoch; nothing to roll over.[/yellow]")
        return
    if before_label is not None and result.label == before_label:
        console.print(
            f"[green]Epoch '{result.label}' not due for rollover yet.[/green]")
        return
    console.print(
        f"[green]Rolled over: closed '{before_label}', opened '{result.label}' "
        f"(id={result.id}).[/green]")
    if export_closed and before_label:
        with session_scope() as session:
            manifest = export_epoch(session, before_label, str(out_dir))
        console.print(f"[green]Exported closed epoch. Manifest: {manifest}[/green]")


@seeds_app.command("import")
def seeds_import(
    path: Path,
    epoch_label: str = typer.Option("", help="Tag seed events with this epoch."),
) -> None:
    """Import .i2p/.b32.i2p seeds from a text file (deduplicated)."""
    with session_scope() as session:
        epoch_id = _epoch_id_for_label(session, epoch_label)
        inserted, seen = import_seed_file(session, path, epoch_id=epoch_id)
    console.print(f"Imported {inserted} new sites from {seen} valid seed lines.")


@cross_layer_app.command("import")
def cross_layer_import(
    path: Path,
    source_type: str = typer.Option("floodfill_netdb", help="Source label."),
    epoch_label: str = typer.Option("", help="Tag observations with this epoch."),
) -> None:
    """Import cross-layer/network-source observations."""
    with session_scope() as session:
        epoch_id = _epoch_id_for_label(session, epoch_label)
        inserted, seen, observations = import_cross_layer_file(
            session, path, source_type=source_type, epoch_id=epoch_id)
    console.print(
        f"Imported {inserted} new sites from {seen} observed hosts; "
        f"wrote {observations} network observation rows."
    )


@cross_layer_app.command("lookup")
def cross_layer_lookup(
    value: str = typer.Argument(..., help=".i2p hostname, raw b32, full .b32.i2p, or URL."),
    persist: bool = typer.Option(True, help="Persist the observation."),
    enqueue_all: bool = typer.Option(False, help="Enqueue candidate-only rows too."),
) -> None:
    """Resolve and validate one candidate using SAM/LeaseSet evidence."""
    from .cross_layer import lookup_candidate, persist_cross_layer_result

    result = lookup_candidate(value)
    if persist:
        with session_scope() as session:
            persist_cross_layer_result(session, result, source_detail="cli:cross-layer lookup",
                                       enqueue_all=enqueue_all)
    table = Table(title="Cross-layer Lookup")
    table.add_column("Field")
    table.add_column("Value")
    for key in ["input_value", "host", "canonical_b32", "lookup_method", "leaseset_found",
                "leaseset_hash", "leaseset_type", "routing_key", "published", "expires",
                "gateway_count", "floodfill_count", "confidence", "raw_error"]:
        table.add_row(key, str(getattr(result, key)))
    console.print(table)


@cross_layer_app.command("harvest-netdb")
def cross_layer_harvest_netdb(
    epoch_label: str = typer.Option("", help="Tag observations with this epoch."),
) -> None:
    """Harvest local netDB router infos (requires FLOODFILL_MODE=true)."""
    from .cross_layer import harvest_netdb

    with session_scope() as session:
        epoch_id = _epoch_id_for_label(session, epoch_label)
        result = harvest_netdb(session, epoch_id=epoch_id)
    console.print(result)


@cross_layer_app.command("census-local-netdb")
def cross_layer_census_local_netdb(
    epoch_label: str = typer.Option("", help="Tag observations with this epoch (default: open epoch)."),
) -> None:
    """Tier 1 client-mode census: one NetworkObservation per router in the
    local netDb store (sampled view, not the full DHT)."""
    from .cross_layer import census_local_netdb

    setup_logging()
    with session_scope() as session:
        if epoch_label:
            epoch_id = _epoch_id_for_label(session, epoch_label)
        else:
            epoch = get_open_epoch(session)
            epoch_id = epoch.id if epoch else None
        result = census_local_netdb(session, epoch_id)
    console.print(result)


xlayer_app = typer.Typer(help="Per-epoch cross-layer association pass (Tier 1)")
app.add_typer(xlayer_app, name="xlayer")


@xlayer_app.command("pass")
def xlayer_pass_cmd(
    limit: int = typer.Option(settings.xlink_per_cycle_limit, help="Max sites to associate."),
    epoch_label: str = typer.Option("", help="Epoch (default: open epoch)."),
) -> None:
    """Run one bounded cross-layer association pass for the epoch."""
    from .xlayer_pass import run_xlayer_pass

    setup_logging()
    with session_scope() as session:
        epoch = _resolve_epoch(session, epoch_label, resume=not epoch_label)
        result = run_xlayer_pass(session, epoch, limit)
    console.print(result)


@proxy_app.command("check")
def proxy_check() -> None:
    """Check local I2P HTTP proxy and optional test eepsite."""
    tcp_ok = tcp_proxy_available()
    console.print(f"TCP proxy check: {'OK' if tcp_ok else 'FAILED'}")
    ok, message = asyncio.run(fetch_test_eepsite())
    console.print(f"Test eepsite check: {'OK' if ok else 'FAILED'} - {message}")


def _epoch_id_for_label(session, epoch_label: str) -> int | None:
    if not epoch_label:
        return None
    from .epochs import get_epoch
    epoch = get_epoch(session, epoch_label)
    if epoch is None:
        console.print(f"[red]No such epoch '{epoch_label}'.[/red]")
        raise typer.Exit(1)
    return epoch.id


def _resolve_epoch(session, epoch_label: str, resume: bool):
    """Return the Epoch to run under, per --epoch-label / --resume flags."""
    if resume:
        epoch = get_open_epoch(session)
        if epoch is None:
            console.print("[red]No OPEN epoch to resume. Open one with 'epoch open'.[/red]")
            raise typer.Exit(1)
        return epoch
    if epoch_label:
        try:
            return open_epoch(session, epoch_label)
        except EpochError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1)
    epoch = get_open_epoch(session)
    if epoch is None:
        console.print("[red]No OPEN epoch. Use --epoch-label to open one or --resume.[/red]")
        raise typer.Exit(1)
    return epoch


@app.command("verify")
def verify(
    limit: int = typer.Option(50, help="Maximum number of sites to verify."),
    epoch_label: str = typer.Option("", help="Epoch to tag attempts with (default: open epoch)."),
) -> None:
    """Verify candidate eepsite reachability."""
    setup_logging()
    with session_scope() as session:
        epoch = _resolve_epoch(session, epoch_label, resume=not epoch_label)
        result = asyncio.run(verify_batch(session, limit, epoch))
    console.print(result)


@app.command("crawl")
def crawl(
    limit: int = typer.Option(10, help="Maximum number of sites to crawl."),
    epoch_label: str = typer.Option("", help="Epoch to tag attempts with (default: open epoch)."),
) -> None:
    """Crawl reachable eepsites."""
    setup_logging()
    with session_scope() as session:
        epoch = _resolve_epoch(session, epoch_label, resume=not epoch_label)
        result = asyncio.run(crawl_batch(session, limit, epoch))
    console.print(result)


@app.command("run")
def run(
    once: bool = typer.Option(False, help="Run one verify+crawl cycle and exit."),
    epoch_label: str = typer.Option("", help="Open and run under this new epoch label."),
    resume: bool = typer.Option(False, help="Resume the currently open epoch."),
    verify_limit: int = typer.Option(settings.verify_limit),
    crawl_limit: int = typer.Option(settings.crawl_limit),
    sleep_seconds: int = typer.Option(settings.sleep_seconds, help="Sleep between cycles."),
) -> None:
    """Run the scheduler: janitor, then verify+crawl cycles until stopped."""
    setup_logging()
    with session_scope() as session:
        janitor_counts = run_janitor(session)
        # A restart after long downtime must not resume a stale epoch: roll
        # it if it reached EPOCH_DURATION_DAYS while the crawler was down.
        from .epochs import rollover_if_due
        rollover_if_due(session)
        epoch = _resolve_epoch(session, epoch_label, resume=resume or not epoch_label)
    console.print(f"Janitor: {janitor_counts}")
    console.print(f"Running under epoch '{epoch.label}' (id={epoch.id}).")

    if not tcp_proxy_available():
        if settings.proxy_wait_on_startup:
            console.print(
                "[yellow]I2P HTTP proxy unavailable; waiting for it "
                "instead of exiting.[/yellow]")
            wait_for_proxy()
        else:
            console.print("[red]I2P HTTP proxy unavailable; refusing to start.[/red]")
            raise typer.Exit(1)

    if once:
        result = asyncio.run(run_once(epoch, verify_limit=verify_limit, crawl_limit=crawl_limit))
        console.print(result)
    else:
        asyncio.run(run_loop(epoch.label, epoch.id, verify_limit=verify_limit,
                             crawl_limit=crawl_limit, sleep_seconds=sleep_seconds))


@app.command("admit-leasesets")
def admit_leasesets(
    limit: int = typer.Option(500, help="Maximum new hosts to admit from the lease-set harvest."),
    epoch_label: str = typer.Option("", help="Epoch to tag discoveries with (default: open epoch)."),
) -> None:
    """Admit new .b32.i2p destinations seen in the VPS floodfill lease-set harvest."""
    setup_logging()
    with session_scope() as session:
        epoch = _resolve_epoch(session, epoch_label, resume=not epoch_label)
        result = admit_leaseset_discoveries(session, epoch.id, limit=limit)
    console.print(result)


@app.command("stats")
def stats(
    epoch_label: str = typer.Option("", help="Restrict observation counts to this epoch."),
) -> None:
    """Show crawler state summary."""
    with session_scope() as session:
        epoch_id = _epoch_id_for_label(session, epoch_label) if epoch_label else None
        data = collect_stats(session, epoch_id=epoch_id)
    table = Table(title="Crawler Stats")
    table.add_column("Metric")
    table.add_column("Value")
    for key in ["total_pages", "total_links", "total_attempts", "network_observations",
                "cross_layer_observations", "cross_layer_sites", "cross_layer_crawled"]:
        table.add_row(key.replace("_", " ").title(), str(data[key]))
    for source, count in sorted((data.get("source_counts") or {}).items(), key=lambda x: str(x[0])):
        table.add_row(f"Source:{source}", str(count))
    for state, count in sorted(data["state_counts"].items()):
        table.add_row(f"Sites:{state}", str(count))
    for error, count in sorted(data["site_errors"].items()):
        table.add_row(f"Error:{error}", str(count))
    console.print(table)


@export_app.command("epoch")
def export_epoch_cmd(
    label: str = typer.Argument(..., help="Epoch label to export."),
    out_dir: Path = typer.Option(Path(settings.export_dir), help="Export root directory."),
) -> None:
    """Export one epoch: CSVs + optional SQL dump + SHA-256 manifest."""
    setup_logging()
    with session_scope() as session:
        manifest = export_epoch(session, label, str(out_dir))
    console.print(f"[green]Epoch exported. Manifest: {manifest}[/green]")


@export_app.command("graph")
def export_graph(
    nodes: Path = typer.Option(Path("nodes.csv"), help="Output node CSV path."),
    edges: Path = typer.Option(Path("edges.csv"), help="Output edge CSV path."),
    epoch_label: str = typer.Option("", help="Restrict edges to this epoch."),
) -> None:
    """Export induced graph nodes and edges as CSV."""
    with session_scope() as session:
        epoch_id = _epoch_id_for_label(session, epoch_label) if epoch_label else None
        node_count, edge_count = export_graph_csv(session, nodes, edges, epoch_id=epoch_id)
    console.print(f"Exported {node_count} nodes to {nodes} and {edge_count} edges to {edges}.")


@app.callback(invoke_without_command=True)
def main(version: bool = typer.Option(False, "--version", help="Show version and exit.")) -> None:
    if version:
        console.print(__version__)
        raise typer.Exit()
