# XL-I2P Study 2 — Hardened Longitudinal Crawler

Longitudinal I2P application-layer measurement instrument for a ~4-month
unattended campaign. Rebuilt from the Study 1 codebase with every empirically
confirmed failure mode fixed (see the "What changed vs Study 1" section below).

**Topology** (see `docs/architecture.png`, editable source in
`docs/architecture.drawio`):

| Host | Role | Address (example) |
|---|---|---|
| VM1 | Crawler; local I2P router kept for SAM LeaseSet lookups + client-sampled netDb census | 192.167.51.178 |
| VM2 | MariaDB + data analysis | 192.167.48.48 |
| VPS | I2P router (public IP, floodfill) — HTTP egress for the crawl (via SSH tunnel) + floodfill netDb sensor | 46.62.224.1 |

The crawler on VM1 writes **live** to MariaDB on VM2 over the internal
subnet (TCP 3306). Per-epoch immutable exports (CSV + optional SQL dump +
SHA-256 manifest) are frozen on VM2 for analysis. The crawler's HTTP
fetches egress through the VPS router's HTTP proxy over a persistent SSH
tunnel (see "Crawl egress" below); VM1's own I2P router is retained only
for SAM LeaseSet lookups and its client-sampled netDb census.

**Repository layout**

```
xl_i2p/            # crawler + read-only dashboard (xl_i2p/dashboard.py)
tests/             # 62 tests, no real network (httpx mocked, SQLite)
systemd/           # crawler, dashboard, and epoch-rollover units
schema.sql         # MariaDB schema (9 tables + 2 views)
.env.example       # copy to .env — every setting documented, sane defaults
requirements.txt   # Python 3.10+
vps_harvester/     # VPS floodfill netDb sensor (own README + tests)
docs/              # architecture diagram (png + editable drawio)
seeds.example.txt  # seed-file format: one .i2p host per line
```

**Requirements:** Python 3.10+, Java I2P router with HTTP proxy + SAM
enabled, MariaDB 10.6+.

## What changed vs Study 1 (empirical fixes)

- **Startup janitor** (`xl_i2p/janitor.py`): `VERIFYING`/`CRAWLING` rows stale
  longer than `STALE_MINUTES` return to `RETRY_READY`; orphaned `STARTED`
  attempts become `INTERRUPTED`. Study 1 left 2 sites stuck in `CRAWLING`
  (2,877 wasted attempts) + 2 orphaned attempts.
- **Exponential backoff with jitter** (`xl_i2p/retry.py`): failures schedule
  `next_retry_at`; after `MAX_RETRIES` a site goes terminal for the epoch
  (`UNREACHABLE` / `ERROR`). Study 1 had no backoff.
- **Real error taxonomy** (`xl_i2p/taxonomy.py`): `DNS_ERROR`,
  `CONNECT_TIMEOUT`, `READ_TIMEOUT`, `CONNECTION_REFUSED`, `HTTP_4XX`,
  `HTTP_5XX`, `TLS_ERROR`, `PROXY_ERROR`, `PARSE_ERROR`,
  `CONTENT_TOO_LARGE`; `UNKNOWN_ERROR` is the last resort. Study 1 logged
  3,463/3,463 crawl failures as `UNKNOWN_ERROR`.
- **Seed dedup** (`xl_i2p/seeds.py`): `seed_events` upserts on
  `(host, source_type, source_key)`; repeats bump `count`. Study 1 wrote
  400,720 rows (one host: 17,993 duplicates).
- **Epoch model** (`xl_i2p/models.py`, `xl_i2p/epochs.py`): `epochs` table;
  `epoch_id` on attempts/pages/links/seed_events/observations. Pages are
  keyed per `(normalized_url, epoch_id)` so re-crawls are new observations.
  Opening an epoch resets the cohort retry schedule → the same reachable
  cohort is re-probed and re-crawled every epoch (Study 1 was additive, not
  longitudinal).
- **Exception containment**: per-site `try/except` + `asyncio.gather(
  return_exceptions=True)`; fresh SQLAlchemy session per site and per
  cycle — no long-lived session.
- **JSON-lines file logs + heartbeat** (`xl_i2p/logging_setup.py`):
  daily-rotated logs and `heartbeat.json` (also a `heartbeats` DB row).
- **Floodfill gating** (`xl_i2p/cross_layer.py::harvest_netdb`): netDB
  harvesting is disabled unless `FLOODFILL_MODE=true`. Floodfill needs a
  public IP + inbound UDP/TCP (Network: OK); our VM1 had no public IP,
  so the default build is client-mode only.

## Deployment runbook

Three machines: **VM2** (MariaDB + dashboard), **VM1** (crawler + local
I2P router), **VPS** (floodfill sensor + HTTP egress for the crawl).
Deploy in that order.

### VM2 — MariaDB

```sql
CREATE DATABASE xl_i2p_study2 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- least-privilege crawler user: reachable only from VM1's internal IP
CREATE USER 'xl_i2p_crawler'@'192.167.51.178' IDENTIFIED BY '<strong password>';
GRANT SELECT, INSERT, UPDATE, DELETE ON xl_i2p_study2.* TO 'xl_i2p_crawler'@'192.167.51.178';
-- read-only analyst (local analysis on VM2)
CREATE USER 'xl_i2p_analyst'@'localhost' IDENTIFIED BY '<strong password>';
GRANT SELECT ON xl_i2p_study2.* TO 'xl_i2p_analyst'@'localhost';
FLUSH PRIVILEGES;
```

`server.cnf`: `bind-address = 192.167.48.48` (internal interface only).
Firewall: allow TCP 3306 **from 192.167.51.178 only**, e.g.
`ufw allow from 192.167.51.178 to any port 3306`.

Then load the schema: `mysql xl_i2p_study2 < schema.sql`
(or `python -m xl_i2p db init` from VM1 once connectivity works).

### VM1 — crawler (+ local I2P router)

```bash
sudo useradd -r -m -s /bin/bash xl-i2p
sudo mkdir -p /opt/xl-i2p && sudo chown xl-i2p:xl-i2p /opt/xl-i2p
# as xl-i2p (or any user with read access to the install dir):
sudo -u xl-i2p git clone https://github.com/joshberk/XL-I2P-Study2.git /opt/xl-i2p
sudo apt-get install -y python3-venv   # Ubuntu 24.04: system pip is externally managed
sudo -u xl-i2p python3 -m venv /opt/xl-i2p/.venv
sudo -u xl-i2p /opt/xl-i2p/.venv/bin/pip install -r /opt/xl-i2p/requirements.txt
sudo -u xl-i2p cp /opt/xl-i2p/.env.example /opt/xl-i2p/.env
# then edit /opt/xl-i2p/.env: DB_HOST=<VM2 internal IP>, DB_PASSWORD=...
```

1. VM1's own router no longer carries crawl traffic — it only serves SAM
   LeaseSet lookups and the client-sampled netDb census — so the crawl does
   not wait on its integration. What gates the measurement window: the VPS
   router seasoned 2–3 weeks (Network: OK, stable peer count) and the SSH
   tunnel healthy (step 4 below).
2. `python -m xl_i2p proxy check` — proxy must be OK.
3. Import seeds: `python -m xl_i2p seeds import seeds.txt`
4. Open epoch 1: `python -m xl_i2p epoch open 2026-Q4`
5. Smoke test: `python -m xl_i2p run --once --resume`
6. Install + start the service:
   `sudo cp systemd/xl-i2p-crawler.service /etc/systemd/system/`
   `sudo systemctl daemon-reload && sudo systemctl enable --now xl-i2p-crawler`
7. Watch `tail -f /opt/xl-i2p/logs/xl-i2p.log` and `heartbeat.json`.

The service runs `run --resume`, so it attaches to the open epoch after
any reboot. If no epoch is open it waits for one to be opened (it does not
die). The first epoch must be opened manually (`epoch open <label>`);
after that, rollover is automatic — see below.

### VPS — floodfill sensor + crawl egress

The network-layer half of the cross-layer story runs on a cheap cloud VPS
with a public IP — our VM1 had no public IP, so it can never be a floodfill. The harvester scans the VPS router's `netDb/` every 6 hours,
ships JSONL batches to VM2, and the ingest cron loads them into
`network_observations` as `source_type='vps_floodfill_netdb'`.

Full provisioning runbook — VPS spin-up, Java I2P install, floodfill
switch, harvester install, VM2 pull + ingest cron, verification — is in
[`vps_harvester/README.md`](vps_harvester/README.md). **Season the VPS 2–3
weeks before the measurement window**: week-1 census data is warmup, not
measurement.

### Crawl egress: via the VPS router (SSH tunnel)

VM1's router never got a public IP and stayed `Testing`/NAT'd, so the
crawler's HTTP fetches ride the VPS router's tunnels through a persistent
SSH tunnel — no code changes, one env var.

**Measurement-vantage disclosure.** All reachability measurements
(`crawl_attempts`, pages, links) are single-vantage: the VPS router. The
floodfill census (`vps_floodfill_netdb`) comes from that same router's
network position, so crawl observations and floodfill observations are
*not* independent vantages. What stays dual-source is the
network-observation layer: VM1's client-sampled netDb view
(`local_netdb`) vs the VPS floodfill's DHT view
(`vps_floodfill_netdb`) — genuinely different sensors, analyzed
separately (see "Source-type discipline" below).

1. On VM1, generate a dedicated key as the crawler user and note its
   fingerprint:
   `ssh-keygen -t ed25519 -f ~/.ssh/vps_tunnel -N "" -C "vm1-crawler-tunnel"`
2. Append the public key to the VPS login user's `~/.ssh/authorized_keys`,
   then verify with `ssh-keygen -l -f`: the fingerprint must match VM1's
   before going further.
3. On VM1, run a persistent tunnel forwarding `127.0.0.1:4445` to the
   VPS's `127.0.0.1:4444` (the VPS proxy port stays localhost-only, never
   exposed to the internet).
4. Verify: `curl -x http://127.0.0.1:4445 -I http://i2p-projekt.i2p/`
   should return HTTP headers.
5. Set `I2P_HTTP_PROXY=http://127.0.0.1:4445` in the crawler `.env` — put
   any comment on its own line, systemd env files do not strip trailing
   comments — and restart the crawler. This is the permanent data path,
   not a shakedown workaround.

### Epoch rollover (automatic)

Epochs roll automatically every `EPOCH_DURATION_DAYS` (default 30):

```bash
sudo cp systemd/xl-i2p-epoch-rollover.service systemd/xl-i2p-epoch-rollover.timer /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now xl-i2p-epoch-rollover.timer
```

The timer runs `epoch rollover --export-closed` daily; it is a no-op until
the open epoch reaches its max age, then it closes the epoch, exports it
(CSVs + SQL dump + SHA-256 manifest), and opens the next one (label
auto-incremented, e.g. `2026-Q4` → `2026-Q4-02`). The running crawler
detects the rollover on its next cycle and switches to the new epoch, and
it also rolls over by itself as a backstop if the timer ever misses.
Set `EPOCH_AUTO_ROLLOVER=false` to return to fully manual epochs.

Manual rollover (if ever needed):

```bash
python -m xl_i2p epoch close 2026-Q4
python -m xl_i2p export epoch 2026-Q4        # CSVs + manifest (+ SQL dump if mysqldump exists)
python -m xl_i2p epoch open 2027-Q1          # resets cohort for re-probing
# no restart needed: the running crawler picks up the new open epoch next cycle
```

Analyze the frozen exports under `EXPORT_DIR/epoch_<label>_<ts>/`;
verify `manifest.json` SHA-256 hashes before analysis.

## Dashboard (read-only, runs on VM2)

A dark "Mission Control" ops console for watching the campaign live.
It runs next to MariaDB on VM2, reads the live database **read-only**
(SELECTs only, never writes), and shows:

- **Status bar**: crawler liveness (ALIVE/STALE/DEAD, pulsing pill), open
  epoch label, epoch age, days until auto-rollover, last heartbeat.
- **KPI cards**: verify/crawl attempts + success rates, pages fetched,
  links found.
- **Cohort grid**: site counts across all 10 states.
- **Error taxonomy bars** and **cross-layer panel**: top errors, epoch-loop
  LeaseSet observations, `local_netdb` and `vps_floodfill_netdb` network
  observations (this epoch + cumulative).
- **Churn**: newly reachable vs lost sites, from epoch 2 onward.
- **Health**: recent heartbeats and currently-stuck VERIFYING/CRAWLING sites.
- A nav link to the **Research archive** (see below).

Numbers auto-refresh every 60s; no external assets (renders over a slow link).
`/api/stats` returns the same numbers as JSON for programmatic checks;
`/healthz` is an unauthenticated DB-connectivity probe.

**Research archive** (`/research`): a second, history-oriented page for analysis
instead of operations — cumulative "ever reachable / ever crawled" counters,
cohort lifetime reach, a per-epoch ledger (lost vs newly reachable), a survival
curve, the discovery funnel, flap leaders (most alive↔dead transitions), and a
per-site timeline lookup that replaces routine SQL lookups. Same token auth,
same service; `/api/research` and `/api/research/site?host=` serve its JSON.

### Install on VM2

1. Create a **read-only** MariaDB user (least privilege):
```sql
CREATE USER 'xl_i2p_dashboard'@'localhost' IDENTIFIED BY '<strong password>';
GRANT SELECT ON xl_i2p_study2.* TO 'xl_i2p_dashboard'@'localhost';
FLUSH PRIVILEGES;
```
2. Write `/opt/xl-i2p/dashboard.env` (separate from the crawler `.env`,
   because the credentials differ):
```
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=xl_i2p_dashboard
DB_PASSWORD=<strong password>
DB_NAME=xl_i2p_study2
DASHBOARD_TOKEN=<long random token>
DASHBOARD_HOST=0.0.0.0
DASHBOARD_PORT=8080
```
3. Copy `systemd/xl-i2p-dashboard.service` to `/etc/systemd/system/`,
   then `systemctl daemon-reload && systemctl enable --now xl-i2p-dashboard`.

### Access

- `DASHBOARD_HOST=0.0.0.0` is **private-LAN only**: VM2 has no public IP, so
  the dashboard is reachable from inside the private network at
  `http://192.167.48.48:8080/?token=<token>`.
- For off-network access (e.g. from a phone outside the LAN), do NOT expose
  the port — use an SSH tunnel instead:
  `ssh -L 8080:localhost:8080 <vm2-user>@<vm2>`, then open
  `http://localhost:8080/?token=<token>` locally.
- Prefer `DASHBOARD_HOST=127.0.0.1` if only SSH-tunnel access is needed.

## CLI reference

```
python -m xl_i2p db init                 # create tables
python -m xl_i2p janitor                 # run startup janitor once
python -m xl_i2p epoch open|close|list
python -m xl_i2p seeds import <file>
python -m xl_i2p cross-layer import|lookup|harvest-netdb
python -m xl_i2p proxy check
python -m xl_i2p verify|crawl [--epoch-label L]
python -m xl_i2p run [--once] [--epoch-label L | --resume]
python -m xl_i2p stats [--epoch-label L]
python -m xl_i2p export epoch <label>
python -m xl_i2p export graph
python -m xl_i2p --version
```

## Tests

`python -m pytest tests/ -q` — 62 tests, no real network (httpx mocked,
SQLite). Covers janitor recovery, backoff, taxonomy, epoch tagging and
rollover, seed dedup, page-cap strictness, kill-mid-cycle restart
simulation, the cross-layer association loop (pass selection,
SAM-down degradation, per-site error isolation, local netDb census dedup
and malformed-file handling), netDb path resolution (override, fallback,
unreadable-dir skip), and the dashboard API.

## Floodfill / cross-layer netDB harvesting

Our VM1 had no public IP, so its I2P router can never be a floodfill — hence
the separate VPS cloud server, which also carries the crawler's HTTP
egress (see "Crawl egress" above). Network-layer collection therefore
runs on two sensors:

**VM1 router — SAM lookups + client-sampled netDb census (no public IP needed).**
- Per-cycle association pass (`xl_i2p/xlayer_pass.py`): every scheduler
  cycle, up to `XLINK_PER_CYCLE_LIMIT` (default 20) REACHABLE/CRAWLED sites
  lacking a cross-layer observation for the current epoch get a SAM naming +
  LeaseSet lookup against VM1's router, persisted with
  `source_detail='epoch-loop:…'` and the epoch tagged. Degrades gracefully
  when SAM is down. Toggle with `XLINK_ENABLED`. Manual run:
  `python -m xl_i2p xlayer pass`.
- Local netDb census (`cross_layer.census_local_netdb`): one
  `NetworkObservation` per router in VM1's own netDb store,
  `source_type='local_netdb'`, with the sampled-view disclosure in
  `source_detail`. Runs at most every `NETDB_CENSUS_INTERVAL_SECONDS`
  (default 86400). Manual run: `python -m xl_i2p cross-layer census-local-netdb`.
  Set `I2P_NETDB_DIR` to the router's netDb path when the router runs as a
  different OS user than the crawler (e.g. `I2P_NETDB_DIR=/home/administrator/.i2p/netDb`);
  the crawler user needs read+traverse rights on that directory.

**VPS — floodfill sensor** (`vps_harvester/`, separate zip): a cheap
cloud VM with a public IP runs I2P as floodfill; `netdb_harvester/harvest.py`
scans netDb → JSONL batches (6-hour timer), `ship.py` rsyncs them to VM2
(hourly), and `ingest/ingest_netdb.py` loads them into
`network_observations` with `source_type='vps_floodfill_netdb'`. See
`vps_harvester/README.md` for the provisioning runbook. **Season the VPS
for weeks before the measurement window** — week-1 census data is warmup.

**Source-type discipline:** `local_netdb` (VM1 client-sampled) vs
`vps_floodfill_netdb` (VPS floodfill, full DHT). Analysis must filter on
`source_type`; the two are never interchangeable.

The old floodfill-gated `harvest_netdb()` (`FLOODFILL_MODE`) remains for a
future floodfill-capable vantage host and stays disabled here.

## License

MIT — see `LICENSE`.
