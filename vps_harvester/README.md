# VPS Floodfill Harvester — XL-I2P Study 2, Tier 2

Standalone network-layer sensor. A cheap cloud VPS with a **public IP** runs
Java I2P as a floodfill router; this harvester periodically scans the
router's `netDb/` directory (router infos + leaseSets), writes JSONL batches,
ships them to the Study 2 MariaDB host (VM2) over rsync/SSH, where
`ingest/ingest_netdb.py` loads them into `network_observations`.

This exists because our VM1 had no public IP, so its I2P router can never
be a floodfill. The 4-month application-layer crawl is unaffected and stays
on VM1; this sensor supplies the network-layer half of the cross-layer story.

**Source-type discipline (load-bearing for the dissertation's disclosure):**
- `vps_floodfill_netdb` — full-DHT view from this floodfill sensor.
- `local_netdb` — client-sampled view from the VM1 vantage router's own
  netDb store (recorded by the crawler itself, Tier 1).
Analysis must always filter on `source_type`; the two are never interchangeable.

## Layout

```
netdb_harvester/
  parse.py    # stdlib-only netDb binary parsing (vendored; no xl_i2p import)
  harvest.py  # incremental netDb scan -> JSONL batches (cursor: harvest_cursor.json)
  ship.py     # rsync new batches -> VM2 (cursor: shipped.json)
ingest/
  ingest_netdb.py   # VM2 side: JSONL -> network_observations (PyMySQL)
  requirements.txt
systemd/
  netdb-harvest.service / netdb-harvest.timer   # every 6h
  netdb-ship.service    / netdb-ship.timer      # hourly
tests/
  test_harvester.py   # synthetic fixtures; documents byte-layout assumptions
```

## VPS spin-up checklist (exact steps)

Target: one small Ubuntu 24.04 cloud VPS with a **public IPv4** acting only
as a floodfill sensor. Tested on a Hetzner CX23 (2 vCPU / 4 GB / 40 GB /
20 TB, ~$6.49/mo, Helsinki region — region/type availability varies, pick
whatever small instance your provider offers with a public IPv4). Do
Phase 0 a day early — provider account verification can take hours.

### Phase 0 — account, SSH key, VM2 outbound test
1. Sign up at https://www.hetzner.com/cloud and create a project. Complete
   any identity verification they ask for — don't leave this for Saturday.
2. Make sure you have an SSH key on your laptop:
   `ssh-keygen -t ed25519 -C "vps-floodfill"` (skip if you already have one).
   Add the public key in Hetzner console → Security → SSH Keys.
3. On **VM2**, test outbound internet (this decides the data path):
   ```bash
   curl -sI --max-time 10 https://example.com | head -1
   ```
   If that returns `200 OK`, VM2 can pull batches from the VPS (Phase 7A).
   If it times out, use the manual transfer path (Phase 7B).

### Phase 1 — create the server
In your provider's console, create one small Ubuntu 24.04 instance:
- **Public IPv4:** enabled (do not disable)
- **SSH keys:** select the key from Phase 0
- Name it `i2p-floodfill-01`, Create.

Note the IPv4 address. From your laptop:
```bash
VPS=<the IPv4, e.g. 203.0.113.10>
ssh root@$VPS
```

### Phase 2 — base OS
```bash
apt-get update && apt-get -y upgrade
apt-get install -y ufw rsync curl ca-certificates software-properties-common
ufw allow 22/tcp && ufw --force enable
ufw status verbose
```

### Phase 3 — install Java I2P (Ubuntu PPA)
The old `deb.i2p2.de` repo is dead; for Ubuntu the I2P project recommends
their PPA:
```bash
add-apt-repository -y ppa:i2p-maintainers/i2p
apt-get update
apt-get install -y i2p
```
Answer the installer prompts: run as user `i2psvc`, start on boot **yes**.
```bash
systemctl status i2p --no-pager | head -8
sleep 90
find /var/lib/i2p -maxdepth 3 -name "router.config" 2>/dev/null
ss -tlnp | grep 7657   # router console listening on localhost
```
Note the `router.config` path (usually `/var/lib/i2p/i2p-config/router.config`).
If `find` returns nothing, wait another minute and retry — it appears on
first successful start.

### Phase 4 — discover I2P's ports, open the firewall
```bash
ss -ulpn | grep java    # UDP port (SSU2 transport) — note the number
ss -tlnp | grep java    # TCP port (NTCP2 transport) — note the number
ufw allow <UDP-port>/udp
ufw allow <TCP-port>/tcp
ufw status verbose
systemctl restart i2p
```

### Phase 5 — console access, bandwidth, floodfill switch
From your laptop (keep this terminal open):
```bash
ssh -L 7657:localhost:7657 root@$VPS
```
Open `http://localhost:7657/` in your browser:
1. **Bandwidth** (Configuration page): set Inbound **2048** KB/s, Outbound
   **1024** KB/s. (At full tilt that's ~2.6 TB/mo — well under the 20 TB cap.)
2. Back on the VPS, flip the floodfill switch:
   ```bash
   CFG=$(find /var/lib/i2p -maxdepth 3 -name "router.config" 2>/dev/null | head -1)
   echo "config: $CFG"
   grep -q "floodfillParticipant" "$CFG" || echo "i2np.floodfillParticipant=true" >> "$CFG"
   systemctl restart i2p
   ```
3. Watch the console home page: Network goes **Testing → OK** (typically
   30–120 minutes on first run — do not proceed until it says OK).
   Confirm floodfill: the router's own info/sidebar shows the floodfill
   flag, and `journalctl -u i2p --since "2 hours ago" | grep -i floodfill`
   shows participation messages. Promotion can lag Network: OK by a few
   hours; that's normal.
4. Confirm the netDb directory is populating:
   `ls /var/lib/i2p/i2p-config/netDb | wc -l` (should climb into the hundreds+)

### Phase 6 — install the harvester
```bash
# from your laptop: clone the repo and copy just the harvester subdir
git clone https://github.com/joshberk/XL-I2P-Study2.git xl-i2p-study2
scp -r xl-i2p-study2/vps_harvester root@$VPS:/opt/netdb-harvester-tmp
ssh root@$VPS
mkdir -p /opt/netdb-harvester && cp -r /opt/netdb-harvester-tmp/* /opt/netdb-harvester/ && rm -rf /opt/netdb-harvester-tmp
cd /opt/netdb-harvester && cp .env.example .env
```
Edit `.env`:
```
NETDB_DIR=/var/lib/i2p/i2p-config/netDb   # verify with: ls <that dir>
SPOOL_DIR=/var/spool/netdb-harvester
SENSOR_ID=vps-ff-01
# leave VM2_* unset — the VPS cannot reach VM2 (no public IP on VM2's network),
# so the push ship timer stays OFF (see Phase 7)
```
```bash
mkdir -p /var/spool/netdb-harvester
chown -R i2psvc:i2psvc /opt/netdb-harvester /var/spool/netdb-harvester
# run harvest as the router's user so netDb is readable:
sed -i 's/^User=.*/User=i2psvc/; s/^Group=.*/Group=i2psvc/' systemd/netdb-harvest.service
# smoke test (harvest.py auto-loads .env from its own directory, so no
# manual env export is needed; run it the way the timer will):
cd /opt/netdb-harvester && sudo -u i2psvc python3 -m netdb_harvester.harvest
ls /opt/netdb-harvester/spool/   # batch-*.jsonl appears
# enable ONLY the harvest timer (ship timer stays disabled — nothing to push to):
cp systemd/netdb-harvest.service systemd/netdb-harvest.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now netdb-harvest.timer
systemctl list-timers | grep netdb
```

### Phase 7A — VM2 pulls batches (if Phase 0 outbound test passed)
On **VM2** as root:
```bash
ssh-keygen -t ed25519 -f /root/.ssh/netdb_pull -N "" -C "vm2-netdb-pull"
ssh-copy-id -i /root/.ssh/netdb_pull.pub root@<VPS-IP>
ssh -i /root/.ssh/netdb_pull -o BatchMode=yes root@<VPS-IP> true && echo PULL-OK
mkdir -p /var/spool/netdb-vps
# hourly pull:
crontab -l | { cat; echo '15 * * * * /usr/bin/rsync -az --timeout=120 -e "ssh -i /root/.ssh/netdb_pull -o BatchMode=yes -o StrictHostKeyChecking=accept-new" root@<VPS-IP>:/opt/netdb-harvester/spool/ /var/spool/netdb-vps/ >> /var/log/netdb-pull.log 2>&1'; } | crontab -
```
Then install the ingest exactly as in "VM2 side" below, with
`INGEST_SPOOL_DIR=/var/spool/netdb-vps`. Ingest is idempotent, so
re-pulled files are safe.

### Phase 7B — manual transfer (if VM2 has no outbound internet)
1. On your laptop: `scp -r root@<VPS-IP>:/var/spool/netdb-harvester/ ./netdb-batches/`
2. Move the new `batch-*.jsonl` files into VM2 via your hosting console's
   file transfer, into `/var/spool/netdb-vps/` (create it per Phase 7A).
3. The VM2 ingest cron (below) picks them up. Repeat weekly — or revisit
   7A if the network admins ever allow outbound.

### Phase 8 — verify, then walk away
1. On the VPS: `systemctl list-timers | grep netdb` shows the harvest timer;
   `/var/spool/netdb-harvester/` gains a new batch every 6 hours.
2. On VM2 after the first pull + ingest run:
   ```sql
   SELECT source_type, COUNT(*) FROM network_observations
   GROUP BY source_type;
   -- expect rows with source_type='vps_floodfill_netdb'
   ```
3. Then leave it alone. **Week 1 is warmup, not measurement** — don't cite
   the floodfill census until the router has seasoned 2–3 weeks. Epoch 1's
   network-layer story rests on Tier 1 (client-mode) until then.

## VM2 side

Prerequisite: the hourly pull cron from Phase 7A is landing batches in
`/var/spool/netdb-vps/`, and the `xl_i2p_ingest` MariaDB user exists
(see the VM2 MariaDB runbook).

**Important:** MariaDB on VM2 binds to the internal address
(`bind-address = 192.167.48.48`), *not* localhost. The ingest must use
`DB_HOST=192.167.48.48` — `127.0.0.1` will be refused.

### 1. Service user + install dir
```bash
sudo useradd -r -m -s /bin/bash xl-i2p-ingest
sudo mkdir -p /opt/xl-i2p/ingest
# transfer ingest/ingest_netdb.py and ingest/requirements.txt here via the portal
sudo chown -R xl-i2p-ingest:xl-i2p-ingest /opt/xl-i2p/ingest
```

### 2. Python env (Ubuntu 24.04 needs python3-venv; system pip is externally managed)
```bash
sudo apt-get install -y python3-venv
sudo -u xl-i2p-ingest python3 -m venv /opt/xl-i2p/ingest/.venv
sudo -u xl-i2p-ingest /opt/xl-i2p/ingest/.venv/bin/pip install -r /opt/xl-i2p/ingest/requirements.txt
```

### 3. Credentials (.env lives next to the script; the script auto-loads it)
```bash
sudo -u xl-i2p-ingest nano /opt/xl-i2p/ingest/.env
sudo chmod 600 /opt/xl-i2p/ingest/.env
```
Contents (`DB_PASSWORD` is the ingest password from `/root/.xl-i2p-db-passwords`):
```
DB_HOST=192.167.48.48
DB_PORT=3306
DB_USER=xl_i2p_ingest
DB_PASSWORD=<ingest password>
DB_NAME=xl_i2p_study2
INGEST_SPOOL_DIR=/var/spool/netdb-vps
```

### 4. Spool + log permissions
The pull cron (root) writes batches here; the ingest user needs to read them
and write its `ingested.json` cursor:
```bash
sudo chown xl-i2p-ingest:xl-i2p-ingest /var/spool/netdb-vps
sudo touch /var/log/netdb-ingest.log
sudo chown xl-i2p-ingest:xl-i2p-ingest /var/log/netdb-ingest.log
```

### 5. Smoke test
```bash
sudo -u xl-i2p-ingest /opt/xl-i2p/ingest/.venv/bin/python /opt/xl-i2p/ingest/ingest_netdb.py
sudo mysql -e "SELECT source_type, COUNT(*) FROM xl_i2p_study2.network_observations GROUP BY source_type;"
# expect rows with source_type='vps_floodfill_netdb'
```

### 6. Schedule (every 30 min, as xl-i2p-ingest)
```bash
sudo crontab -u xl-i2p-ingest -l | { cat; echo '*/30 * * * * /opt/xl-i2p/ingest/.venv/bin/python /opt/xl-i2p/ingest/ingest_netdb.py >> /var/log/netdb-ingest.log 2>&1'; } | sudo crontab -u xl-i2p-ingest -
sudo crontab -u xl-i2p-ingest -l
```

Ingest is idempotent (per-batch cursor + per-record dedup), so re-pulled or
overlapping files are safe.

## Tests

```bash
cd vps_harvester && python3 -m pytest tests/ -q
```
11 tests, stdlib only (no pymysql needed — the DB layer is faked).
Covers: routerInfo/leaseSet parsing incl. truncated files, incremental
cursor behavior, ship cursor with faked rsync, ingest idempotency and
epoch tagging.
