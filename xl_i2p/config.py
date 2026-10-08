"""Environment-driven configuration.

Every operational parameter is an environment variable with a default.
Defaults for the crawl loop reproduce the Study 1 validation parameters.
DB connection defaults to TCP on 127.0.0.1:3306 so VM1 can point at VM2
(e.g. DB_HOST=192.167.48.48) without code changes.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, fields
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # --- Database (TCP so VM1 can reach MariaDB on VM2) ---
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = _int("DB_PORT", 3306)
    db_user: str = os.getenv("DB_USER", "xl_i2p")
    db_password: str = os.getenv("DB_PASSWORD", "CHANGE_ME")
    db_name: str = os.getenv("DB_NAME", "xl_i2p_study2")

    # --- I2P ---
    i2p_http_proxy: str = os.getenv("I2P_HTTP_PROXY", "http://127.0.0.1:4444")
    user_agent: str = os.getenv("CRAWLER_USER_AGENT", "XL-I2P-Study2/2.0")
    known_test_eepsite: str = os.getenv("KNOWN_TEST_EEPSITE", "http://identiguy.i2p/")
    # Floodfill / netDB harvesting. Disabled by default: floodfill participation
    # requires a public IP and inbound UDP/TCP reachability, which our VM1
    # did not have. Enable only after Network: OK on a publicly reachable host.
    floodfill_mode: bool = _bool("FLOODFILL_MODE", False)

    # --- Epoch ---
    epoch_label: str = os.getenv("EPOCH_LABEL", "")
    # Automatic epoch rollover: when the open epoch reaches this age, it is
    # closed (and exported by the rollover timer) and a new epoch is opened.
    # This is the central mechanism of the longitudinal study — without it a
    # four-month run would produce one giant epoch and no churn signal.
    epoch_duration_days: int = _int("EPOCH_DURATION_DAYS", 30)
    epoch_auto_rollover: bool = _bool("EPOCH_AUTO_ROLLOVER", True)

    # --- I2P proxy startup behavior ---
    # If True, `run` waits for the I2P HTTP proxy to become available instead
    # of exiting when it is down at startup (e.g. router still integrating
    # after a reboot). The loop also skips cycles while the proxy is down.
    proxy_wait_on_startup: bool = _bool("PROXY_WAIT_ON_STARTUP", True)
    proxy_retry_seconds: int = _int("PROXY_RETRY_SECONDS", 60)

    # --- Crawl loop (Study 1 validation defaults) ---
    verify_limit: int = _int("VERIFY_LIMIT", 100)
    crawl_limit: int = _int("CRAWL_LIMIT", 20)
    sleep_seconds: int = _int("SLEEP_SECONDS", 300)
    max_concurrent_requests: int = _int("MAX_CONCURRENT_REQUESTS", 5)
    max_concurrent_sites: int = _int("MAX_CONCURRENT_SITES", 2)
    request_timeout_seconds: float = _float("REQUEST_TIMEOUT_SECONDS", 60)
    max_pages_per_site: int = _int("MAX_PAGES_PER_SITE", 25)
    max_depth_per_site: int = _int("MAX_DEPTH_PER_SITE", 2)
    crawl_delay_seconds: float = _float("CRAWL_DELAY_SECONDS", 2)
    max_content_bytes: int = _int("MAX_CONTENT_BYTES", 2_000_000)

    # --- Cross-layer Tier 1 (client-mode; works without a public IP) ---
    # Per-cycle bounded association pass (SAM naming + LeaseSet lookup).
    xlink_enabled: bool = _bool("XLINK_ENABLED", True)
    xlink_per_cycle_limit: int = _int("XLINK_PER_CYCLE_LIMIT", 20)
    # Local netDb census: one NetworkObservation per router in the vantage
    # router's own netDb store (client-sampled view, not the full DHT).
    netdb_census_enabled: bool = _bool("NETDB_CENSUS_ENABLED", True)
    netdb_census_interval_seconds: int = _int("NETDB_CENSUS_INTERVAL_SECONDS", 86400)
    # Lease-set discovery feed: admit previously unseen .b32.i2p destinations
    # published in the VPS floodfill sensor's lease-set harvest as new
    # DISCOVERED sites (bounded per run; the verify pass then determines
    # which are actually web services). Closes the link-only discovery blind
    # spot: most eepsites are isolated and never appear in outlinks.
    leaseset_discovery_enabled: bool = _bool("LEASESET_DISCOVERY_ENABLED", True)
    leaseset_discovery_interval_seconds: int = _int("LEASESET_DISCOVERY_INTERVAL_SECONDS", 3600)
    leaseset_discovery_per_run: int = _int("LEASESET_DISCOVERY_PER_RUN", 500)
    # Explicit path to the vantage router's netDb store. Needed when the
    # router runs as a different OS user than the crawler (e.g. router as
    # "administrator" -> /home/administrator/.i2p/netDb, crawler as xl-i2p).
    # The crawler user needs read+traverse rights on this directory.
    i2p_netdb_dir: str = os.getenv("I2P_NETDB_DIR", "")

    # --- Hardening ---
    stale_minutes: int = _int("STALE_MINUTES", 30)
    max_retries: int = _int("MAX_RETRIES", 5)
    backoff_base_seconds: float = _float("BACKOFF_BASE_SECONDS", 60)
    backoff_max_seconds: float = _float("BACKOFF_MAX_SECONDS", 86400)
    heartbeat_seconds: int = _int("HEARTBEAT_SECONDS", 60)
    log_dir: str = os.getenv("LOG_DIR", "logs")
    export_dir: str = os.getenv("EXPORT_DIR", "exports")

    # --- Dashboard (read-only, runs on VM2) ---
    # Bearer/?token= auth for the dashboard. Empty = open access; only leave
    # empty on a trusted internal network.
    dashboard_token: str = os.getenv("DASHBOARD_TOKEN", "")
    # 0.0.0.0 is private-LAN only: VM2 has no public IP. Prefer 127.0.0.1 and
    # reach it via SSH port-forward from outside the LAN.
    dashboard_host: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    dashboard_port: int = _int("DASHBOARD_PORT", 8080)

    @property
    def database_url(self) -> str:
        override = os.getenv("DATABASE_URL")
        if override:
            return override
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        return (
            f"mysql+pymysql://{user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}?charset=utf8mb4"
        )

    def snapshot(self) -> dict:
        """JSON-serializable copy of the effective config, stored per epoch."""
        data = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name == "db_password":
                value = "***"
            data[f.name] = value
        return data

    def snapshot_json(self) -> str:
        return json.dumps(self.snapshot(), sort_keys=True, default=str)


settings = Settings()
