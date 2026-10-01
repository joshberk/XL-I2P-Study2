from __future__ import annotations

from enum import Enum


class SiteState(str, Enum):
    NEW = "NEW"
    DISCOVERED = "DISCOVERED"
    VERIFYING = "VERIFYING"
    REACHABLE = "REACHABLE"
    UNREACHABLE = "UNREACHABLE"
    CRAWLING = "CRAWLING"
    CRAWLED = "CRAWLED"
    RETRY_READY = "RETRY_READY"
    ERROR = "ERROR"
    PAUSED = "PAUSED"


class AttemptType(str, Enum):
    VERIFY = "VERIFY"
    CRAWL = "CRAWL"
    HARVEST = "HARVEST"


class AttemptStatus(str, Enum):
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    INTERRUPTED = "INTERRUPTED"  # set by the janitor for attempts orphaned by a kill


class EpochStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class ErrorType(str, Enum):
    # Transport / proxy
    PROXY_UNAVAILABLE = "PROXY_UNAVAILABLE"
    PROXY_TIMEOUT = "PROXY_TIMEOUT"
    PROXY_ERROR = "PROXY_ERROR"
    DNS_ERROR = "DNS_ERROR"
    CONNECT_TIMEOUT = "CONNECT_TIMEOUT"
    READ_TIMEOUT = "READ_TIMEOUT"
    WRITE_TIMEOUT = "WRITE_TIMEOUT"
    CONNECTION_REFUSED = "CONNECTION_REFUSED"
    TLS_ERROR = "TLS_ERROR"
    # HTTP
    HTTP_4XX = "HTTP_4XX"
    HTTP_5XX = "HTTP_5XX"
    HTTP_ERROR = "HTTP_ERROR"  # legacy label, kept for compatibility
    # Content
    NON_HTML_CONTENT = "NON_HTML_CONTENT"
    CONTENT_TOO_LARGE = "CONTENT_TOO_LARGE"
    PARSER_ERROR = "PARSER_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    # I2P
    I2P_RESOLUTION_FAILED = "I2P_RESOLUTION_FAILED"
    # Internal
    DB_ERROR = "DB_ERROR"
    INTERRUPTED = "INTERRUPTED"
    # Last resort only
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
