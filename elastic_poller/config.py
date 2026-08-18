"""Environment configuration for elastic-poller."""

from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv

import lm_logs

load_dotenv()

# Exit codes
OK = 0
ERROR_CODE_UNKNOWN = 1
ERROR_CODE_VALIDATION_FAILED = 2
ERROR_CODE_EVENT_MAPPING_FAILED = 3
ERROR_CODE_EVENT_DELIVERY_FAILED = 4
ERROR_CODE_HTTP = 5
ERROR_CODE_UNEXPECTED = 6

EDWIN_CREDENTIAL_VARS = (
    ("EDWIN_ORG", "DEXDA_ORG"),
    ("EDWIN_ID", "DEXDA_ID"),
    ("EDWIN_TOKEN", "DEXDA_TOKEN"),
)


def env_bool(name: str, default: bool = False) -> bool:
    """Return True when an environment variable is set to a truthy string."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    """Read an integer environment value without failing module import."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return -1


def getenv_alias(*names: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first non-empty environment variable from *names."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def edwin_org() -> Optional[str]:
    return getenv_alias("EDWIN_ORG", "DEXDA_ORG")


def edwin_client_id() -> Optional[str]:
    return getenv_alias("EDWIN_ID", "DEXDA_ID")


def edwin_client_token() -> Optional[str]:
    return getenv_alias("EDWIN_TOKEN", "DEXDA_TOKEN")


def has_edwin_credentials() -> bool:
    """True when org, client id, and token are available from either naming scheme."""
    return all(
        getenv_alias(edwin_name, legacy_name)
        for edwin_name, legacy_name in EDWIN_CREDENTIAL_VARS
    )


def missing_edwin_credential_names() -> list[str]:
    """Return human-readable names for any missing Edwin credential pair."""
    missing: list[str] = []
    for edwin_name, legacy_name in EDWIN_CREDENTIAL_VARS:
        if not getenv_alias(edwin_name, legacy_name):
            missing.append(f"{edwin_name} or {legacy_name}")
    return missing


# Elasticsearch
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASS = os.getenv("ELASTIC_PASS")
ELASTIC_TOKEN = os.getenv("ELASTIC_TOKEN")
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_BATCH_SIZE = env_int("ELASTIC_BATCH_SIZE", 500)
ELASTIC_INDEX = os.getenv("ELASTIC_INDEXS")
ELASTIC_QUERY = os.getenv("ELASTIC_QUERY", "*")
ELASTIC_VERIFY_SSL = env_bool("ELASTIC_VERIFY_SSL", default=True)
ELASTIC_PIT_KEEP_ALIVE = os.getenv("ELASTIC_PIT_KEEP_ALIVE", "5m")
ELASTIC_OVERLAP_MS = env_int("ELASTIC_OVERLAP_MS", 300000)
DEDUPE_MAX_RECORDS = env_int("DEDUPE_MAX_RECORDS", 250000)
DEDUPE_MAX_SIZE_MB = env_int("DEDUPE_MAX_SIZE_MB", 256)

# Edwin (EDWIN_* preferred; DEXDA_* env vars kept for compatibility)
PAUSE_INTERVAL = os.getenv("POLLER_INTERVAL", 240)
EDWIN_ORG = edwin_org()
EDWIN_ID = edwin_client_id()
EDWIN_TOKEN = edwin_client_token()

# Deprecated module-level aliases (prefer EDWIN_* above).
DEXDA_ORG = EDWIN_ORG
DEXDA_ID = EDWIN_ID
DEXDA_TOKEN = EDWIN_TOKEN

# Poller runtime
BOOKMARK_PATH = os.getenv("BOOKMARK_PATH")
DEBUG = env_bool("DEBUG", default=False)
LOG_ENABLED = env_bool("LOG", default=True)

# Optional LM Logs shipping
LM_LOGS_ENABLED = env_bool("LM_LOGS_ENABLED", default=False)
LM_LOGS_VERBOSE = env_bool("LM_LOGS_VERBOSE", default=False)
LM_LOGS_ACCOUNT = os.getenv("LM_LOGS_ACCOUNT") or edwin_org()
LM_LOGS_BEARER_TOKEN = os.getenv("LM_LOGS_BEARER_TOKEN")
LM_LOGS_RESOURCE_ID = os.getenv("LM_LOGS_RESOURCE_ID")
FAILED_PAYLOAD_PATH = os.getenv("FAILED_PAYLOAD_PATH")


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is invalid."""


def validate_config() -> None:
    """Validate deployment configuration before entering the poll loop."""
    errors: list[str] = []
    if not ELASTIC_URL:
        errors.append("ELASTIC_URL is required")
    if not ELASTIC_INDEX:
        errors.append("ELASTIC_INDEXS is required")
    if ELASTIC_BATCH_SIZE <= 0:
        errors.append("ELASTIC_BATCH_SIZE must be greater than zero")
    try:
        interval = int(PAUSE_INTERVAL)
        if interval < 0:
            raise ValueError
    except (TypeError, ValueError):
        errors.append("POLLER_INTERVAL must be a non-negative integer")
    if ELASTIC_OVERLAP_MS < 0:
        errors.append("ELASTIC_OVERLAP_MS must be non-negative")
    if DEDUPE_MAX_RECORDS <= 0:
        errors.append("DEDUPE_MAX_RECORDS must be greater than zero")
    if DEDUPE_MAX_SIZE_MB <= 0:
        errors.append("DEDUPE_MAX_SIZE_MB must be greater than zero")
    if ELASTIC_TOKEN and (ELASTIC_USER or ELASTIC_PASS):
        errors.append("Use ELASTIC_TOKEN or ELASTIC_USER/ELASTIC_PASS, not both")
    if not ELASTIC_TOKEN and bool(ELASTIC_USER) != bool(ELASTIC_PASS):
        errors.append("ELASTIC_USER and ELASTIC_PASS must be provided together")
    if not has_edwin_credentials():
        errors.append(
            "Missing Edwin credentials: " + ", ".join(missing_edwin_credential_names())
        )
    if LM_LOGS_ENABLED and not (LM_LOGS_ACCOUNT and LM_LOGS_BEARER_TOKEN):
        errors.append(
            "LM_LOGS_ACCOUNT and LM_LOGS_BEARER_TOKEN are required when "
            "LM_LOGS_ENABLED is true"
        )
    if errors:
        raise ConfigurationError("; ".join(errors))

logger = lm_logs.configure_logging(
    debug=DEBUG,
    log_enabled=LOG_ENABLED,
    lm_logs_enabled=LM_LOGS_ENABLED,
    lm_logs_account=LM_LOGS_ACCOUNT,
    lm_logs_bearer_token=LM_LOGS_BEARER_TOKEN,
    lm_logs_resource_id=LM_LOGS_RESOURCE_ID,
    lm_logs_verbose=LM_LOGS_VERBOSE,
)
