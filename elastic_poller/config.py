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
ELASTIC_BATCH_SIZE = int(os.getenv("ELASTIC_BATCH_SIZE", 500))
ELASTIC_INDEX = os.getenv("ELASTIC_INDEXS")
ELASTIC_QUERY = os.getenv("ELASTIC_QUERY", "*")
ELASTIC_VERIFY_SSL = env_bool("ELASTIC_VERIFY_SSL", default=False)
ELASTIC_PIT_KEEP_ALIVE = os.getenv("ELASTIC_PIT_KEEP_ALIVE", "5m")

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

logger = lm_logs.configure_logging(
    debug=DEBUG,
    log_enabled=LOG_ENABLED,
    lm_logs_enabled=LM_LOGS_ENABLED,
    lm_logs_account=LM_LOGS_ACCOUNT,
    lm_logs_bearer_token=LM_LOGS_BEARER_TOKEN,
    lm_logs_resource_id=LM_LOGS_RESOURCE_ID,
    lm_logs_verbose=LM_LOGS_VERBOSE,
)
