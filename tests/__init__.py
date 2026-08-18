"""Test package — ensures the repository root is importable."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import elastic_poller

CONFIG_PATCH_ATTRS = {
    "ELASTIC_URL",
    "ELASTIC_INDEX",
    "ELASTIC_BATCH_SIZE",
    "ELASTIC_QUERY",
    "ELASTIC_USER",
    "ELASTIC_PASS",
    "ELASTIC_TOKEN",
    "ELASTIC_VERIFY_SSL",
    "ELASTIC_PIT_KEEP_ALIVE",
    "EDWIN_ORG",
    "EDWIN_ID",
    "EDWIN_TOKEN",
}
BOOKMARK_PATCH_ATTRS = {"bookmark_dir", "bookmark_file"}


def patch_target(name: str):
    """Return the module that owns a patchable elastic_poller setting."""
    if name == "send_event":
        return elastic_poller.delivery
    if name in BOOKMARK_PATCH_ATTRS:
        return elastic_poller.bookmark
    if name in CONFIG_PATCH_ATTRS:
        return elastic_poller.config
    return elastic_poller
