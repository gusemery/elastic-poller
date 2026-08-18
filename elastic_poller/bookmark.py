"""Bookmark file persistence."""

from __future__ import annotations

import os

from elastic_poller import config

bookmark_dir = config.BOOKMARK_PATH.rstrip("/") if config.BOOKMARK_PATH else "."
bookmark_file = os.path.join(bookmark_dir, f"{config.EDWIN_ORG}.elastic.bookmark")


def get_bookmark() -> int:
    """Read the bookmark file. Creates the file with 0 if it does not exist."""
    os.makedirs(bookmark_dir, exist_ok=True)
    if not os.path.exists(bookmark_file):
        config.logger.info("Bookmark file not found, creating it")
        with open(bookmark_file, "w") as fh:
            fh.write("0")

    with open(bookmark_file, "r") as fh:
        return int(float(fh.read()))


def set_bookmark(bookmark: int) -> None:
    """Persist the bookmark as epoch milliseconds (last successfully sent event)."""
    os.makedirs(bookmark_dir, exist_ok=True)
    with open(bookmark_file, "w") as fh:
        fh.write(str(int(bookmark)))


# Legacy names kept for tests and external callers.
getBookmark = get_bookmark
setBookmark = set_bookmark
