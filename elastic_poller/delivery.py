"""Edwin event mapping and delivery."""

from __future__ import annotations

import time
from typing import Any, Dict, List

import common_event
import edwin_request
from elastic_poller import config


class EventMappingError(Exception):
    """Raised when an Elasticsearch hit cannot be mapped to CEF."""


class DeliveryError(Exception):
    """Raised when Edwin delivery cannot be completed."""


_client = None


def _get_client():
    global _client
    if _client is not None:
        token = _client.access_token
        expires_at = token.get("expires_at")
        if expires_at is None or float(expires_at) > time.time() + 60:
            return _client

    auth_dict = {
        "edwin_org": config.EDWIN_ORG,
        "client_id": config.EDWIN_ID,
        "client_secret": config.EDWIN_TOKEN,
    }
    _client = edwin_request.EdwinRequest.new_from_param(auth_dict=auth_dict)
    return _client


def create_event(payload: Dict[str, Any]):
    """Map a raw Elasticsearch hit to a CommonEvent using elastic_event_mappings.yaml."""
    try:
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name="elastic_event_mappings.yaml",
            mapping_file_path=".",
            original_record=payload,
        )
        config.logger.debug("Successfully mapped event payload to CEF")
    except Exception as error:
        config.logger.exception("Exception mapping Elasticsearch event")
        raise EventMappingError(str(error)) from error
    return event


def send_event(event_list: List[Dict[str, Any]]) -> bool:
    """Deliver a batch of CEF events to Edwin. Returns False if any batch fails."""
    global _client
    try:
        client = _get_client()
        access_token = client.access_token
        success = client.send(access_token=access_token, data=event_list)
        if not success:
            config.logger.error("send_event: one or more batches failed to deliver")
            _client = None
            return False
        config.logger.debug("Successfully sent Edwin events")
    except Exception as error:
        config.logger.exception("Exception delivering events to Edwin")
        raise DeliveryError(str(error)) from error
    return True


# Legacy name kept for tests and external callers.
createEvent = create_event
