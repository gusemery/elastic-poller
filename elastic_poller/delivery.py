"""Edwin event mapping and delivery."""

from __future__ import annotations

import sys
from typing import Any, Dict, List

import common_event
import edwin_request
from elastic_poller import config


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
        config.logger.error("Exception in createEvent: %s", error)
        sys.exit(config.ERROR_CODE_EVENT_MAPPING_FAILED)
    return event


def send_event(event_list: List[Dict[str, Any]]) -> bool:
    """Deliver a batch of CEF events to Edwin. Returns False if any batch fails."""
    auth_dict = {
        "edwin_org": config.EDWIN_ORG,
        "client_id": config.EDWIN_ID,
        "client_secret": config.EDWIN_TOKEN,
    }
    try:
        client = edwin_request.EdwinRequest.new_from_param(auth_dict=auth_dict)
        access_token = client.access_token
        success = client.send(access_token=access_token, data=event_list)
        if not success:
            config.logger.error("send_event: one or more batches failed to deliver")
            return False
        config.logger.debug("Successfully sent Edwin events")
    except Exception as error:
        config.logger.error("Exception in send_event: %s", error)
        sys.exit(config.ERROR_CODE_VALIDATION_FAILED)
    return True


# Legacy name kept for tests and external callers.
createEvent = create_event
