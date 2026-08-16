# SPDX-FileCopyrightText: 2025 LogicMonitor, Inc.
#
# SPDX-License-Identifier: LicenseRef-All-rights-reserved

"""Module providing logic sending HTTP requests to Dexda."""
import json
import logging
import os
import pathlib
import time
import traceback
import typing
import sys
from datetime import datetime, timedelta
import dotenv
import requests
import pydantic
import yaml
import common_event
from itertools import islice
from pathlib import Path
from typing import Iterable, List, Any

_logger = logging.getLogger(__name__)


class _DexdaAuth(pydantic.BaseModel, extra="forbid", strict=True):
    """Pydantic model for validating user-supplied Dexda auth config."""

    dexda_org: str
    client_id: str
    client_secret: str


class _DexdaAuthToken(typing.TypedDict):
    """Dexda Auth Token object."""

    access_token: str
    issued_token_type: str
    token_type: str
    expires_in: int
    expires_at: int
    now: int


class DexdaRequest:
    """Class for sending data to Dexda."""

    _FILE_DIR: str = "src/logicmonitor/dexda/common_event_integration_sdk/"
    
    _HEADERS: typing.Dict = {
        "Content-Type": "application/json",
        "Accepts": "application/json"
    }
    
    @classmethod
    def new_from_file(
        cls,
        auth_file_name: str,
        auth_file_path: typing.Optional[str] = None,
    ) -> "DexdaRequest":
        """Class method to start new DexdaRequest using config files.
        :param auth_file_name: Name of auth config file to use.
        :param auth_file_path: File path of config file to use
        (optional).
        :raises FileNotFoundError: Cannot find the config file using the
        name (and filepath, if passed) given.
        :raises ValueError: Missing one or more of the required values
        in auth config file.
        :returns: Instance of DexdaRequest started using provided files.
        """
        _file_path = (
            auth_file_path if auth_file_path is not None else cls._FILE_DIR
        )
        _afp = pathlib.Path(_file_path).joinpath(auth_file_name)
        try:
            _auth_yaml: dict = yaml.safe_load(_afp.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Unable to find auth config file\n"
                f'File name: "{auth_file_name}"\n'
                f'Path: "{_afp}"'
            ) from e
        _logger.debug(
            "Auth read from file: %s\nFull path: %s", auth_file_name, _afp
        )
        return cls.new_from_param(_auth_yaml, "FILE")

    @classmethod
    def new_from_env(
        cls,
    ) -> "DexdaRequest":
        """Class method to start new DexdaRequest using .env file.
        :raises ValueError: Missing one or more of the required values
        in the .env file.
        :returns: Instance of DexdaRequest started using .env file.
        """
        dotenv.load_dotenv()
        auth_dict: dict = {
            "dexda_org": os.environ.get("DEXDA_ORG"),
            "client_id": os.environ.get("CLIENT_ID"),
            "client_secret": os.environ.get("CLIENT_SECRET"),
        }
        return cls.new_from_param(auth_dict, ".ENV")

    @classmethod
    def new_from_param(
        cls,
        auth_dict: dict[str, str],
        init_type: typing.Union[str, None] = None,
    ) -> "DexdaRequest":
        """Class method to start new DexdaRequest using params.
        :param auth_dict: Dict containing Dexda Org and API key.
        :param init_type: Init type.
        :raises ValueError: Missing one or more of the required values
        in auth_dict param.
        :returns: Instance of DexdaRequest started using provided params.
        """
        auth_model: "_DexdaAuth" = _DexdaAuth.model_validate(obj=auth_dict)
        if init_type is None:
            init_type = "PARAM"
        return cls(auth_model, init_type)

    def __init__(self, auth_data: "_DexdaAuth", init_type: str) -> None:
        """
        :param auth_data: Dict containing Dexda Org, Client ID and
        Secret.
        :param init_type: Init type.
        """
        _logger.debug("init type: %s", init_type)
        self._client_data = {
            "client_id": auth_data.client_id,
            "client_secret": auth_data.client_secret,
        }
        self.portal_url = f"https://{auth_data.dexda_org}.dexda.ai"
        self._token_endpoint = f"{self.portal_url}/auth/token"
        self._data_endpoint = f"{self.portal_url}/integration/event/v1"
        self.access_token = self.retrieve_access_token()

    def retrieve_access_token(
        self,
    ) -> "_DexdaAuthToken":
        """Attempt to get access token from Dexda using client_id and
        client_secret.
        :raises RequestException: Error in HTTP request.
        :returns: Access token received from Dexda.
        """
        #print(self.portal_url)
        #print(self._client_data["client_id"] + "-" + self._client_data["client_secret"])
        try:
            response = requests.post(
                url=self._token_endpoint,
                data={"grant_type": "client_credentials", **self._client_data},
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accepts": "application/json",
                },
                timeout=30,
            )
            response.raise_for_status()
            # Only HTTP 200 is acceptable status code, anything else is raised
            # as an error, if not already raised by raise_for_status()
            if response.status_code != 200:
                raise requests.exceptions.RequestException
            return typing.cast(_DexdaAuthToken, response.json())
        except requests.exceptions.RequestException:
            _logger.error(
                "Exception in _get_access_token()\n%s",
               str(traceback.format_exc()),
            )
            raise

    def batched(self, iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
        """Yield successive `size`-item lists from *iterable*."""
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def writePayload(self, data: str):
        events = json.dumps(data, indent=4)
        timestamp = int(datetime.now().timestamp() * 1000)
        with open(f'bad_payloads/{timestamp}.json', 'w') as fh:
            fh.write(events)
        fh.close

    def send(
        self,
        access_token: str,
        data: typing.List[typing.Dict[str, typing.Union[str, int, typing.Dict[str, str]]]]
    ) -> bool:
        """Send data to Dexda.
        :param access_token: Access token received from Dexda.
        :param data: List of CEF dicts to send to Dexda.
        :raises ValueError: Error processing payload data.
        :raises RequestException: Error in HTTP request.
        :returns: Boolean indicating successful request.
        """

        batchcount = 100
        totalCount = 0
        all_succeeded = True
        for batch in self.batched(data, batchcount):
            totalCount = totalCount + len(batch)
            print('Batch (' + str(totalCount) + ')')

            auth_header = {"Authorization": f"Bearer {access_token.get('access_token')}"}
            headers = {**self._HEADERS, **auth_header}

            retry_max = 3
            retry_backoff = 5
            batch_succeeded = False
            response = None
            for attempt in range(retry_max):
                try:
                    response = requests.post(
                        url=self._data_endpoint,
                        data=json.dumps(batch),
                        headers=headers,
                        timeout=360
                    )
                    response.raise_for_status()
                    logging.info("Response status code: %s\n"
                        "Response body: %s",
                        response.status_code, response.json())
                    batch_succeeded = True
                    break
                except requests.exceptions.RequestException:
                    if response is not None and response.status_code == 422:
                        print("Error detected in payload data\n%s",
                                    response.json())
                        raise ValueError(response.json()) from None
                    # Unrecoverable client errors: persist payload and fail the send
                    if response is not None and 400 <= response.status_code < 500:
                        self.writePayload(batch)
                        all_succeeded = False
                        break
                    print("Payload: \n%s", batch)
                    print("Exception in send()\n%s",
                            str(traceback.format_exc()))
                    time.sleep(retry_backoff * (attempt + 1))

            if not batch_succeeded:
                all_succeeded = False
                if response is None or not (400 <= response.status_code < 500):
                    logging.error("Maximum retries exhausted for batch")
        return all_succeeded
