import base64
import json
import logging
from typing import Any

import requests
from airflow.models import Variable
from pendulum import datetime, duration, now, parse, Duration, DateTime

logger: logging.Logger = logging.getLogger(__name__)


def _set_talkdesk_api_auth_token() -> str:
    """
    Gets a new temporary Talkdesk API auth token by presenting the client ID
    and the client secret and stores the token and expiry in Airflow Variables.
    cf. https://docs.talkdesk.com/reference/oauthtoken-basic-cc
    and https://stackoverflow.com/questions/65187566
    """
    talkdesk_api_account_name: str = Variable.get("talkdesk_api_account_name")
    talkdesk_api_client_id: str = Variable.get("talkdesk_api_client_id")
    talkdesk_api_client_secret: str = Variable.get("talkdesk_api_client_secret")

    url: str = f"https://{talkdesk_api_account_name}.talkdeskid.com/oauth/token"
    signed_request: str = base64.b64encode(
        bytes(f"{talkdesk_api_client_id}:{talkdesk_api_client_secret}", "UTF-8")
    ).decode()
    headers: dict[str, str] = {
        "Authorization": f"Basic {signed_request}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    scopes: list[str] = [
        "data-reports:read",
        "data-reports:write",
        "record-lists:manage",
    ]
    data: dict[str, str] = dict(grant_type="client_credentials", scope=" ".join(scopes))
    http_post: dict[str, Any] = dict(url=url, headers=headers, data=data)

    logger.info(f"Requesting auth token from {url}")
    response: requests.Response = requests.post(**http_post)
    response.raise_for_status()
    response_json: dict[str, Any] = json.loads(response.content.decode())

    auth_token: str = response_json["access_token"]
    auth_seconds: int = response_json.get("expires_in", 599)
    auth_duration: Duration = duration(seconds=auth_seconds)
    auth_expires_at: DateTime = now("UTC") + auth_duration

    logger.info(
        f"Received TalkDesk auth token {auth_token[:10]}, "
        f"expires {auth_expires_at}."
    )
    Variable.set("talkdesk_api_auth_token", auth_token)
    Variable.set("talkdesk_api_auth_expires_at", auth_expires_at)
    return auth_token


def get_talkdesk_api_auth_token() -> str:
    """
    Checks expiry of current token in Airflow Variables and retrieves a new one
    if less than a minute remains.
    :return: Talkdesk auth token
    """
    auth_token: str = Variable.get("talkdesk_api_auth_token", default_var=None)
    expires_at: str = Variable.get("talkdesk_api_auth_expires_at", default_var=None)
    if (
        auth_token
        and expires_at
        and parse(expires_at) - duration(seconds=60) > now("UTC")
    ):
        logger.info(f"Reusing auth token {auth_token[:10]} until {expires_at}.")
    else:
        auth_token = _set_talkdesk_api_auth_token()

    return auth_token
