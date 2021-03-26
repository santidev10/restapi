import logging

from django.conf import settings
from googleads import adwords
from googleads import oauth2
from googleads.common import ZeepServiceProxy
import requests

from aw_reporting.adwords_api import load_web_app_settings

logger = logging.getLogger(__name__)
API_VERSION = "v201809"


def load_client_settings():
    conf = {
        "user_agent": settings.PERFORMIQ_OAUTH_USER_AGENT,
        "client_id": settings.PERFORMIQ_OAUTH_CLIENT_ID,
        "client_secret": settings.PERFORMIQ_OAUTH_CLIENT_SECRET,
        "developer_token": "C2L8KuJbIRw40vmNtaKgZw",
    }
    return conf
