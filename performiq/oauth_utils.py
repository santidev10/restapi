import logging

from django.conf import settings
from googleads import adwords
from googleads import oauth2
from googleads.common import ZeepServiceProxy
import requests

logger = logging.getLogger(__name__)
API_VERSION = "v201809"


def load_client_settings():
    conf = {
        "user_agent": settings.PERFORMIQ_OAUTH_USER_AGENT,
        "client_id": settings.PERFORMIQ_OAUTH_CLIENT_ID,
        "client_secret": settings.PERFORMIQ_OAUTH_CLIENT_SECRET
    }
    return conf


def get_customers(refresh_token):
    config = load_client_settings()
    aw_client = get_client(
        client_customer_id=None,
        refresh_token=refresh_token,
        **config
    )
    customer_service = aw_client.GetService(
        "CustomerService", version=API_VERSION
    )
    return customer_service.getCustomers()


def get_client(**kwargs):
    api_settings = load_client_settings()
    api_settings.update(kwargs)
    return _get_client(**api_settings)


def _get_client(developer_token, client_id, client_secret, user_agent,
                refresh_token, client_customer_id=None, **_):
    oauth2_client = oauth2.GoogleRefreshTokenClient(
        client_id, client_secret, refresh_token
    )
    client_obj = adwords.AdWordsClient(
        developer_token,
        oauth2_client,
        user_agent=user_agent,
        client_customer_id=client_customer_id,
        cache=ZeepServiceProxy.NO_CACHE,
    )
    return client_obj


def get_google_access_token_info(token):
    url = "https://www.googleapis.com/oauth2/v3/tokeninfo?" \
          "access_token={}".format(token)
    token_info = requests.get(url).json()
    return token_info
