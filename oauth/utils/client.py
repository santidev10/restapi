import logging

from oauth2client import client

from django.conf import settings
from googleads import adwords
from googleads import oauth2
from googleads.common import ZeepServiceProxy
import requests

from aw_reporting.adwords_api import load_web_app_settings

logger = logging.getLogger(__name__)
API_VERSION = "v201809"



def get_adwords_client(developer_token, client_id, client_secret, user_agent,
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


def get_flow(client_settings, scopes):
    # new popup flow, different than redirect flow
    flow = client.OAuth2WebServerFlow(
        client_id=client_settings.get("client_id"),
        client_secret=client_settings.get("client_secret"),
        scope=scopes,
        access_type="offline",
        response_type="code",
        prompt="consent",  # SEE https://github.com/googleapis/google-api-python-client/issues/213
        redirect_uri=settings.GOOGLE_APP_OAUTH2_REDIRECT_URL,
        origin=settings.GOOGLE_APP_OAUTH2_ORIGIN
    )
    return flow
