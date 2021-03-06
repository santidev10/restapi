import logging

import yaml
from google.ads.google_ads import oauth2
from google.ads.google_ads.client import GoogleAdsClient

logger = logging.getLogger(__name__)


def load_settings():
    with open("aw_reporting/google_ads/google-ads.yaml", "r") as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    return conf


def _get_client(*_, **settings) -> GoogleAdsClient:
    """
    create a google ads client, given the provided settings
    for more on login_customer_id, @see https://developers.google.com/google-ads/api/docs/concepts/call-structure#cid
    :param _:
    :param settings:
    :return:
    """
    developer_token = settings.pop("developer_token")
    login_customer_id = str(settings.pop("login_customer_id"))
    oauth2_client = oauth2.get_credentials(settings)
    client_obj = GoogleAdsClient(
        oauth2_client,
        developer_token,
        login_customer_id=login_customer_id
    )
    return client_obj


def get_client(**kwargs):
    """
    get the google ads client. Override loaded settings with the passed kwargs
    :param kwargs:
    :return:
    """
    api_settings = load_settings()
    api_settings.update(kwargs)
    return _get_client(**api_settings)
