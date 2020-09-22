from django.conf import settings

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build

from performiq.models.models import OAuthAccount
from performiq.models.constants import ENTITY_STATUS_MAP_TO_ID


API_VERSION = "v1"
SERVICE_NAME = "displayvideo"
DISCOVERY_SERVICE_URL = f"https://displayvideo.googleapis.com/$discovery/rest?version={API_VERSION}"
OAUTH_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"


def load_credentials(account: OAuthAccount):
    """
    given an OAuthAccount, build a GoogleCredentials instance.
    used to instantiate a client
    """
    credentials = {
        "access_token": account.token,
        "refresh_token": account.refresh_token,
        "client_id": settings.PERFORMIQ_OAUTH_CLIENT_ID,
        "client_secret": settings.PERFORMIQ_OAUTH_CLIENT_SECRET,
        "user_agent": settings.PERFORMIQ_OAUTH_USER_AGENT,
        "token_expiry": 0,
        "token_uri": OAUTH_TOKEN_URI,
    }
    return GoogleCredentials(**credentials)


def get_discovery_resource(credentials: GoogleCredentials):
    """
    given a GoogleCredentials instance, build a resource instance
    for interacting with the displayvideo API
    """
    params = {
        "serviceName": SERVICE_NAME,
        "version": API_VERSION,
        "credentials": credentials,
        "discoveryServiceUrl": DISCOVERY_SERVICE_URL,
    }
    return build(**params)

class PartnerAdapter:
    """
    adapt from Google's representation to performiq.models.DV360Partner
    representation
    """

    # maps google field name to ours
    field_name_mapping = {
        "name": "name",
        "partnerId": "id",
        "updateTime": "update_time",
        "displayName": "display_name",
        "entityStatus": "entity_status",
    }

    # field value to callable which does adapting
    field_value_mapping = {
        "entity_status": "adapt_entity_status",
    }

    def adapt(self, dv360_data: dict) -> dict:
        adapted = {}
        # adapt field names
        for original_name, adapted_name in self.field_name_mapping.items():
            adapted[adapted_name] = dv360_data.get(original_name, None)

        # adapt values
        for adapted_name, callable_name in self.field_value_mapping.items():
            adapted[adapted_name] = getattr(self, callable_name)(adapted[adapted_name])

        return adapted

    @staticmethod
    def adapt_entity_status(value: str) -> int:
        """map entity status string to our int id representation"""
        return ENTITY_STATUS_MAP_TO_ID.get(value)
