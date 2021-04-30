import concurrent.futures
import os
import csv
import sys
from typing import Type

from django.conf import settings
from googleapiclient.discovery import build, Resource
from oauth2client.client import GoogleCredentials
from rest_framework.serializers import Serializer
from uuid import uuid4

from oauth.constants import EntityStatusType
from oauth.constants import OAuthType
from oauth.models import DV360Advertiser
from oauth.models import DV360Partner
from oauth.models import OAuthAccount
from performiq.models.constants import ENTITY_STATUS_MAP_TO_ID
from utils.db.functions import safe_bulk_create
from utils.dv360_api import DV360Connector

# SDF csv rows may contain huge cells
csv.field_size_limit(sys.maxsize)
API_VERSION = "v1"
SERVICE_NAME = "displayvideo"
DISCOVERY_SERVICE_URL = f"https://displayvideo.googleapis.com/$discovery/rest?version={API_VERSION}"
OAUTH_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"


def load_credentials(account: OAuthAccount = None, refresh_token=None, access_token=None):
    """
    given an OAuthAccount, build a GoogleCredentials instance.
    used to instantiate a client
    """
    if account:
        access_token = account.token
        refresh_token = account.refresh_token
    credentials = {
        "access_token": access_token,
        "refresh_token": refresh_token,
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


class DV360BaseAdapter:
    """
    adapt from Google's displayvideo api representation to performiq.models.*
    representation
    """
    # field value to callable which does adapting
    field_value_mapping = {
        "entity_status": "adapt_entity_status",
    }

    class Meta:
        abstract = True

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


def serialize_dv360_list_response_items(
        response: dict,
        items_key: str,
        adapter_class: Type[DV360BaseAdapter],
        serializer_class: Type[Serializer]
) -> list:
    """
    given a json response from a "list" method list response from dv,
    persist the items with the given adapter and serializer
    :param response: a dict response from a dv discovery resource
    :param items_key: the name of the node enclosing the items
    :param adapter_class: the adapter class to be used in adapting from response to our stored format
    :param serializer_class: serializer to validate and save adapted data
    :return list: list of serializers
    """
    items = response[items_key]
    serializers = []
    for item in items:
        adapted = adapter_class().adapt(item)
        serializer = serializer_class(data=adapted)
        if not serializer.is_valid():
            continue
        serializers.append(serializer)

    return serializers


class PartnerAdapter(DV360BaseAdapter):
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


class AdvertiserAdapter(DV360BaseAdapter):
    """
    adapt from Google's representation to performiq.models.DV360Advertiser
    representation
    """
    # maps google field name to ours
    field_name_mapping = {
        "name": "name",
        "advertiserId": "id",
        "partnerId": "partner_id",
        "updateTime": "update_time",
        "displayName": "display_name",
        "entityStatus": "entity_status",
    }


class CampaignAdapter(DV360BaseAdapter):
    """
    adapt from Google's representation to performiq.models.DV360Campaign
    representation
    """
    field_name_mapping = {
        "name": "name",
        "campaignId": "id",
        "advertiserId": "advertiser_id",
        "updateTime": "update_time",
        "displayName": "display_name",
        "entityStatus": "entity_status",
    }


class InsertionOrderAdapter(DV360BaseAdapter):
    field_name_mapping = {
        "name": "name",
        "campaignId": "campaign_id",
        "displayName": "display_name",
        "entityStatus": "entity_status",
        "updateTime": "update_time",
        "insertionOrderId": "id"
    }


def request_partners(resource: Resource) -> dict:
    """
    given a Discovery Resource, request
    the list of available partners
    :param resource: Discovery Resource
    :return: dict response
    """
    return resource.partners().list().execute()


def request_partner_advertisers(partner: DV360Partner, resource: Resource) -> dict:
    """
    NOTE: To be used by DVSynchronizer
    given a DV360Partner instance and discovery Resource
    request an advertiser's list of campaigns, and
    return the response
    :param partner:
    :param resource:
    :return response:
    """
    return resource.advertisers().list(partnerId=str(partner.id)).execute()


def request_advertiser_campaigns(advertiser: DV360Advertiser, resource: Resource) -> dict:
    """
    NOTE: To be used by DVSynchronizer
    given a DV360Advertiser instance and discovery Resource
    request an advertiser's list of campaigns, and return
    the response
    :param advertiser:
    :param resource:
    :return response:
    """
    return resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()


def request_insertion_orders(advertiser: DV360Advertiser, resource):
    return resource.advertisers().insertionOrders().list(advertiserId=str(advertiser.id)).execute()


def retrieve_sdf_items(oauth_account: OAuthAccount, advertiser_ids: list[int], fields_mapping: dict[str, str],
                       model, conn_method: str):
    """
    Function to query, download, and save DV360 SDF API responses
    :param oauth_account: OAuthAccount to create oauth credentials
    :param advertiser_ids: List of advertiser ids to retrieve child resources
    :param fields_mapping: Dict of Django model field names to SDF column names. Used to map SDF column names
        to db column names
    :param model: OAuth model to create instances for with SDf results
    :param conn_method: Name of DV360Connector method to retrieve SDF's
    :return:
    """
    def get(conn: DV360Connector, advertiser_id: int, target_dir: str):
        """ Helper function to invoke method on DV360Connector for ThreadPoolExecutor """
        res = getattr(conn, conn_method)(advertiser_id, target_dir)
        return res

    # Prepare directories to store sdf files for each advertiser. Downloaded files are zip files which must be
    # unzipped, and files always have same filenames. Create separate directories to avoid name clash
    base_sdf_dir = f"/tmp/sdf_{uuid4()}"
    os.mkdir(base_sdf_dir)
    dirs = []
    for i in range(len(advertiser_ids)):
        d = f"{base_sdf_dir}/sdf_{i}"
        os.mkdir(d)
        dirs.append(d)

    # Each thread needs separate httplib2 instance
    # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
    # Prepare args for each thread to retrieve sdf report for each advertiser
    all_args = [
        (DV360Connector(oauth_account.token, oauth_account.refresh_token), a_id, target_dir)
        for a_id, target_dir in zip(advertiser_ids, dirs)
    ]
    all_fps = []
    with concurrent.futures.thread.ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(get, *args) for args in all_args]
        sdf_fps = [f.result() for f in concurrent.futures.as_completed(futures)]
        all_fps.extend(sdf_fps)

    # Prepare to gather data from all files
    data = []
    for fp in all_fps:
        with open(fp, "r") as file:
            reader = csv.DictReader(file)
            data.append([row for row in reader])

    # Prepare all data separated into create or update lists
    all_to_create = []
    all_to_update = []
    for rows in data:
        to_create, to_update = prepare_sdf_items(rows, fields_mapping, model)
        all_to_update.extend(to_update)
        all_to_create.extend(to_create)
    safe_bulk_create(model, all_to_create)
    model.objects.bulk_update(all_to_update, fields=set(fields_mapping.keys()) - {"id"})


def prepare_sdf_items(rows: list[dict], report_mapping: dict[str, str], model, exists_filter=None) -> tuple[list, list]:
    """
    Separate model instances instantiated from sdf report rows into create or update lists
    :param rows: List of dictionaries with SDF report keys and column values, such as csv.DictReader for a SDF csv file
    :param report_mapping:
    :param model: OAuth DV360 model
    :param exists_filter: Optional filter for retrieving model rows to check existence for
    :return: Tuple of lists of create and update items
    """
    to_create = []
    to_update = []
    exists = set(model.objects.filter(**exists_filter or {}).values_list("id", flat=True))
    # Some models require additional values
    extra = {"oauth_type": OAuthType.DV360.value} \
        if "oauth_type" in set(f.name for f in model._meta.get_fields()) else {}
    for row in rows:
        data = {db_field: row.get(report_field) for db_field, report_field in report_mapping.items()}
        data.update(extra)
        try:
            data["entity_status"] = EntityStatusType["ENTITY_STATUS_" + data["entity_status"].upper()].value
        except KeyError:
            data["entity_status"] = None
        obj = model(**data)
        container = to_update if int(obj.id) in exists else to_create
        container.append(obj)
    return to_create, to_update


