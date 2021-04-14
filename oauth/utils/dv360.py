from django.conf import settings

from googleapiclient.discovery import build, Resource
from oauth2client.client import GoogleCredentials
from rest_framework.serializers import Serializer

from oauth.constants import EntityStatusType
from oauth.models import DV360Advertiser
from oauth.models import DV360Partner
from oauth.models import OAuthAccount
from performiq.models.constants import ENTITY_STATUS_MAP_TO_ID

from typing import Type

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


class LineItemAdapter(DV360BaseAdapter):
    field_name_mapping = {
        "name": "name",
        "lineItemId": "id",
        "insertionOrderId": "insertion_order_id",
        "displayName": "display_name",
        "entityStatus": "entity_status",
        "updateTime": "update_time",
    }


class AdgroupAdapter(DV360BaseAdapter):
    field_name_mapping = {
        "name": "name",
        "adGroupId": "id",
        "lineItemId": "line_item_id",
        "displayName": "display_name",
        "entityStatus": "entity_status",
        "updateTime": "update_time",
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


def request_line_items(advertiser: DV360Advertiser, resource):
    return resource.advertisers().lineItems().list(advertiserId=str(advertiser.id)).execute()


def request_adgroups(advertiser: DV360Advertiser, resource):
    return resource.advertisers().adGroups().list(advertiserId=str(advertiser.id)).execute()


def retrieve_sdf_items(dv_connector, advertiser_ids, fields_mapping, model):
    import concurrent.futures
    import os
    from uuid import uuid4
    import csv
    from utils.db.functions import safe_bulk_create

    base_sdf_dir = f"/tmp/sdf_{uuid4()}"
    os.mkdir(base_sdf_dir)
    dirs = []
    for i in range(len(advertiser_ids)):
        d = f"{base_sdf_dir}/sdf_{i}"
        os.mkdir(d)
        dirs.append(d)

    all_args = [(a_id, target_dir) for a_id, target_dir in zip(advertiser_ids, dirs)]
    all_fps = []
    with concurrent.futures.thread.ThreadPoolExecutor(max_workers=len(all_args)) as executor:
        futures = [executor.submit(dv_connector.get_line_items_sdf_report, *args) for args in all_args]
        sdf_fps = [f.result() for f in concurrent.futures.as_completed(futures)]
        all_fps.extend(sdf_fps)
    data = []
    for fp in all_fps:
        with open(fp, "r") as file:
            reader = csv.DictReader(file)
            data.append([row for row in reader])
    all_to_create = []
    all_to_update = []
    for rows in data:
        to_create, to_update = prepare_sdf_items(rows, fields_mapping, model)
        all_to_update.extend(to_update)
        all_to_create.extend(to_create)
    safe_bulk_create(model, all_to_create)
    model.objects.bulk_update(all_to_update, fields=list(set(fields_mapping.keys()) - set("id")))
    print('done')



def prepare_sdf_items(rows, mapping, model, exists_filter=None):
    to_create = []
    to_update = []
    exists = set(model.objects.filter(**exists_filter or {}).values_list("id", flat=True))
    for row in rows:
        data = {db_field: row.get(report_field) for db_field, report_field in mapping.items()}
        try:
            data["entity_status"] = EntityStatusType["ENTITY_STATUS_" + data["entity_status"].upper()].value
        except KeyError:
            data["entity_status"] = None
        obj = model(**data)
        container = to_update if obj.id in exists else to_create
        container.append(obj)
    return to_create, to_update


