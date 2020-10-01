from datetime import timedelta

from django.db.models import Q
from django.utils import timezone
from googleapiclient.errors import HttpError
from rest_framework.status import HTTP_403_FORBIDDEN
from concurrent.futures import ThreadPoolExecutor

from googleapiclient.discovery import Resource
from performiq.models.models import DV360Partner
from performiq.models.constants import OAuthType
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.advertiser_serializer import AdvertiserSerializer
from performiq.tasks.dv360.serializers.partner_serializer import PartnerSerializer
from performiq.tasks.dv360.sync_dv_records import serialize_dv360_list_response_items
from performiq.utils.dv360 import AdvertiserAdapter
from performiq.utils.dv360 import PartnerAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2


class OAuthAccountDVSynchronizer:
    # TODO continue here
    pass


def sync_dv_partners():
    """
    Updates partners for accounts that were either created
    recently, or have not been recently updated
    """
    # TODO development only
    query = OAuthAccount.objects.filter(oauth_type=OAuthType.DV360.value, revoked_access=False)

    # created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
    # updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
    # query = OAuthAccount.objects.filter(
    #     Q(oauth_type=OAuthType.DV360.value) &
    #     Q(revoked_access=False) &
    #     (Q(created_at__gte=created_threshold) | Q(updated_at__lte=updated_threshold))
    # )
    contexts = []
    for account in query:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        try:
            partners_response = request_partners(resource)
        except HttpError as e:
            if e.resp.status == HTTP_403_FORBIDDEN:
                account.revoked_access = True
                account.save(update_fields=["revoked_access"])
                continue
            else:
                raise e
        partner_serializers = serialize_dv360_list_response_items(
            response=partners_response,
            items_name="partners",
            adapter_class=PartnerAdapter,
            serializer_class=PartnerSerializer
        )
        partners = [serializer.save() for serializer in partner_serializers]

        for partner in partners:
            try:
                advertisers_response = request_partner_advertisers(partner, resource)
            except Exception as e:
                raise e
            advertiser_serializers = serialize_dv360_list_response_items(
                response=advertisers_response,
                items_name="advertisers",
                adapter_class=AdvertiserAdapter,
                serializer_class=AdvertiserSerializer,
            )
            advertisers = [serializer.save() for serializer in advertiser_serializers]
            account.dv360_advertisers.set(advertisers)

        account.dv360_partners.set(partners)
        account.updated_at = timezone.now()
        account.save(update_fields=["updated_at"])

def request_partner_advertisers(partner: DV360Partner, resource: Resource) -> dict:
    """
    given a DV360Partner instance and discovery Resource
    request an advertiser's list of campaigns, and
    return the response
    :param partner:
    :param resource:
    :return response:
    """
    return resource.advertisers().list(partnerId=str(partner.id)).execute()


def request_partners(resource: Resource) -> dict:
    """
    given a Discovery Resource, request
    the list of available partners
    :param resource: Discovery Resource
    :return: dict response
    """
    return resource.partners().list().execute()
