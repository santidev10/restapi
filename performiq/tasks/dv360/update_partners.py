from datetime import timedelta

from django.db.models import Q
from django.utils import timezone
from googleapiclient.errors import HttpError
from googleapiclient.discovery import Resource
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.models.constants import OAuthType
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.advertiser_serialzer import AdvertiserSerializer
from performiq.tasks.dv360.serializers.partner_serializer import PartnerSerializer
from performiq.utils.dv360 import AdvertiserAdapter
from performiq.utils.dv360 import PartnerAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2


def update_partners():
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
    for account in query:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        try:
            partners_response = resource.partners().list().execute()
        except HttpError as e:
            if e.resp.status == HTTP_403_FORBIDDEN:
                account.revoked_access = True
                account.save(update_fields=["revoked_access"])
                continue
            else:
                raise e
        items = partners_response["partners"]
        partners = []
        # TODO dry this out
        for item in items:
            adapted = PartnerAdapter().adapt(item)
            serializer = PartnerSerializer(data=adapted)
            if not serializer.is_valid(raise_exception=True):
                continue
            partner = serializer.save()
            partners.append(partner)
            # TODO thread this
            update_advertisers(resource, partner.id)

        account.dv360_partners.set(partners)
        account.updated_at = timezone.now()
        account.save(update_fields=["updated_at"])

def update_advertisers(resource: Resource, partner_id: int) -> None:
    try:
        advertisers_response = resource.advertisers().list(partnerId=partner_id).execute()
    except Exception as e:
        raise e
    items = advertisers_response["advertisers"]
    advertiers = []
    # TODO dry this out
    for item in items:
        adapted = AdvertiserAdapter().adapt(item)
        serializer = AdvertiserSerializer(data=adapted)
        if not serializer.is_valid():
            continue
        advertiser = serializer.save()
        # TODO only need this if we associate to the owning account
        advertiers.append(advertiser)
