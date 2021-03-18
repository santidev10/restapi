from concurrent.futures import ThreadPoolExecutor
from itertools import cycle

from googleapiclient.discovery import Resource

from oauth.models import DV360Advertiser
from oauth.models import OAuthAccount
from performiq.tasks.dv360.serializers.campaign_serializer import CampaignSerializer
from performiq.utils.dv360 import CampaignAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import serialize_dv360_list_response_items


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2
THREAD_CEILING = 5


def update_campaigns():
    """
    Update all eligible advertiser campaigns
    """
    # TODO development only
    advertisers_query = DV360Advertiser.objects.all()

    # updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
    # query = DV360Advertiser.objects.filter(updated_at__lte=updated_threshold)

    # get all accounts for pool
    advertiser_ids = advertisers_query.values_list("id", flat=True)
    accounts = OAuthAccount.objects \
        .filter(dv360_partners__advertisers__id__in=list(advertiser_ids)) \
        .distinct()

    # we want as many threads as there are available resources (good access tokens)
    executor_contexts = []
    for account in accounts:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        context = {
            "account": account,
            "resource": resource,
        }
        executor_contexts.append(context)
    resource_context_pool = cycle(executor_contexts)

    with ThreadPoolExecutor(max_workers=max(len(executor_contexts), THREAD_CEILING)) as executor:
        # spread threaded load as evenly as possible over available good access tokens
        for advertiser in advertisers_query.prefetch_related("partner__oauth_accounts"):
            context = next(resource_context_pool)
            while account not in advertiser.partner.oauth_accounts.all():
                context = next(resource_context_pool)
            # unpack context after we get a resource with credentials from an owning account
            account, resource = map(context.get, ("account", "resource"))
            executor.submit(persist_advertiser_campaigns, advertiser, resource)


def persist_advertiser_campaigns(advertiser: DV360Advertiser, resource: Resource) -> None:
    """
    given a DV360Advertiser instance and discovery Resource
    request an advertiser's list of campaigns, and persist them
    :param advertiser:
    :param resource:
    """
    try:
        campaign_response = resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()
    except Exception as e:
        raise e
    campaigns = serialize_dv360_list_response_items(
        response=campaign_response,
        items_name="campaigns",
        adapter_class=CampaignAdapter,
        serializer_class=CampaignSerializer
    )