import random
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import cycle
from typing import Callable
from typing import Type

from django.utils import timezone
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from rest_framework.serializers import Serializer
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.models.constants import OAuthType
from performiq.models.models import DV360Advertiser
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.advertiser_serializer import AdvertiserSerializer
from performiq.tasks.dv360.serializers.campaign_serializer import CampaignSerializer
from performiq.utils.dv360 import CampaignAdapter, AdvertiserAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import DV360BaseAdapter



def sync_dv_campaigns():
    """
    syncs advertisers' campaigns, threading each advertiser
    list request through sync_dv_records
    :return:
    """
    Synchronizer = DVCampaignSynchronizer()
    Synchronizer.run()


class DVCampaignSynchronizer:

    UPDATED_THRESHOLD_MINUTES = 30
    CREATED_THRESHOLD_MINUTES = 2
    THREAD_CEILING = 5

    future_contexts = []
    campaign_responses = []

    def run(self) -> None:
        """
        threads a given function. Creates a pool of Discovery Resources,
        each from a distinct owning OAuthAccount's credentials, then
        cycles through Resource and model instances, running the
        passed func on its own thread, up to a limit of
        THREAD_CEILING
        :rtype: None
        """
        advertisers_query = DV360Advertiser.objects.all()
        executor_contexts = self.get_executor_contexts(advertisers_query)
        resource_context_pool = cycle(executor_contexts)

        # with ThreadPoolExecutor(max_workers=max(len(executor_contexts), self.THREAD_CEILING)) as executor:
        # TODO get this threading going
        with ThreadPoolExecutor(max_workers=1) as executor:
            # spread threaded load as evenly as possible over available resources
            for advertiser in advertisers_query.prefetch_related("oauth_accounts"):
                # traverse relation from instance to oauth accounts
                advertiser_oauth_accounts = advertiser.oauth_accounts.all()
                if not len(advertiser_oauth_accounts):
                    continue
                # cycle resources until we have a resource whose account has access
                # to the dv record or until we've checked all resources
                valid_oauth_account = False
                for _ in range(len(executor_contexts)):
                    context = next(resource_context_pool)
                    account, resource = map(context.get, ("account", "resource"))
                    if account in advertiser_oauth_accounts:
                        valid_oauth_account = True
                        break
                if not valid_oauth_account:
                    continue
                # unpack context after we get a resource with credentials from an owning account
                print(f"submitting new thread for {account}")
                future = executor.submit(request_advertiser_campaigns, advertiser, resource)
                self.future_contexts.append({
                    "future": future,
                    "account": account,
                    "advertiser": advertiser,
                })

        self.unpack_responses_from_futures()
        self.save_campaigns_from_responses()

    @staticmethod
    def get_executor_contexts(advertisers_query) -> list:
        """
        get a list of contexts for the ThreadPoolExecutor
        :return:
        """
        # get all distinct oauth accounts related to the dv360 objects that are being updated
        advertiser_ids = advertisers_query.values_list("id", flat=True)
        accounts = OAuthAccount.objects \
            .filter(
                oauth_type=OAuthType.DV360.value,
                revoked_access=False,
                dv360_advertisers__id__in=list(advertiser_ids)
            ) \
            .distinct()

        # create resources and pack in context dict for cycling
        executor_contexts = []
        for account in accounts:
            credentials = load_credentials(account)
            resource = get_discovery_resource(credentials)
            context = {
                "account": account,
                "resource": resource,
            }
            executor_contexts.append(context)

        # prevents overuse of any one oauth credential, especially for small dv object sets
        random.shuffle(executor_contexts)
        return executor_contexts

    def unpack_responses_from_futures(self):
        """
        unpack the list of futures from the futures_contexts list,
        and handle any exceptions, like bad responses
        :return:
        """
        # serialize response data
        print(f"serializing response data")
        for context in self.future_contexts:
            future, account, advertiser = map(context.get, ("future", "account", "advertiser"))
            try:
                self.campaign_responses.append(future.result())
            except HttpError as e:
                # account doesn't have visibility to the advertiser
                if e.resp.status == HTTP_403_FORBIDDEN:
                    advertiser.oauth_accounts.remove(account)
                    continue
                print(f"unhandled HttpError: {e}")
                raise e
            except Exception as e:
                # request timeout
                if "timeout" in str(e):
                    continue
                print(f"Unhandled Exception: {e}")
                raise e

    def save_campaigns_from_responses(self):
        """
        save campaigns from the campaign_responses
        list after they have been unpacked
        :return:
        """
        # serialize the campaigns
        for response in self.campaign_responses:
            serializers = serialize_dv360_list_response_items(
                response=response,
                items_name="campaigns",
                adapter_class=CampaignAdapter,
                serializer_class=CampaignSerializer,
            )
            # save the campaigns
            for serializer in serializers:
                serializer.save()



# def sync_dv_advertisers():
#     """
#     syncs partners' advertisers, threading each partner
#     list request through sync_dv_records
#     :return:
#     """
#     model_query = DV360Partner.objects.all()
#     # created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
#     # updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
#     sync_dv_records(
#         model_query=model_query,
#         model_id_filter="dv360_partners__id__in",
#         oauth_accounts_prefetch="oauth_accounts",
#         dv_model_account_relation=["oauth_accounts"],
#         request_function=request_partner_advertisers,
#         serializer_function_args={
#             "items_name": "advertisers",
#             "adapter_class": AdvertiserAdapter,
#             "serializer_class": AdvertiserSerializer,
#         }
#     )





def serialize_dv360_list_response_items(
        response: dict,
        items_name: str,
        adapter_class: Type[DV360BaseAdapter],
        serializer_class: Type[Serializer]
) -> list:
    """
    given a json response from a "list" method list response from dv,
    persist the items with the given adapter and serializer
    :param response: a dict response from a dv discovery resource
    :param items_name: the name of the node enclosing the items
    :param adapter_class: the adapter class to be used in adapting from response to our stored format
    :param serializer_class: serializer to validate and save adapted data
    :return list: list of serializers
    """
    items = response[items_name]
    serializers = []
    for item in items:
        adapted = adapter_class().adapt(item)
        serializer = serializer_class(data=adapted)
        if not serializer.is_valid():
            continue
        serializers.append(serializer)

    return serializers


def request_advertiser_campaigns(advertiser: DV360Advertiser, resource: Resource) -> dict:
    """
    given a DV360Advertiser instance and discovery Resource
    request an advertiser's list of campaigns, and return
    the response
    :param advertiser:
    :param resource:
    :return response:
    """
    return resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()
