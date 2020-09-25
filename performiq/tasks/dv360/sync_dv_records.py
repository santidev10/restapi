import random
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import cycle
from typing import Callable

from django.utils import timezone
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from rest_framework.serializers import Serializer
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.models.constants import OAuthType
from performiq.models.models import DV360Advertiser
from performiq.models.models import DV360Partner
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.advertiser_serializer import AdvertiserSerializer
from performiq.tasks.dv360.serializers.campaign_serializer import CampaignSerializer
from performiq.utils.dv360 import CampaignAdapter, AdvertiserAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import DV360BaseAdapter


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2
THREAD_CEILING = 5


def sync_dv_campaigns():
    model_query = DV360Advertiser.objects.all()
    # created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
    # updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
    sync_dv_records(
        model_query=model_query,
        model_id_filter="dv360_partners__advertisers__id__in",
        oauth_accounts_prefetch="partner__oauth_accounts",
        dv_model_account_relation=["partner", "oauth_accounts"],
        request_function=request_advertiser_campaigns,
        serializer_function_args={
            "items_name": "campaigns",
            "adapter_class": CampaignAdapter,
            "serializer_class": CampaignSerializer,
        }
    )


def sync_dv_advertisers():
    model_query = DV360Partner.objects.all()
    # created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
    # updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
    sync_dv_records(
        model_query=model_query,
        model_id_filter="dv360_partners__id__in",
        oauth_accounts_prefetch="oauth_accounts",
        dv_model_account_relation=["oauth_accounts"],
        request_function=request_partner_advertisers,
        serializer_function_args={
            "items_name": "advertisers",
            "adapter_class": AdvertiserAdapter,
            "serializer_class": AdvertiserSerializer,
        }
    )


def sync_dv_records(
        model_query,
        model_id_filter: str,
        oauth_accounts_prefetch: str,
        dv_model_account_relation: list,
        request_function: Callable,
        serializer_function_args: dict
) -> None:
    """
    threads a given function. Creates a pool of Discovery Resources,
    each from a distinct owning OAuthAccount's credentials, then
    cycles through Resource and model instances, running the
    passed func on its own thread, up to a limit of
    THREAD_CEILING
    :param model_query:
    :param model_id_filter:
    :param oauth_accounts_prefetch:
    :param dv_model_account_relation:
    :param request_function:
    :param serializer_function_args:
    :rtype: None
    """
    # get all distinct oauth accounts related to the dv360 objects that are being updated
    model_object_ids = model_query.values_list("id", flat=True)
    accounts = OAuthAccount.objects \
        .filter(**{model_id_filter: list(model_object_ids)}) \
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
    resource_context_pool = cycle(executor_contexts)

    futures_with_context = []
    # with ThreadPoolExecutor(max_workers=max(len(executor_contexts), THREAD_CEILING)) as executor:
    with ThreadPoolExecutor(max_workers=1) as executor:
        # spread threaded load as evenly as possible over available resources
        for model_instance in model_query.prefetch_related(oauth_accounts_prefetch):
            # traverse relation from instance to oauth accounts
            relation = model_instance
            for relation_name in dv_model_account_relation:
                relation = getattr(relation, relation_name)
            instance_oauth_accounts = relation.all()
            # cycle contexts until we have a resource whose account has access to the dv record
            context = next(resource_context_pool)
            account, resource = map(context.get, ("account", "resource"))
            while account not in instance_oauth_accounts:
                context = next(resource_context_pool)
                account, resource = map(context.get, ("account", "resource"))
            # unpack context after we get a resource with credentials from an owning account
            print(f"submitting new thread for {account}")
            future = executor.submit(request_function, model_instance, resource)
            future_with_context = {
                "future": future,
                "account": account,
            }
            futures_with_context.append(future_with_context)

    # serialize response data
    print(f"serializing response data")
    responses = []
    for future_with_context in futures_with_context:
        try:
            responses.append(future_with_context.get("future").result())
        except HttpError as e:
            if e.resp.status == HTTP_403_FORBIDDEN:
                account = future_with_context.get("account")
                account.revoked_access = True
                account.save(update_fields=["revoked_access"])
    for response in responses:
        serializers = serialize_dv360_list_response_items(response=response, **serializer_function_args)
        for serializer in serializers:
            serializer.save()


def serialize_dv360_list_response_items(
        response: dict,
        items_name: str,
        adapter_class: DV360BaseAdapter,
        serializer_class: Serializer
) -> list:
    """
    given a json response from a "list" method list response from dv,
    persist the items with the given adapter and serializer
    :param response: a dict response from a dv discovery resource
    :param items_name: the name of the node enclosing the items
    :param adapter_class: the adapter class to be used in adapting from response to our stored format
    :param serializer_class: serializer to valdatate and save adapted data
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
    print(f"request advertiser campaigns. Resource: {resource}. Advertiser: {advertiser}")
    return resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()
    # try:
    #     response = resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()
    # except Exception as e:
    #     raise e
    # return response


def request_partner_advertisers(partner: DV360Partner, resource: Resource) -> dict:
    """
    given a DV360Partner instance and discovery Resource
    request an advertiser's list of campaigns, and
    return the response
    :param partner:
    :param resource:
    :return response:
    """
    try:
        response = resource.advertisers().list(partnerId=str(partner.id)).execute()
    except Exception as e:
        raise e
    return response

