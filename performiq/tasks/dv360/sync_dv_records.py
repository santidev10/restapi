import random
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import cycle

from django.db.models import Q
from django.utils import timezone
from googleapiclient.errors import HttpError
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.models.constants import OAuthType
from performiq.models.models import DV360Advertiser
from performiq.models.models import DV360Partner
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.advertiser_serializer import AdvertiserSerializer
from performiq.tasks.dv360.serializers.campaign_serializer import CampaignSerializer
from performiq.tasks.dv360.serializers.partner_serializer import PartnerSerializer
from performiq.utils.dv360 import AdvertiserAdapter
from performiq.utils.dv360 import CampaignAdapter
from performiq.utils.dv360 import PartnerAdapter
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import request_advertiser_campaigns
from performiq.utils.dv360 import request_partner_advertisers
from performiq.utils.dv360 import request_partners
from performiq.utils.dv360 import serialize_dv360_list_response_items
from saas import celery_app


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2


@celery_app.task
def sync_dv_partners(force_emails=False, force_all=False, sync_advertisers=False):
    """
    Updates partners for accounts that were either created
    recently, or have not been recently updated
    :param force_emails: force update on a list of emails belonging to OAuthAccounts
    :param force_all: force update on all dv360 oauth accounts that haven't revoked access
    :param sync_advertisers: syncs advertisers as well. This is currently the only way to
        link advertisers to oauth accounts
    """
    if force_emails:
        query = OAuthAccount.objects.filter(email__in=force_emails)
    elif force_all:
        query = OAuthAccount.objects.filter(oauth_type=OAuthType.DV360.value, revoked_access=False)
    else:
        created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
        updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
        query = OAuthAccount.objects.filter(
            Q(oauth_type=OAuthType.DV360.value) &
            Q(revoked_access=False) &
            (Q(created_at__gte=created_threshold) | Q(updated_at__lte=updated_threshold))
        )
    for account in query:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        try:
            partners_response = request_partners(resource)
        except HttpAccessTokenRefreshError:
            account.revoked_access = True
            account.save(update_fields=["revoked_access"])
            continue
        partner_serializers = serialize_dv360_list_response_items(
            response=partners_response,
            items_key="partners",
            adapter_class=PartnerAdapter,
            serializer_class=PartnerSerializer
        )
        partners = [serializer.save() for serializer in partner_serializers]
        account.dv360_partners.set(partners)

        if not sync_advertisers:
            continue
        # the only place we can sync an oauth accounts' advertisers is here
        for partner in partners:
            try:
                advertisers_response = request_partner_advertisers(str(partner.id), resource)
            except Exception as e:
                raise e
            advertiser_serializers = serialize_dv360_list_response_items(
                response=advertisers_response,
                items_key="advertisers",
                adapter_class=AdvertiserAdapter,
                serializer_class=AdvertiserSerializer,
            )
            advertisers = [serializer.save() for serializer in advertiser_serializers]
            account.dv360_advertisers.set(advertisers)

        account.updated_at = timezone.now()
        account.save(update_fields=["updated_at"])


@celery_app.task
def sync_dv_advertisers():
    """
    syncs partners' advertisers, threading each partner list request
    :return:
    """
    DVAdvertiserSynchronizer().run()


@celery_app.task
def sync_dv_campaigns():
    """
    syncs advertisers' campaigns, threading each advertiser list request
    :return:
    """
    DVCampaignSynchronizer().run()


class AbstractThreadedDVSynchronizer:

    UPDATED_THRESHOLD_MINUTES = 30
    CREATED_THRESHOLD_MINUTES = 2
    THREAD_CEILING = 5

    future_contexts = []
    responses = []

    # required inheritor fields:
    query = None
    model_id_filter = None
    response_items_key = None
    adapter_class = None
    serializer_class = None
    request_function = None

    class Meta:
        abstract = True

    @staticmethod
    def get_request_function():
        raise NotImplementedError

    def run(self) -> None:
        """
        threads a given function. Creates a pool of Discovery Resources,
        each from a distinct owning OAuthAccount's credentials, then
        cycles through Resource and model instances, running the
        passed func on its own thread, up to a limit of
        THREAD_CEILING
        :rtype: None
        """
        executor_contexts = self.get_executor_contexts()
        resource_context_pool = cycle(executor_contexts)

        max_workers = min(len(executor_contexts), self.THREAD_CEILING)
        print(f"max workers: {max_workers}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # spread threaded load as evenly as possible over available resources
            for instance in self.query.prefetch_related("oauth_accounts"):
                # traverse relation from instance to oauth accounts
                instance_oauth_accounts = instance.oauth_accounts.all()
                if not len(instance_oauth_accounts):
                    continue
                # cycle resources until we have a resource whose account has access
                # to the dv record or until we've exhausted all resources
                valid_oauth_account = False
                for _ in range(len(executor_contexts)):
                    context = next(resource_context_pool)
                    account, credentials = map(context.get, ("account", "credentials"))
                    if account in instance_oauth_accounts:
                        valid_oauth_account = True
                        break
                if not valid_oauth_account:
                    continue
                # NOTE: sharing the same resource instance between threads results in a memory allocation error
                resource = get_discovery_resource(credentials)
                print(f"submitting new thread for {account}")
                request_function = self.get_request_function()
                future = executor.submit(request_function, str(instance.id), resource)
                self.future_contexts.append({
                    "future": future,
                    "account": account,
                    "instance": instance,
                })

        self.unpack_responses()
        self.save_response_data()

    def get_executor_contexts(self) -> list:
        """
        get a list of contexts for the ThreadPoolExecutor
        :return:
        """
        # get all distinct oauth accounts related to the dv360 objects that are being updated
        instance_ids = self.query.values_list("id", flat=True)
        filters = {
            "oauth_type": OAuthType.DV360.value,
            "revoked_access": False,
            self.model_id_filter: list(instance_ids),
        }
        accounts = OAuthAccount.objects.filter(**filters).distinct()

        # create resources and pack in context dict for cycling
        executor_contexts = []
        for account in accounts:
            credentials = load_credentials(account)
            context = {
                "account": account,
                "credentials": credentials,
            }
            executor_contexts.append(context)

        # prevents overuse of any one oauth credential, especially for small dv object sets
        random.shuffle(executor_contexts)
        return executor_contexts

    def unpack_responses(self):
        """
        unpack the list of futures from the futures_contexts list,
        and handle any exceptions, like bad responses
        :return:
        """
        # serialize response data
        print(f"unpacking response data, handling exceptions")
        for context in self.future_contexts:
            future, account, instance = map(context.get, ("future", "account", "instance"))
            try:
                self.responses.append(future.result())
            except HttpAccessTokenRefreshError:
                account.revoked_access = True
                account.save(update_fields=["revoked_access"])
                continue
            except HttpError as e:
                # account doesn't have visibility to the instance
                if e.resp.status == HTTP_403_FORBIDDEN:
                    instance.oauth_accounts.remove(account)
                    continue
                raise e
            except Exception as e:
                # request timeout
                if "timeout" in str(e):
                    continue
                raise e

    def save_response_data(self):
        """
        save campaigns from the campaign_responses
        list after they have been unpacked
        :return:
        """
        print("saving responses")
        # serialize the advertiser/campaign instances
        for response in self.responses:
            serializers = serialize_dv360_list_response_items(
                response=response,
                items_key=self.response_items_key,
                adapter_class=self.adapter_class,
                serializer_class=self.serializer_class,
            )
            # save the campaigns
            for serializer in serializers:
                serializer.save()


class DVAdvertiserSynchronizer(AbstractThreadedDVSynchronizer):

    query = DV360Partner.objects.all()
    model_id_filter = "dv360_partners__id__in"
    response_items_key = "advertisers"
    adapter_class = AdvertiserAdapter
    serializer_class = AdvertiserSerializer

    @staticmethod
    def get_request_function():
        return request_partner_advertisers


class DVCampaignSynchronizer(AbstractThreadedDVSynchronizer):

    query = DV360Advertiser.objects.all()
    model_id_filter = "dv360_advertisers__id__in"
    response_items_key = "campaigns"
    adapter_class = CampaignAdapter
    serializer_class = CampaignSerializer

    @staticmethod
    def get_request_function():
        return request_advertiser_campaigns
