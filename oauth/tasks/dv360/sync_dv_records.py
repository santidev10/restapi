import random
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import cycle

from django.db.models import Q
from django.utils import timezone
from googleapiclient.errors import HttpError
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.status import HTTP_403_FORBIDDEN

from oauth.models import AdGroup
from oauth.models import DV360Advertiser
from oauth.models import LineItem
from oauth.models import DV360Partner
from oauth.models import OAuthAccount
from oauth.constants import EntityStatusType
from oauth.constants import OAuthType
from oauth.tasks.dv360.serializers import AdvertiserSerializer
from oauth.tasks.dv360.serializers import CampaignSerializer
from oauth.tasks.dv360.serializers import InsertionOrderSerializer
from oauth.tasks.dv360.serializers import PartnerSerializer
from oauth.utils.dv360 import AdvertiserAdapter
from oauth.utils.dv360 import CampaignAdapter
from oauth.utils.dv360 import InsertionOrderAdapter
from oauth.utils.dv360 import PartnerAdapter
from oauth.utils.dv360 import get_discovery_resource
from oauth.utils.dv360 import load_credentials
from oauth.utils.dv360 import retrieve_sdf_items
from oauth.utils.dv360 import request_advertiser_campaigns
from oauth.utils.dv360 import request_partner_advertisers
from oauth.utils.dv360 import request_partners
from oauth.utils.dv360 import request_insertion_orders
from oauth.utils.dv360 import serialize_dv360_list_response_items
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
from saas import celery_app


SYNC_DV360_TASK_LOCK = "sync_dv360_task_lock"
UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2
logger = logging.getLogger(__name__)


def _revoked(oauth_account):
    oauth_account.revoked_access = True
    oauth_account.save(update_fields=["revoked_access"])
    message = f"DV360 OAuth access lost for OAuthAccount: {oauth_account.id}"
    logger.warning(message)


@celery_app.task
def sync_dv360():
    is_acquired = REDIS_CLIENT.lock(SYNC_DV360_TASK_LOCK).acquire(blocking=False)
    if is_acquired:
        sync_dv_partners()
        sync_dv_advertisers()
        sync_dv_campaigns()
        sync_insertion_orders()
        sync_line_items()
        sync_adgroups()
        unlock.run(lock_name=SYNC_DV360_TASK_LOCK, fail_silently=True)


@celery_app.task
def sync_dv_partners(oauth_account_ids: list = False, force_all=False, sync_advertisers=False):
    """
    Updates partners for accounts that were either created
    recently, or have not been recently updated
    :param oauth_account_ids: force update on a list of emails belonging to OAuthAccounts
    :param force_all: force update on all dv360 oauth accounts that haven't revoked access
    :param sync_advertisers: syncs advertisers as well. This is currently the only way to
        link advertisers to oauth accounts
    """
    logger.info(f"starting dv partners sync...")
    if oauth_account_ids and isinstance(oauth_account_ids, list):
        query = OAuthAccount.objects.filter(oauth_type=OAuthType.DV360.value, id__in=oauth_account_ids)
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
        except (HttpAccessTokenRefreshError, HttpError) as err:
            # OAuth revoked access also includes HttpError 403
            if isinstance(err, HttpError):
                status = getattr(err, "args", [{"status": None}])[0]["status"]
                # service unavailable
                if status == "503":
                    continue
                elif status == "403":
                    _revoked(account)
                    continue
                else:
                    raise
            else:
                # HttpAccessTokenRefreshError lost access
                _revoked(account)
                continue
        partner_serializers = serialize_dv360_list_response_items(
            response=partners_response,
            items_key="partners",
            adapter_class=PartnerAdapter,
            serializer_class=PartnerSerializer
        )
        partners = [serializer.save() for serializer in partner_serializers]
        account.dv360_partners.set(partners)

        # the only place we can sync an oauth accounts' advertisers is here
        if not sync_advertisers:
            continue

        for partner in partners:
            try:
                advertisers_response = request_partner_advertisers(partner, resource)
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

    if oauth_account_ids:
        OAuthAccount.objects.filter(id__in=oauth_account_ids).update(synced=True)


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


@celery_app.task
def sync_insertion_orders():
    DVInsertionOrderSynchronizer().run()


@celery_app.task
def sync_line_items(oauth_account_ids=None):
    mapping = {
        "id": "Line Item Id",
        "name": "Name",
        "entity_status": "Status",
        "insertion_order_id": "Io Id",
    }

    ids_filter = Q() if oauth_account_ids is None else Q()
    for oauth in OAuthAccount.objects.filter(ids_filter, oauth_type=OAuthType.DV360.value, is_enabled=True, is_synced=True):
        advertiser_ids = oauth.dv360_advertisers.all().values_list("id", flat=True)
        retrieve_sdf_items(oauth, advertiser_ids, mapping, LineItem, "get_line_items_sdf_report")


@celery_app.task
def sync_adgroups(oauth_account_ids=None):
    mapping = {
        "id": "Ad Group Id",
        "name": "Name",
        "line_item_id": "Line Item Id",
    }
    ids_filter = Q() if oauth_account_ids is None else Q()
    for oauth in OAuthAccount.objects.filter(ids_filter, oauth_type=OAuthType.DV360.value, is_enabled=True,
                                             is_synced=True):
        advertiser_ids = oauth.dv360_advertisers.all().values_list("id", flat=True)
        retrieve_sdf_items(oauth, advertiser_ids, mapping, AdGroup)


class AbstractThreadedDVSynchronizer:

    UPDATED_THRESHOLD_MINUTES = UPDATED_THRESHOLD_MINUTES
    CREATED_THRESHOLD_MINUTES = CREATED_THRESHOLD_MINUTES
    THREAD_CEILING = 5

    # required inheritor fields:
    model_id_filter = None
    response_items_key = None
    adapter_class = None
    serializer_class = None
    request_function = None

    def __init__(self):
        self.query = None
        self.future_contexts = []
        self.responses = []

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
        if not len(executor_contexts):
            logger.info(f"No available OAuthAccounts to work with for {self.__class__.__name__}")
            return
        resource_context_pool = cycle(executor_contexts)

        max_workers = min(len(executor_contexts), self.THREAD_CEILING)
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
                request_function = self.get_request_function()
                logger.info(f"submitting new thread for instance: {instance}")
                future = executor.submit(request_function, instance, resource)
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
        logger.info(f"unpacking dv response data, handling exceptions")
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
        logger.info("saving dv responses")
        # serialize the advertiser/campaign instances
        for response in self.responses:
            if not response:
                continue
            serializers = serialize_dv360_list_response_items(
                response=response,
                items_key=self.response_items_key,
                adapter_class=self.adapter_class,
                serializer_class=self.serializer_class,
            )
            # save the advertisers/campaigns
            for serializer in serializers:
                serializer.save()


class DVAdvertiserSynchronizer(AbstractThreadedDVSynchronizer):

    model_id_filter = "dv360_partners__id__in"
    response_items_key = "advertisers"
    adapter_class = AdvertiserAdapter
    serializer_class = AdvertiserSerializer

    def __init__(self):
        super().__init__()
        self.query = DV360Partner.objects.filter(entity_status=EntityStatusType.ENTITY_STATUS_ACTIVE.value)

    def run(self):
        logger.info(f"starting dv advertisers sync...")
        super().run()

    @staticmethod
    def get_request_function():
        return request_partner_advertisers


class DVCampaignSynchronizer(AbstractThreadedDVSynchronizer):

    model_id_filter = "dv360_advertisers__id__in"
    response_items_key = "campaigns"
    adapter_class = CampaignAdapter
    serializer_class = CampaignSerializer

    def __init__(self):
        super().__init__()
        self.query = DV360Advertiser.objects.filter(entity_status=EntityStatusType.ENTITY_STATUS_ACTIVE.value)

    def run(self):
        logger.info(f"starting dv campaign sync...")
        super().run()

    @staticmethod
    def get_request_function():
        return request_advertiser_campaigns


class DVInsertionOrderSynchronizer(AbstractThreadedDVSynchronizer):
    model_id_filter = "dv360_advertisers__id__in"
    response_items_key = "insertionOrders"
    adapter_class = InsertionOrderAdapter
    serializer_class = InsertionOrderSerializer

    def __init__(self):
        super().__init__()
        self.query = DV360Advertiser.objects.filter(entity_status=EntityStatusType.ENTITY_STATUS_ACTIVE.value)

    def run(self):
        logger.info(f"starting insertion orders sync...")
        super().run()

    @staticmethod
    def get_request_function():
        return request_insertion_orders
