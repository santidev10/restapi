import logging
import time

from celery import chain
from django.conf import settings
from django.db import Error

from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.updaters.cf_account_connection import CFAccountConnector
from aw_reporting.google_ads.utils import detect_success_aw_read_permissions
from aw_reporting.models import Account
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import group_chorded
from utils.celery.tasks import unlock
from utils.exception import retry

logger = logging.getLogger(__name__)

LOCK_NAME = "update_campaigns"
MAX_TASK_COUNT = 100


@celery_app.task
def setup_update_campaigns():
    """
    This task should only ever be called once, or recalled after failing
    Update tasks are setup by finalize_campaigns_update with updated cursor value
    """
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, timeout=60 * 60 * 2).acquire(blocking=False)
    if is_acquired:
        setup_mcc_update_tasks.delay()


@celery_app.task
def setup_mcc_update_tasks():
    """
    Update permissions and setup task signatures to update all MCC accounts
    :param mcc_ids: list
    :return: list
    """
    mcc_ids = list(
        Account.objects.filter(can_manage_clients=True, is_active=True).order_by("id").values_list("id", flat=True))
    logger.debug("Starting Google Ads update for campaigns")
    if not settings.IS_TEST:
        CFAccountConnector().update()
    detect_success_aw_read_permissions()

    account_update_tasks = group_chorded([
        mcc_account_update.si(mcc_id, index + 1, len(mcc_ids))
        for index, mcc_id in enumerate(mcc_ids, start=0)
    ]).set(queue=Queue.HOURLY_STATISTIC)
    job = chain(
        account_update_tasks,
        setup_cid_update_tasks.si(),
    )
    return job()


@celery_app.task
def setup_cid_update_tasks():
    """
    Setup task signatures to update all CID accounts under all MCC accounts
    :return:
    """
    # Batch update tasks
    cid_account_ids = GoogleAdsUpdater.get_accounts_to_update(hourly_update=True, size=MAX_TASK_COUNT)
    task_signatures = [
        cid_campaign_update.si(cid_id).set(queue=Queue.HOURLY_STATISTIC)
        for cid_id in cid_account_ids
    ]
    campaign_update_tasks = group_chorded(task_signatures).set(queue=Queue.HOURLY_STATISTIC)
    job = chain(
        campaign_update_tasks,
        finalize_campaigns_update.si(),
        unlock.si(lock_name=LOCK_NAME, fail_silently=True).set(queue=Queue.HOURLY_STATISTIC),
    )
    return job()


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error,))
def mcc_account_update(mcc_id, index, total):
    """
    Update single MCC account
    """
    mcc_account = Account.objects.get(id=mcc_id)
    GoogleAdsUpdater(mcc_account).update_accounts_as_mcc()
    logger.debug("ACCOUNTS UPDATE COMPLETE %s/%s FOR MCC: %s", index, total, mcc_id)


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error,))
def cid_campaign_update(cid_id):
    """
    Update single CID account
    """
    start = time.time()
    cid_account = Account.objects.get(id=cid_id)
    updater = GoogleAdsUpdater(cid_account)
    updater.update_account_performance()
    updater.update_campaigns()
    logger.debug("CID CAMPAIGNS UPDATE COMPLETE FOR CID: %s. Took: %s", cid_id, time.time() - start)


@celery_app.task
def finalize_campaigns_update():
    """
    Call finalize methods
    Sets up the next batch of update tasks
    :return:
    """
    add_relation_between_report_and_creation_campaigns()
    logger.debug("Google Ads account and campaign update complete")
