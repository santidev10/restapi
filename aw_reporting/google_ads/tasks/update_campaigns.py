import itertools
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
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import group_chorded
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.exception import retry

logger = logging.getLogger(__name__)

LOCK_NAME = "update_campaigns"


@celery_app.task(expires=TaskExpiration.HOURLY_AW_UPDATE, soft_time_limit=TaskTimeout.HOURLY_AW_UPDATE)
def setup_update_campaigns():
    mcc_ids = list(Account.objects.filter(can_manage_clients=True, is_active=True).order_by("id").values_list("id", flat=True))
    job = chain(
        lock.si(lock_name=LOCK_NAME, countdown=60, max_retries=60, expire=TaskExpiration.HOURLY_AW_UPDATE).set(queue=Queue.HOURLY_STATISTIC),
        setup_mcc_update_tasks.si(mcc_ids),
    )
    return job()


@celery_app.task
def setup_mcc_update_tasks(mcc_ids):
    """
    Update permissions and setup task signatures to update all MCC accounts
    :param mcc_ids: list
    :return: list
    """
    logger.info("Starting Google Ads update for campaigns")
    if not settings.IS_TEST:
        CFAccountConnector().update()
    detect_success_aw_read_permissions()

    account_update_tasks = group_chorded([
        mcc_account_update.si(mcc_id, index + 1, len(mcc_ids))
        for index, mcc_id in enumerate(mcc_ids, start=0)
    ]).set(queue=Queue.HOURLY_STATISTIC)
    job = chain(
        account_update_tasks,
        setup_cid_update_tasks.si(mcc_ids),
    )
    return job()


@celery_app.task
def setup_cid_update_tasks(mcc_ids):
    """
    Setup task signatures to update all CID accounts under all MCC accounts
    :param mcc_ids:
    :return:
    """
    campaign_update_tasks = itertools.chain.from_iterable(
        create_cid_tasks(mcc_id) for mcc_id in mcc_ids
    )
    campaign_update_tasks = group_chorded(campaign_update_tasks).set(queue=Queue.HOURLY_STATISTIC)
    job = chain(
        campaign_update_tasks,
        unlock.si(lock_name=LOCK_NAME).set(queue=Queue.HOURLY_STATISTIC),
        finalize_campaigns_update.si(),
    )
    return job()


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error, ))
def mcc_account_update(mcc_id, index, total):
    """
    Update single MCC account
    """
    mcc_account = Account.objects.get(id=mcc_id)
    GoogleAdsUpdater().update_accounts_for_mcc(mcc_account=mcc_account)
    logger.debug(f"ACCOUNTS UPDATE COMPLETE {index}/{total} FOR MCC: {mcc_id}")


def create_cid_tasks(mcc_id):
    """
    Create task signatures for all cid accounts under mcc_id
    :return: list -> Celery update tasks
    """
    cid_accounts = []
    for account in Account.objects.filter(managers=mcc_id, can_manage_clients=False, is_active=True):
        try:
            int(account.id)
            cid_accounts.append(account)
        except ValueError:
            continue
    task_signatures = [
        cid_campaign_update.si(mcc_id, cid_account.id, index + 1, len(cid_accounts)).set(queue=Queue.HOURLY_STATISTIC)
        for index, cid_account in enumerate(cid_accounts, start=0)
    ]
    return task_signatures


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error, ))
def cid_campaign_update(mcc_id, cid_id, index, total):
    """
    Update single CID account
    """
    start = time.time()
    mcc_account = Account.objects.get(id=mcc_id)
    cid_account = Account.objects.get(id=cid_id)

    updater = GoogleAdsUpdater()
    updater.update_campaigns(mcc_account, cid_account)
    logger.debug(f"CID CAMPAIGNS UPDATE COMPLETE {index}/{total} FOR CID: {cid_id} MCC: {mcc_id}. Took: {time.time() - start}")


@celery_app.task
def finalize_campaigns_update():
    logger.debug("Adding relations between reports and campaign creations")
    add_relation_between_report_and_creation_campaigns()
    logger.info(f"Campaign update complete")
