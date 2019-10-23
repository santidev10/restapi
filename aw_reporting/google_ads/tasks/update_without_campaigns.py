import itertools
import logging
import time

from celery import chain
from django.db import Error

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdaterContinueException
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

LOCK_NAME = "update_without_campaigns"


@celery_app.task(expires=TaskExpiration.FULL_AW_UPDATE, soft_time_limit=TaskTimeout.FULL_AW_UPDATE)
def setup_update_without_campaigns():
    job = chain(
        lock.si(lock_name=LOCK_NAME, countdown=60, max_retries=60, expire=TaskExpiration.FULL_AW_UPDATE).set(queue=Queue.DELIVERY_STATISTIC_UPDATE),
        setup_cid_update_tasks.si()
    )
    return job()


@celery_app.task
def setup_cid_update_tasks():
    logger.debug("Starting Google Ads update without campaigns")
    mcc_accounts = Account.objects.filter(can_manage_clients=True, is_active=True).values_list("id", flat=True)
    cid_update_tasks = itertools.chain.from_iterable(
        account_update_all_except_campaigns(mcc_id)
        for mcc_id in mcc_accounts
    )
    cid_update_tasks = group_chorded(cid_update_tasks).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
    job = chain(
        cid_update_tasks,
        unlock.si(lock_name=LOCK_NAME).set(queue=Queue.DELIVERY_STATISTIC_UPDATE),
        finalize_update.si(),
    )
    return job()


@celery_app.task
def account_update_all_except_campaigns(mcc_id):
    """
    Create task signatures with cid_update_all_except_campaigns for each account under mcc_id
    :return: list -> Celery update tasks
    """
    cid_account_ids = GoogleAdsUpdater.get_accounts_to_update_for_mcc(mcc_id)
    task_signatures = [
        cid_update_all_except_campaigns.si(cid, mcc_id, index + 1, len(cid_account_ids)).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
        for index, cid in enumerate(cid_account_ids, start=0)
    ]
    return task_signatures


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error, ))
def cid_update_all_except_campaigns(cid_id, mcc_id, index, total):
    """
    Update single CID account
    """
    start = time.time()
    try:
        mcc_account = Account.objects.get(id=mcc_id)
        cid_account = Account.objects.get(id=cid_id)
        updater = GoogleAdsUpdater()
        updater.update_all_except_campaigns(mcc_account, cid_account)
        logger.debug(f"FINISH CID UPDATE WITHOUT CAMPAIGNS {index}/{total} FOR CID: {cid_id} MCC: {mcc_id}. Took: {time.time() - start}")
    except GoogleAdsUpdaterContinueException:
        logger.error(f"ERROR CID UPDATE WITHOUT CAMPAIGNS {index}/{total} FOR CID: {cid_id} MCC: {mcc_id}. Took: {time.time() - start}")


@celery_app.task
def finalize_update():
    logger.debug("Adding relations between reports and ad_group and ad creations")
    add_relation_between_report_and_creation_ad_groups()
    add_relation_between_report_and_creation_ads()
    logger.info(f"Google Ads update without campaigns complete")

