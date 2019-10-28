import logging
import time

from celery import chain
from django.db import Error

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdaterContinueException
from aw_reporting.models import Account
from audit_tool.models import APIScriptTracker
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import group_chorded
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.exception import retry

logger = logging.getLogger(__name__)

LOCK_NAME = "update_without_campaigns"
MAX_TASK_COUNT = 50


@celery_app.task
def setup_update_without_campaigns():
    job = chain(
        lock.si(lock_name=LOCK_NAME, countdown=60, max_retries=None).set(queue=Queue.DELIVERY_STATISTIC_UPDATE),
        setup_cid_update_tasks.si()
    )
    return job()


@celery_app.task
def setup_cid_update_tasks():
    logger.debug("Starting Google Ads update without campaigns")
    cursor, _ = APIScriptTracker.objects.get_or_create(name=LOCK_NAME)
    cid_account_ids = GoogleAdsUpdater().get_accounts_to_update()[cursor.cursor:MAX_TASK_COUNT + cursor.cursor]
    task_signatures = [
        cid_update_all_except_campaigns.si(cid_id).set(queue=Queue.HOURLY_STATISTIC)
        for cid_id in cid_account_ids
    ]
    update_tasks = group_chorded(task_signatures).set(queue=Queue.HOURLY_STATISTIC)
    job = chain(
        update_tasks,
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
    add_relation_between_report_and_creation_ad_groups()
    add_relation_between_report_and_creation_ads()
    cursor = APIScriptTracker.objects.get(name=LOCK_NAME)
    if cursor.cursor > Account.objects.filter(can_manage_clients=False, is_active=True).count():
        cursor.cursor = 0
    else:
        cursor.cursor = cursor.cursor + MAX_TASK_COUNT
    setup_cid_update_tasks.delay()


