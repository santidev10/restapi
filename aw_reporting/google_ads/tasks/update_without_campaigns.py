import logging
import time

from celery import chain
from django.db import Error

from audit_tool.models import APIScriptTracker
from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdaterContinueException
from aw_reporting.models import Account
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import group_chorded
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
from utils.exception import retry


logger = logging.getLogger(__name__)

LOCK_NAME = "update_without_campaigns"
MAX_TASK_COUNT = 20


@celery_app.task
def setup_update_without_campaigns():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, 60 * 60).acquire(blocking=False)
    if is_acquired:
        setup_cid_update_tasks.delay()


@celery_app.task
def setup_cid_update_tasks():
    logger.debug("Starting Google Ads update without campaigns")
    cursor, _ = APIScriptTracker.objects.get_or_create(name=LOCK_NAME)
    limit = MAX_TASK_COUNT + cursor.cursor
    cid_account_ids = GoogleAdsUpdater().get_accounts_to_update()[cursor.cursor:limit]
    task_signatures = [
        cid_update_all_except_campaigns.si(cid_id).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
        for cid_id in cid_account_ids
    ]
    update_tasks = group_chorded(task_signatures).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
    job = chain(
        update_tasks,
        finalize_update.si(),
        unlock.si(lock_name=LOCK_NAME).set(queue=Queue.DELIVERY_STATISTIC_UPDATE),
    )
    return job()


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error, ))
def cid_update_all_except_campaigns(cid_id):
    """
    Update single CID account
    """
    start = time.time()
    try:
        cid_account = Account.objects.get(id=cid_id)
        updater = GoogleAdsUpdater()
        updater.update_all_except_campaigns(cid_account)
        logger.debug(f"FINISH CID UPDATE WITHOUT CAMPAIGNS FOR CID: {cid_id} Took: {time.time() - start}")
    except GoogleAdsUpdaterContinueException:
        logger.warning(f"ERROR CID UPDATE WITHOUT CAMPAIGNS FOR CID: {cid_id}")


@celery_app.task
def finalize_update():
    """
    Call finalize methods
    Sets up the next batch of update tasks
    :return:
    """
    add_relation_between_report_and_creation_ad_groups()
    add_relation_between_report_and_creation_ads()
    cursor = APIScriptTracker.objects.get(name=LOCK_NAME)
    next_cursor = cursor.cursor + MAX_TASK_COUNT
    # Reset cursor if greater than length of amount of accounts
    if next_cursor > len(GoogleAdsUpdater().get_accounts_to_update()):
        next_cursor = 0
    cursor.cursor = next_cursor
    cursor.save()

