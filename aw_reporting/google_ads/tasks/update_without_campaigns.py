import logging
import time

from celery import chain
from django.db import Error

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.models import Account
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import group_chorded
from utils.celery.tasks import unlock
from utils.exception import retry

logger = logging.getLogger(__name__)

LOCK_NAME = "update_without_campaigns"
MAX_TASK_COUNT = 50
LOCK_TIMEOUT = 3600 * 8


@celery_app.task
def setup_update_without_campaigns():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, LOCK_TIMEOUT).acquire(blocking=False)
    if is_acquired:
        setup_cid_update_tasks.delay()


@celery_app.task
def setup_cid_update_tasks():
    logger.debug("Starting Google Ads update without campaigns")
    cid_account_ids = GoogleAdsUpdater.get_accounts_to_update(hourly_update=False, size=MAX_TASK_COUNT)
    task_signatures = [
        cid_update_all_except_campaigns.si(cid_id).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
        for cid_id in cid_account_ids
    ]
    update_tasks = group_chorded(task_signatures).set(queue=Queue.DELIVERY_STATISTIC_UPDATE)
    job = chain(
        update_tasks,
        finalize_update.si(),
        unlock.si(lock_name=LOCK_NAME, fail_silently=True).set(queue=Queue.DELIVERY_STATISTIC_UPDATE),
    )
    return job()


@celery_app.task
@retry(count=10, delay=5, exceptions=(Error,))
def cid_update_all_except_campaigns(cid_id):
    """
    Update single CID account
    """
    start = time.time()
    cid_account = Account.objects.get(id=cid_id)
    updater = GoogleAdsUpdater(cid_account)
    updater.update_all_except_campaigns()
    logger.debug("Finish update without campaigns for CID: %s Took: %s", cid_id, time.time() - start)


@celery_app.task
def finalize_update():
    """
    Call finalize methods
    Sets up the next batch of update tasks
    :return:
    """
    add_relation_between_report_and_creation_ad_groups()
    add_relation_between_report_and_creation_ads()
    logger.debug("Google Ads update without campaigns complete")
