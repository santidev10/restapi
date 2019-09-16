import logging

from celery import chain
from celery import group
from dateutil.parser import parse
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models.functions import Greatest
from django.db.models.functions import Least
from django.utils import timezone

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.models import Account
from aw_reporting.update.tasks.load_hourly_stats import load_hourly_stats
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import group_chorded
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

__all__ = [
    "update_aw_accounts_hourly_stats",
    "update_aw_account_hourly_stats",
]
logger = logging.getLogger(__name__)


LOCK_NAME = "update_aw_accounts_hourly_stats"


@celery_app.task(expires=TaskExpiration.HOURLY_AW_UPDATE, soft_time_limit=TaskTimeout.HOURLY_AW_UPDATE)
def update_aw_accounts_hourly_stats():
    now = timezone.now()
    ongoing_filter = (Q(min_start__lte=now) | Q(min_start__isnull=True)) \
                     & (Q(max_end__gte=now) | Q(max_end__isnull=True))
    accounts = Account.objects.filter(can_manage_clients=False, is_active=True) \
        .annotate(
        min_cc_start=Min("account_creation__campaign_creations__start"),
        min_c_start=Min("campaigns__start_date"),
        max_cc_end=Max("account_creation__campaign_creations__end"),
        max_c_end=Max("campaigns__end_date")) \
        .annotate(max_end=Greatest("max_cc_end", "max_c_end"),
                  min_start=Least("min_cc_start", "min_c_start")) \
        .filter(ongoing_filter)
    account_ids = accounts.values_list("pk", flat=True)
    update_group = group([
        update_aw_account_hourly_stats.si(account_id=account_id, datetime_str=now.isoformat())
        for account_id in account_ids
    ])
    job = chain(
        lock.si(lock_name=LOCK_NAME, countdown=60, max_retries=60),
        group_chorded(update_group),
        unlock.si(lock_name=LOCK_NAME)
    )
    return job()


@celery_app.task
def update_aw_account_hourly_stats(account_id, datetime_str):
    today = parse(datetime_str).date()
    updater = AWDataLoader(today)
    account = Account.objects.get(pk=account_id)
    updater.run_task_with_any_manager(load_hourly_stats, account)
    Account.objects.filter(id=account.id).update(hourly_updated_at=timezone.now())
