import logging
from functools import partial

from celery import group
from celery.task import task
from django.conf import settings
from pytz import utc
from suds import WebFault

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.update.tasks import detect_success_aw_read_permissions
from aw_reporting.update.tasks import recalculate_de_norm_fields
from aw_reporting.update.update_aw_account import update_aw_account
from aw_reporting.utils import command_single_process_lock
from utils.datetime import now_in_default_tz

__all__ = ["update_aw_accounts"]

logger = logging.getLogger(__name__)


@task
@command_single_process_lock("aw_main_update")
def update_aw_accounts(account_ids=None, start=None, end=None):
    pre_process()

    now = now_in_default_tz(utc)
    today = now.date()
    update_account_fn = partial(update_accounts, today=today, start=start, end=end,
                                account_ids=account_ids)
    update_account_fn(is_mcc=True)
    update_account_fn(is_mcc=False)

    post_process()


def pre_process():
    if not settings.IS_TEST:
        create_cf_account_connection()
    detect_success_aw_read_permissions()


def post_process():
    add_relation_between_report_and_creation_campaigns()
    add_relation_between_report_and_creation_ad_groups()
    add_relation_between_report_and_creation_ads()
    recalculate_de_norm_fields()


def create_cf_account_connection():
    from aw_reporting.models import AWConnection, Account, AWAccountPermission
    from aw_reporting.adwords_api import load_web_app_settings, get_customers

    aw_settings = load_web_app_settings()
    connection, created = AWConnection.objects.update_or_create(
        email="promopushmaster@gmail.com",
        defaults=dict(refresh_token=aw_settings['cf_refresh_token']),
    )
    if created:
        try:
            customers = get_customers(
                connection.refresh_token,
                **load_web_app_settings()
            )
        except WebFault as e:
            logger.critical(e)
        else:
            mcc_accounts = [c for c in customers
                            if c['canManageClients'] and not c['testAccount']]
            for ac_data in mcc_accounts:
                data = dict(
                    id=ac_data['customerId'],
                    name=ac_data['descriptiveName'],
                    currency_code=ac_data['currencyCode'],
                    timezone=ac_data['dateTimeZone'],
                    can_manage_clients=ac_data['canManageClients'],
                    is_test_account=ac_data['testAccount'],
                )
                obj, _ = Account.objects.get_or_create(
                    id=data['id'], defaults=data,
                )
                AWAccountPermission.objects.get_or_create(
                    aw_connection=connection, account=obj,
                )


def update_accounts(today, start, end, account_ids, is_mcc: bool):
    from aw_reporting.models import Account
    accounts = Account.objects.filter(is_active=True, can_manage_clients=is_mcc)
    if account_ids is not None:
        accounts = accounts.filter(id__in=account_ids)
    count = len(accounts)
    tasks_signatures = [
        update_aw_account.si(account.id, today, start, end, index, count)
        for index, account in enumerate(accounts)
    ]
    job = group(tasks_signatures)
    result = job.apply_async()
    result.get()


