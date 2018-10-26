import logging
from functools import partial

from celery.task import task
from django.conf import settings
from pytz import timezone
from pytz import utc
from suds import WebFault

from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.update.tasks import detect_success_aw_read_permissions
from aw_reporting.update.tasks import max_ready_date
from aw_reporting.update.tasks import recalculate_de_norm_fields
from utils.datetime import now_in_default_tz

__all__ = ["update_aw_accounts"]

logger = logging.getLogger(__name__)


@task
def update_aw_accounts(account_ids=None, start=None, end=None, forced=False):
    pre_process()

    now = now_in_default_tz(utc)
    today = now.date()
    update_account_fn = partial(update_accounts, today=today, forced=forced, start=start, end=end,
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


def update_accounts(today, forced, start, end, account_ids, is_mcc: bool):
    from aw_reporting.models import Account
    updater = AWDataLoader(today, start=start, end=end)
    accounts = Account.objects.filter(is_active=True, can_manage_clients=is_mcc)
    if account_ids is not None:
        accounts = accounts.filter(id__in=account_ids)
    accounts_to_update = list(filtered_accounts_generator(accounts, forced))
    count = len(accounts_to_update)
    for index, account in enumerate(accounts_to_update):
        logger.info("%d/%d: %s update: %s", index, count, get_account_type_str(is_mcc), account)
        updater.full_update(account)


def filtered_accounts_generator(queryset, forced):
    now = now_in_default_tz(utc)
    for account in queryset:
        tz = timezone(account.timezone)
        if forced or update_account(account, now, tz):
            yield account


def update_account(account, now, tz):
    return not account.update_time or max_ready_date(account.update_time, tz) < max_ready_date(now, tz)


def get_account_type_str(is_mcc):
    return "MCC" if is_mcc else "Customer"
