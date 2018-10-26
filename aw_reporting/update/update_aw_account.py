import logging

from celery import task

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.models import Account

logger = logging.getLogger(__name__)


@task
def update_aw_account(account_id, today, start, end, index, count):
    account = Account.objects.get(id=account_id)
    is_mcc = account.can_manage_clients
    logger.info("%d/%d: %s update: %s", index, count, get_account_type_str(is_mcc), account)
    updater = AWDataLoader(today, start=start, end=end)
    updater.full_update(account)


def get_account_type_str(is_mcc):
    return "MCC" if is_mcc else "Customer"
