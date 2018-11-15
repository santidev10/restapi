import logging

from dateutil.parser import parse

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.models import Account
from saas import celery_app
from utils.exception import ignore_on_error

logger = logging.getLogger(__name__)


@celery_app.task()
@ignore_on_error(logger=logger)
def update_aw_account(account_id, today_str: str, start, end, index, count):
    today = parse(today_str).date()
    account = Account.objects.get(id=account_id)
    is_mcc = account.can_manage_clients
    logger.debug("START {}/{}: {} update: {}".format(index, count, get_account_type_str(is_mcc), account))
    updater = AWDataLoader(today, start=start, end=end)
    updater.full_update(account)
    logger.debug("FINISH {}/{}: {} update: {}".format(index, count, get_account_type_str(is_mcc), account))


def get_account_type_str(is_mcc):
    return "MCC" if is_mcc else "Customer"
