import logging

from dateutil.parser import parse
from django.db import Error

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.models import Account
from saas import celery_app
from utils.exception import ignore_on_error
from utils.exception import retry

logger = logging.getLogger(__name__)


@celery_app.task()
@ignore_on_error(logger=logger)
@retry(count=10, delay=20, exceptions=(Error, ))
def update_aw_account(account_id, today_str: str, start, end, index, count, start_date_str=None, end_date_str=None):
    today = parse(today_str).date()
    start_date = None
    end_date = None
    if start_date_str and end_date_str:
        start_date = parse(start_date_str).date()
        end_date = parse(end_date_str).date()
    account = Account.objects.get(id=account_id)
    is_mcc = account.can_manage_clients
    logger.debug("START {}/{}: {} update: {}".format(index, count, get_account_type_str(is_mcc), account))
    updater = AWDataLoader(today, start=start, end=end, start_date=start_date, end_date=end_date)
    updater.full_update(account)
    logger.debug("FINISH {}/{}: {} update: {}".format(index, count, get_account_type_str(is_mcc), account))


def get_account_type_str(is_mcc):
    return "MCC" if is_mcc else "Customer"
