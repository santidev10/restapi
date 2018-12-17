import logging
from datetime import datetime

import pytz

from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.adwords_reports import AccountInactiveError
from aw_reporting.update.tasks.load_hourly_stats import load_hourly_stats
from saas import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def upload_initial_aw_data(connection_pk):
    from aw_reporting.models import AWConnection
    from aw_reporting.models import Account
    from aw_reporting.aw_data_loader import AWDataLoader
    connection = AWConnection.objects.get(pk=connection_pk)

    updater = AWDataLoader(datetime.now(tz=pytz.utc).date())
    client = get_web_app_client(
        refresh_token=connection.refresh_token,
    )

    mcc_to_update = Account.objects.filter(
        mcc_permissions__aw_connection=connection,
        update_time__isnull=True,  # they were not updated before
    ).distinct()
    for mcc in mcc_to_update:
        client.SetClientCustomerId(mcc.id)
        updater.save_all_customers(client, mcc)

    accounts_to_update = Account.objects.filter(
        managers__mcc_permissions__aw_connection=connection,
        can_manage_clients=False,
        update_time__isnull=True,  # they were not updated before
    )
    for account in accounts_to_update:
        client.SetClientCustomerId(account.id)
        try:
            updater.advertising_account_update(client, account)
            # hourly stats
            load_hourly_stats(client, account)
        except AccountInactiveError:
            account.is_active = False
            account.save()
