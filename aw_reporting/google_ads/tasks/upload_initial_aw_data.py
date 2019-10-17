import logging

from django.utils import timezone

from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.models import Account
from aw_reporting.models import AWConnection
from saas import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def upload_initial_aw_data_task(connection_pk):
    """
    Update all data with connection_pk AWConnection
    :param connection_pk: str -> email
    :return:
    """
    connection = AWConnection.objects.get(pk=connection_pk)
    updater = GoogleAdsUpdater()
    client = get_client(refresh_token=connection.refresh_token)

    mcc_to_update = Account.objects.filter(
        mcc_permissions__aw_connection=connection,
        update_time__isnull=True,  # they were not updated before
    ).distinct()

    for mcc in mcc_to_update:
        updater.update_accounts_for_mcc(mcc_account=mcc)

    accounts_to_update = Account.objects.filter(
        managers__mcc_permissions__aw_connection=connection,
        is_active=True,
        can_manage_clients=False,
        update_time__isnull=True,  # they were not updated before
    )

    for account in accounts_to_update:
        # Try updating with each manager id as login_customer_id
        for manager in account.managers.all():
            try:
                updater.mcc_account = manager
                client.login_customer_id = manager.id
                updater.full_update(account, client=client)
            except Exception as e:
                # Try next manager id
                logger.error(e)
                continue
            else:
                break
        account.update_time = timezone.now()
        account.save()
