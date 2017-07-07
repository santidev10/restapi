from django.core.management.base import BaseCommand
from aw_reporting.utils import command_single_process_lock
from aw_reporting.models import Account
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import load_hourly_stats
import pytz
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    @command_single_process_lock("aw_hourly_update")
    def handle(self, *args, **options):
        accounts = Account.objects.filter(can_manage_clients=False)
        logger.info('Total accounts: {}'.format(len(accounts)))

        updater = AWDataLoader(datetime.now().date())
        for account in accounts:
            updater.run_task_with_any_manager(
                load_hourly_stats, account,
            )
        logger.info('End')
