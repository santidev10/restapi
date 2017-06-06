from django.core.management.base import BaseCommand
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import detect_success_aw_read_permissions
from aw_reporting.utils import command_single_process_lock
from datetime import datetime
from pytz import timezone, utc
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level='INFO')
logger = logging.getLogger(__name__)


class Command(BaseCommand):

    @command_single_process_lock("aw_main_update")
    def handle(self, *args, **options):
        detect_success_aw_read_permissions()

        from aw_reporting.models import Account
        timezones = Account.objects.values_list(
            "timezone", flat=True).order_by("timezone").distinct()

        now = datetime.now(tz=utc)
        today = now.date()
        timezones = [
            t for t in timezones
            if now.astimezone(timezone(t)).hour > 1
        ]
        logger.info("Timezones: {}".format(timezones))

        # first we will update accounts based on MCC timezone
        mcc_to_update = Account.objects.filter(
            timezone__in=timezones,
            updated_date__lt=today,
            can_manage_clients=True,
        )
        updater = AWDataLoader(today)
        for mcc in mcc_to_update:
            updater.full_update(mcc)

        # 2) update all the advertising accounts
        accounts_to_update = Account.objects.filter(
            timezone__in=timezones,
            updated_date__lt=today,
            can_manage_clients=False,
        )

        updater = AWDataLoader(today)
        for account in accounts_to_update:
            updater.full_update(account)

