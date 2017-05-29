from django.core.management.base import BaseCommand
from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import update_aw_read_permissions
from datetime import datetime
from pytz import timezone, utc
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level='INFO')
logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        update_aw_read_permissions()

        from aw_reporting.models import Account
        timezones = Account.objects.values_list(
            "timezone", flat=True).order_by("timezone").distinct()

        now = datetime.now(tz=utc)
        timezones = [
            t for t in timezones
            if now.astimezone(timezone(t)).hour > 1
        ]
        logger.info("Timezones: {}".format(timezones))

        # first we will update accounts based on MCC timezone
        for mcc in Account.objects.filter(timezone__in=timezones,
                                          can_manage_clients=True):
            updater = AWDataLoader(mcc)
            updater.full_update()

