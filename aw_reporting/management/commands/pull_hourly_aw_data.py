import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models.functions import Greatest
from django.db.models.functions import Least
from django.utils import timezone

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.models import Account
from aw_reporting.tasks import get_ad_groups_and_stats
from aw_reporting.tasks import get_campaigns
from aw_reporting.tasks import load_hourly_stats
from aw_reporting.utils import command_single_process_lock

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    @command_single_process_lock("aw_hourly_update")
    def handle(self, *args, **options):
        now = timezone.now()
        ongoing_filter = (Q(min_start__lte=now) | Q(min_start__isnull=True)) \
                         & (Q(max_end__gte=now) | Q(max_end__isnull=True))
        accounts = Account.objects.filter(can_manage_clients=False) \
            .annotate(
            min_cc_start=Min("account_creation__campaign_creations__start"),
            min_c_start=Min("campaigns__start_date"),
            max_cc_end=Max("account_creation__campaign_creations__end"),
            max_c_end=Max("campaigns__end_date")) \
            .annotate(max_end=Greatest("max_cc_end", "max_c_end"),
                      min_start=Least("min_cc_start", "min_c_start")) \
            .filter(ongoing_filter)

        total_accounts = accounts.count()
        logger.info('Total accounts: {}'.format(total_accounts))
        progress = 0
        updater = AWDataLoader(datetime.now().date())
        for account in accounts:
            updater.run_task_with_any_manager(get_campaigns, account)
            updater.run_task_with_any_manager(get_ad_groups_and_stats, account)
            updater.run_task_with_any_manager(load_hourly_stats, account)
            account.hourly_updated_at = timezone.now()
            account.save()
            progress += 1
            logger.info("Processed {}/{} accounts".format(progress, total_accounts))
        logger.info('End')
