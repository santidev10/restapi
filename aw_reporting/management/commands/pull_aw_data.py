import logging

from django.core.management.base import BaseCommand

from aw_reporting.aw_data_loader import AWDataLoader
from aw_reporting.tasks import update_aw_accounts

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--start',
            dest='start',
            help='Start from... options: %s' % ", ".join(
                m.__name__ for m in AWDataLoader.advertising_update_tasks)
        )

        parser.add_argument(
            '--end',
            dest='end',
            help='Last method... options: %s' % ", ".join(
                m.__name__ for m in AWDataLoader.advertising_update_tasks)
        )

        parser.add_argument(
            "--account_ids",
            dest="account_ids",
            help="Account IDs to update as a comma separated string",
            type=str,
            default=None,
        )

        parser.add_argument(
            '--start_date',
            dest='start_date',
            help="Start date to pull data in format yyyy-mm-dd",
            type=str,
            default=None,
        )

        parser.add_argument(
            '--end_date',
            dest='end_date',
            help="End date to pull data in format yyyy-mm-dd",
            type=str,
            default=None,
        )

    def handle(self, *args, **options):
        start = options.get("start")
        end = options.get("end")
        account_ids_str = options.get("account_ids")
        account_ids = account_ids_str.split(",") if account_ids_str is not None else None
        start_date = options.get("start_date")
        end_date = options.get("end_date")
        update_aw_accounts.delay(account_ids, start, end, start_date, end_date)
