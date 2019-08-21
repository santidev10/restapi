import logging

from django.core.management.base import BaseCommand

from aw_reporting.update.update_aw_accounts_hourly_stats import update_aw_accounts_hourly_stats

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--detach",
            "-d",
            dest="detach",
            action="store_true",
            help="Run in background"
        )

    def handle(self, *args, **options):
        if options.get("detach"):
            update_aw_accounts_hourly_stats.delay()
        else:
            update_aw_accounts_hourly_stats()
