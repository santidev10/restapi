from django.core.management import BaseCommand

from aw_reporting.tools.pull_performance_data import PullDataTools
from aw_reporting.tools.pull_performance_data import pull_performance_data


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--year",
            "-y",
            dest="year",
            default=2019,
            type=int,
            help="Pull performance data for what year"
        )

        parser.add_argument(
            "--file_path",
            "-f",
            dest="file_path",
            default="performance_channel_data.csv",
            help="CSV file path to store data"
        )

    def handle(self, *args, **options):
        pull_performance_data(
            options.get("year"),
            PullDataTools.Channel,
            options.get("file_path")
        )
