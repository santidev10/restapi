import csv
import logging
import brand_safety.constants as constants
from django.core.management import BaseCommand


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    update_threshold = 7

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            help="Import list of Youtube ids"
        )
        parser.add_argument(
            "--type",
            help="Channel or video"
        )

    def handle(self, *args, **options):
        youtube_ids = self._read_csv(options["path"])


    def _read_csv(self, path):
        with open(path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            ids = list(reader)
        return ids

    def _config(self, data_type):
        config = {
            constants.VIDEO: {},
            constants.CHANNEL: {},
        }
        return config[data_type]
