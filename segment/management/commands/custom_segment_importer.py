import logging
from django.core.management import BaseCommand

from segment.custom_segment_importer import CustomSegmentImporter


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    update_threshold = 7

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            help="Channel or video"
        )
        parser.add_argument(
            "--path",
            help="Import list of Youtube ids"
        )

    def handle(self, *args, **options):
        importer = CustomSegmentImporter(*args, **options)
        importer.run()
