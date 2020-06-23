import logging

from django.core.management import BaseCommand

from segment.persistent_segment_importer import PersistentSegmentImporter

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--data_type",
            help="channel or video"
        )
        parser.add_argument(
            "--segment_type",
            help="whitelist, blacklist, apex"
        )
        parser.add_argument(
            "--title",
            help="Segment title"
        )
        parser.add_argument(
            "--thumbnail",
            help="Segment thumbnail to create"
        )
        parser.add_argument(
            "--audit_category",
            help="Integer id of AuditCategory item"
        )
        parser.add_argument(
            "--path",
            help="Import list of Youtube ids"
        )

    def handle(self, *args, **options):
        importer = PersistentSegmentImporter(*args, **options)
        importer.run()
