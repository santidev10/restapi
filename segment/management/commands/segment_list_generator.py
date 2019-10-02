import logging

from django.core.management.base import BaseCommand
from segment.segment_list_generator import SegmentListGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            help="Generator type",
            type=int
        )

    def handle(self, *args, **kwargs):
        list_generator_type = kwargs["type"]
        list_generator = SegmentListGenerator(list_generator_type)
        list_generator.run()
