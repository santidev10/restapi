import logging

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

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
        generation_type = kwargs["type"]
        pid_file = f"segment_list_generator_{generation_type}.pid"
        try:
            with PidFile(pid_file, piddir="."):
                list_generator = SegmentListGenerator(generation_type)
                list_generator.run()
        except PidFileError:
            pass
