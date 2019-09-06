import logging

from django.core.management.base import BaseCommand
from segment.segment_list_generator import SegmentListGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        list_generator = SegmentListGenerator()
        list_generator.run()
