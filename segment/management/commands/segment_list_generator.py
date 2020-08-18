import logging

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        from segment.segment_list_generator import SegmentListGenerator
        list_generator = SegmentListGenerator(0)
        list_generator.run()
