import logging

from django.core.management.base import BaseCommand
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.models import CustomSegment
from segment.segment_list_generator import SegmentListGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        type = 1
        generator = SegmentListGenerator(type)
        generator.run()
