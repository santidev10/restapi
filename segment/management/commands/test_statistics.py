import logging

from django.core.management.base import BaseCommand
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.models import CustomSegment
from segment.segment_list_generator import SegmentListGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        # type = 0
        # # type = 2
        # generator = SegmentListGenerator(type)
        # generator.run()
        segment = PersistentSegmentChannel.objects.get(id=129)
        details = segment.calculate_statistics()
        segment.details = details
        segment.save()
        # segment.export_file()
        pass
