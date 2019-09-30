import logging

from django.core.management.base import BaseCommand
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.models import CustomSegment

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        stats = CustomSegment.objects.get(id=199).calculate_statistics()
        pass
