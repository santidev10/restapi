from django.core.management import BaseCommand
import logging
from pid.decorator import pidfile

from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    @pidfile(piddir=".", pidname="audit_segments_update_details.pid")
    def handle(self, *args, **options):
        logger.info("Start")

        # Channels
        for segment in PersistentSegmentChannel.objects.all():
            segment.details = segment.calculate_details()
            segment.save()

        # Videos
        for segment in PersistentSegmentVideo.objects.all():
            segment.details = segment.calculate_details()
            segment.save()

        logger.info("Finish")
