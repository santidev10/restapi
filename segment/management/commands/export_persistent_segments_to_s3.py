from django.core.management import BaseCommand
import logging
from pid.decorator import pidfile

from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    @pidfile(piddir=".", pidname="audit_segments_export.pid")
    def handle(self, *args, **options):
        logger.info("Start")

        # Channels
        for segment in PersistentSegmentChannel.objects.all():
            logger.info("Collecting data for {}".format(segment.get_s3_key()))
            segment.export_to_s3()
            logger.info("Saved {}".format(segment.get_s3_key()))

        # Videos
        for segment in PersistentSegmentVideo.objects.all():
            logger.info("Collecting data for {}".format(segment.get_s3_key()))
            segment.export_to_s3()
            logger.info("Saved {}".format(segment.get_s3_key()))

        logger.info("Finish")
