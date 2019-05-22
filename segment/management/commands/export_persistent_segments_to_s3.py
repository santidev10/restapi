import logging

from pid.decorator import pidfile
from django.core.management import BaseCommand
from django.utils import timezone

from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentFileUpload

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    @pidfile(piddir=".", pidname="audit_segments_export.pid")
    def handle(self, *args, **options):
        logger.info("Start")
        self.finalize_segments(PersistentSegmentChannel)
        self.finalize_segments(PersistentSegmentVideo)
        logger.info("Finish")

    def finalize_segments(self, segment_model):
        """
        Finalize all segments
            Set segment details and upload files to s3
        :return:
        """
        for segment in segment_model.objects.all():
            now = timezone.now()
            s3_filename = segment.get_s3_key(datetime=now)
            logger.info("Collecting data for {}".format(segment.get_s3_key()))
            segment.export_to_s3(s3_filename)
            logger.info("Saved {}".format(segment.get_s3_key()))
            PersistentSegmentFileUpload.objects.create(segment_id=segment.id, filename=s3_filename, created_at=now)
