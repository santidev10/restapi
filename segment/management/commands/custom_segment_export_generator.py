import logging

from django.core.management import BaseCommand
from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from segment.custom_segment_export_generator import CustomSegmentExportGenerator
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            self.run(*args, **options)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="custom_segment_export.pid")
    def run(self, *args, **kwargs):
        generator = CustomSegmentExportGenerator()
        while generator.has_next():
            try:
                generator.generate()
            except CustomSegmentFileUploadQueueEmptyException:
                logger.error("No items in queue")
