from django.core.management import BaseCommand
import logging
from pid.decorator import pidfile

from segment.custom_segment_export_generator import CustomSegmentExportGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    @pidfile(piddir=".", pidname="custom_segment_export.pid")
    def handle(self, *args, **options):
        # Process five at a time
        generator = CustomSegmentExportGenerator()
        generator.generate()
