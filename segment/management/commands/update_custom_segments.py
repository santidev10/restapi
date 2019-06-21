import logging

from django.core.management import BaseCommand
from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from segment.models import CustomSegmentFileUpload
from segment.custom_segment_export_generator import CustomSegmentExportGenerator

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            self.run(*args, **options)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="custom_segment_export.pid")
    def run(self, *args, **kwargs):
        generator = CustomSegmentExportGenerator(updating=True)
        to_update = CustomSegmentFileUpload.objects.filter(completed_at__isnull=False)
        for export in to_update:
            segment = export.segment
            segment.update_statistics()
            generator.generate(export=export)
