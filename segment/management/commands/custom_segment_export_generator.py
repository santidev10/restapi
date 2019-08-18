from datetime import timedelta
import logging

from django.db.models import Q
from django.core.management import BaseCommand
from django.utils import timezone
from pid import PidFile
from pid import PidFileError

from segment.custom_segment_export_generator import CustomSegmentExportGenerator
from segment.models import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUploadQueueEmptyException

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    update_threshold = 7

    def add_arguments(self, parser):
        parser.add_argument(
            "--update",
            help="Update or generate"
        )

    def handle(self, *args, **options):
        export_type = "update" if options.get("update") else "generate"
        pid_file = "custom_segment_export_{}.pid".format(export_type)
        try:
            with PidFile(pid_file, piddir=".") as pid:
                if export_type == "update":
                    self.update(*args, **options)
                else:
                    self.generate(*args, **options)
        except PidFileError:
            pass

    def update(self, *args, **kwargs):
        threshold = timezone.now() - timedelta(days=self.update_threshold)
        generator = CustomSegmentExportGenerator(updating=True)
        to_update = CustomSegmentFileUpload.objects.filter(
            (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
        )
        for export in to_update:
            if export.segment.owner is None:
                continue
            generator.generate(export=export)

    def generate(self, *args, **kwargs):
        generator = CustomSegmentExportGenerator()
        while generator.has_next():
            try:
                generator.generate()
            except CustomSegmentFileUploadQueueEmptyException:
                pass
