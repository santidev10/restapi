from django.core.management.base import BaseCommand
from segment.custom_segment_export_generator import CustomSegmentExportGenerator
from segment.models import CustomSegmentFileUpload

class Command(BaseCommand):


    def handle(self, *args, **kwargs):
        exporter = CustomSegmentExportGenerator()
        file = CustomSegmentFileUpload.objects.get(id=147)
        exporter.generate(file)
