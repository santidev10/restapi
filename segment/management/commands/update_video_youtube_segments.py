import logging
from django.core.management.base import BaseCommand

from segment.models import SegmentVideo


logging.basicConfig(level=logging.INFO)


class Command(BaseCommand):
    help = "Update video YouTube segments"

    def handle(self, *args, **options):
        SegmentVideo.objects.update_youtube_segments()
