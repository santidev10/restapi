from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from django.core.management.base import BaseCommand
import logging
import os
import csv

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Execute a custom audit against existing channels and videos with a provided csv of audit words'

    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--segment',
            help='Title of Segment to export.'
        )
        parser.add_argument(
            '--export',
            help='Set csv export file path',
        )
        parser.add_argument(
            '--title',
            help='Set csv titles',
        )

    def handle(self, *args, **kwargs):
        title = kwargs.get('title')
        export_csv_path = kwargs.get('export')
        csv_title = kwargs.get('title')

        channel_export_path = '{}{} {}'.format(export_csv_path, csv_title, 'Channels Whitelist.csv')
        video_export_path = '{}{} {}'.format(export_csv_path, csv_title, 'Videos Whitelist.csv')

        persistent_channel = PersistentSegmentChannel.objects.get(title__contains=title)
        persistent_video = PersistentSegmentVideo.objects.get(title__contains=title)

        channel_field_names = ["URL", "Title", "Category", "Language", "Thumbnail", "Likes", "Dislikes", "Views", "Subscribers", "Audited Videos", "Bad Words"]
        video_field_names = ["URL", "Title", "Category", "Language", "Thumbnail", "Likes", "Dislikes", "Views", "Bad Words"]

        with open(channel_export_path, mode='w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=channel_field_names)
            writer.writeheader()
            for channel in persistent_channel.related.all():
                writer.writerow(channel.get_exportable_row())

        with open(video_export_path, mode='w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=video_field_names)
            writer.writeheader()
            for video in persistent_video.related.all():
                writer.writerow(video.get_exportable_row())

        print('Export complete: {}'.format(channel_export_path))
        print('Export complete: {}'.format(video_export_path))
