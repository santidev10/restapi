from django.core.management.base import BaseCommand
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentType
from audit_tool.models import TopicAudit
from audit_tool.models import Keyword

import logging
import os
import csv

logger = logging.getLogger('topic_audit')

class Command(BaseCommand):
    help = 'Add a topic.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--title',
            help='Set title of topic and related persistent channel and video segments.'
        )
        parser.add_argument(
            '--file',
            help='Set file path of csv keywords to read from.'
        )

    def handle(self, *args, **kwargs):
        self.validate_options(*args, **kwargs)

        title = kwargs['title']
        csv_file_path = kwargs['file']

        new_persistent_segment_channel = PersistentSegmentChannel(
            title=self.create_segment_title(PersistentSegmentType.CHANNEL, title),
            category=PersistentSegmentCategory.WHITELIST
        )
        new_persistent_segment_video = PersistentSegmentVideo(
            title=self.create_segment_title(PersistentSegmentType.VIDEO, title),
            category=PersistentSegmentCategory.WHITELIST
        )
        # is_running, last_started, last_stopped are set by topic_audit execution
        new_topic = TopicAudit(
            title=title,
            is_running=None,
            from_beginning=None,
            completed_at=None,
            channel_segment=new_persistent_segment_channel,
            video_segment=new_persistent_segment_video,
        )

        keywords = self.read_csv(csv_file_path)
        keyword_objects = [
            Keyword(
                keyword=keyword,
                topic=new_topic
            ) for keyword in keywords
        ]

        new_persistent_segment_channel.save()
        new_persistent_segment_video.save()
        new_topic.save()
        Keyword.objects.bulk_create(keyword_objects)

        self.stdout('Created Topic, PersistentSegmentChannel, PersistentSegmentVideo and Keywords for title: {}.'.format(title))

    @staticmethod
    def create_segment_title(type, title):
        type = PersistentSegmentType.CHANNEL.capitalize() \
            if type == PersistentSegmentType.CHANNEL \
            else PersistentSegmentType.VIDEO.capitalize()

        return '{}s {} {}'.format(type, title, PersistentSegmentCategory.WHITELIST.capitalize())

    @staticmethod
    def read_csv(file_path):
        with open(file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = [row[0] for row in csv_reader]

            return keywords

    def validate_options(self, *args, **kwargs):
        if kwargs['file'] is None:
            self.stdout.write('You must provide a csv file path of keywords to read. --file CSV_PATH')

        elif kwargs['title'] is None:
            self.stdout.write('You must provide a title for this segment. --title TITLE')

        else:
            csv_file_path = kwargs['file']

        try:
            os.path.exists(csv_file_path)

        except OSError:
            self.stdout.write('The provided --file path is invalid.')
