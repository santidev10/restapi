from django.core.management.base import BaseCommand
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentType
from audit_tool.models import TopicAudit
from django.core.management import call_command

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
        parser.add_argument(
            '--immediately',
            help='Flag for whether an audit for the topic should start immediately.'
        )

    def handle(self, *args, **kwargs):
        self.validate_options(*args, **kwargs)

        title = kwargs['title']
        csv_file_path = kwargs['file']
        should_start_immediately = False

        if kwargs.get('immediately'):
            should_start_immediately = True

        keywords = ','.join(self.read_csv(csv_file_path))

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
            keywords=keywords,
            is_running=should_start_immediately,
            last_started=None,
            last_stopped=None,
            channel_segment=new_persistent_segment_channel,
            video_segment=new_persistent_segment_video,
        )
        new_persistent_segment_channel.save()
        new_persistent_segment_video.save()
        new_topic.save()

        self.stdout('Created Topic, PersistentSegmentChannel, and PersistentSegmentVideo with title: {}.'.format(title))

        if should_start_immediately:
            call_command('run_topic_audit', title=title)

    @staticmethod
    def create_segment_title(type, title):
        type = PersistentSegmentType.CHANNEL.capitalize() \
            if type == PersistentSegmentType.CHANNEL \
            else PersistentSegmentType.VIDEO.capitalize()

        return '{}s {} {}'.format(type, title, PersistentSegmentCategory.WHITELIST.capitalize())

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

    def read_csv(self, file_path):
        with open(file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = [row[0] for row in csv_reader]

            return keywords
