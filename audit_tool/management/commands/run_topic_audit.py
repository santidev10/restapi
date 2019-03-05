from django.core.management.base import BaseCommand
from audit_tool.models import TopicAudit
from audit_tool.topic_audit import TopicAudit as TopicAuditor
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
import logging

logger = logging.getLogger('topic_audit')

class Command(BaseCommand):
    help = 'Run a topic audit that has already been created.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--title',
            help='Title of Topic to start.'
        )

    def handle(self, *args, **kwargs):
        title = kwargs['title']

        # check if able to get topic, channel, and video
        try:
            topic = TopicAudit.objects.get(title=title)
        except TopicAudit.DoesNotExist:
            logger.error('A topic with --title {} was not found.'.format(title))
        try:
            persistent_channel_segment = PersistentSegmentChannel.objects.get(related_topic=topic)
        except PersistentSegmentChannel.DoesNotExist:
            logger.error('The related persistent channel segment was not found.')
        try:
            persistent_video_segemnt = PersistentSegmentVideo.objects.get(related_topic=topic)
        except PersistentSegmentChannel.DoesNotExist:
            logger.error('The related persistent video segment was not found.')

        if topic.is_running:
            raise ValueError('The topic with --title {} is already running.'.format(title))

        keywords = topic.keywords.split(',')
        topic_audit = TopicAuditor(
            keywords=keywords,
            topic=topic,
            channel_segment=persistent_channel_segment,
            video_segment=persistent_video_segemnt,
        )
        topic_audit.run()