from django.core.management.base import BaseCommand
from audit_tool.audit import Audit
from related_tool.models import RelatedVideo
import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

logger = logging.getLogger('topic_audit')

class Command(BaseCommand):
    help = 'Start Reaudit.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--related_tool_ignore_seed',
            nargs='?',
            default=True,
            help='Ignore seed files and retrieve using existing db data'
        ),
        parser.add_argument(
            '--related_tool_seed_file',
            help='Set file path of csv keywords to read from.'
        )
        parser.add_argument(
            '--related_tool_seed_type',
            help='Set file export result directory.'
        )
        parser.add_argument(
            '--whitelist',
            help='Set keywords file path.'
        )
        parser.add_argument(
            '--blacklist',
            help='Video or channel audit'
        )
        parser.add_argument(
            '--title',
            help='Audit Export title'
        )
        parser.add_argument(
            '--export',
            help='Audit export directory'
        )
        parser.add_argument(
            '--type',
            help='Audit data type (video or channel)'
        )

    def handle(self, *args, **kwargs):
        related_tool_opts = {
            'ignore_seed': kwargs.pop('related_tool_ignore_seed'),
            'seed_type': kwargs.pop('related_tool_seed_type'),
            'file': kwargs.pop('related_tool_seed_file'),
        }
        related_tool = RelatedTool(*args, **related_tool_opts)
        related_videos = related_tool.run()
        related_audit = RelatedAudit(related_videos, **kwargs)
        related_audit.run()


class RelatedAudit(Audit):
    def __init__(self, related_video_data):
        super().__init__()

        self.related_video_data = related_video_data

    def video_data_generator(self):
        while self.related_video_data:
            batch = self.related_video_data[:self.video_batch_size]
            yield batch
            self.related_video_data = self.related_video_data[self.video_batch_size]
