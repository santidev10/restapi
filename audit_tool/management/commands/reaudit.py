from django.core.management.base import BaseCommand
from audit_tool.reaudit import Reaudit
from related_tool.models import RelatedVideo
import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

logger = logging.getLogger('topic_audit')

class Command(BaseCommand):
    help = 'Start Reaudit.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            help='Set file path of csv keywords to read from.'
        )
        parser.add_argument(
            '--export',
            help='Set file export result directory.'
        )
        parser.add_argument(
            '--keywords',
            help='Set keywords file path.'
        )
        parser.add_argument(
            '--type',
            help='Video or channel audit'
        )
        parser.add_argument(
            '--title',
            help='Export title'
        )
        parser.add_argument(
            '--badwords',
            help='More bad words'
        )
        parser.add_argument(
            '--related_seed',
            help='If data to audit should come from related videos table'
        )

    def handle(self, *args, **kwargs):
        reaudit = Reaudit(*args, **kwargs)
        all_related_videos = RelatedVideo.objects.all()

        if kwargs.get('related_seed'):
            reaudit.related_audit(all_related_videos)

        else:
            reaudit.run()


class RelatedAudit(Reaudit):
    def __init__(self, related_video_data):
        super()__init__()
        self.related_video_data = related_video_data

    def video_data_generator(self):
