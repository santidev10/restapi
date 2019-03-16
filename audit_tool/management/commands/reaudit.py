from django.core.management.base import BaseCommand
from audit_tool.reaudit import Reaudit
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

    def handle(self, *args, **kwargs):
        reaudit = Reaudit(*args, **kwargs)

        if type == 'channel':
            reaudit.channel_run()
        else:
            reaudit.run()

