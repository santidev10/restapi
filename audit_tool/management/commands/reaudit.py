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
            help='Set file export result path.'
        )
        parser.add_argument(
            '--keywords',
            help='Set keywords file path.'
        )
        parser.add_argument(
            '--type',
            help='Channel audit'
        )
        parser.add_argument(
            '--reverse',
            help='Channel audit'
        )

    def handle(self, *args, **kwargs):
        csv_file_path = kwargs['file']
        csv_export_path = kwargs['export']
        csv_keywords_path = kwargs['keywords']
        type = kwargs['type']
        reverse = kwargs.get('reverse', False)

        reaudit = Reaudit(csv_file_path, csv_export_path, csv_keywords_path, reverse=reverse)

        if type == 'channel':
            reaudit.channel_run()
        else:
            reaudit.run()

