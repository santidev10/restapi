from django.core.management.base import BaseCommand
from audit_tool.custom_audit import CustomAudit
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Execute a custom audit against existing channels and videos with a provided csv of audit words'

    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--file',
            help='Set csv file path'
        )
        parser.add_argument(
            '--title',
            help='Set Segment title'
        )

    def handle(self, *args, **kwargs):
        if kwargs['file'] is None:
            self.stdout.write('You must provide a csv file path to read. --file CSV_PATH')
        elif kwargs['title'] is None:
            self.stdout.write('You must provide a title for this segment. --title SEGMENT_TITLE')
        else:
            csv_file_path = kwargs['file']
            segment_title = kwargs['title']

            self.stdout.write('Starting custom audit...\n', ending='')
            custom_audit = CustomAudit(
                csv_file_path=csv_file_path,
                segment_title=segment_title,
            )
            custom_audit.run()


