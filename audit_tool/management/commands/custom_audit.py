from django.core.management.base import BaseCommand
from audit_tool.custom_audit import CustomAudit
import logging
import os
import csv

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
        parser.add_argument(
            '--export',
            help='Export file path.'
        )

    def handle(self, *args, **kwargs):
        if kwargs['file'] is None:
            self.stdout.write('You must provide a csv file path to read. --file CSV_PATH')
        elif kwargs['title'] is None:
            self.stdout.write('You must provide a title for this segment. --title SEGMENT_TITLE')
        elif kwargs['export'] is None:
            self.stdout.write('You must a file path to export to. --export')
        else:
            csv_file_path = kwargs['file']
            segment_title = kwargs['title']
            csv_export_path = kwargs['export']

            try:
                os.path.exists(csv_export_path)
            except OSError:
                self.stdout.write('The provided --export path is invalid.')

            self.stdout.write('Starting custom audit...\n', ending='')
            custom_audit = CustomAudit(
                csv_file_path=csv_file_path,
                segment_title=segment_title,
            )
            results = custom_audit.run()

            with open(csv_export_path, mode='w') as csv_file:
                headers = ['Channel ID', 'Found words']
                writer = csv.DictWriter(csv_file, fieldnames=headers)

                writer.writeheader()
                for key, value in results.items():
                    writer.writerow({
                        headers[0]: key,
                        headers[1]: value
                    })

            self.stdout.write('Custom audit results exported to: {}'.format(csv_export_path))




