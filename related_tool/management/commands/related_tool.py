from django.core.management import BaseCommand
from related_tool.related import Related

class Command(BaseCommand):
    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--export_force',
            help='Export existing items in database without getting related items.'
        )
        parser.add_argument(
            '--ignore_seed',
            help='Ignore seed files and get related items for existing unscanned items in database'
        )
        parser.add_argument(
            '--seed_type',
            help='Set seed data type (video, channel)'
        )
        parser.add_argument(
            '--file',
            help='File path of seed csv file'
        )
        parser.add_argument(
            '--export',
            help='Export directory e.g. ~/Desktop/'
        )
        parser.add_argument(
            '--title',
            help='Set title of export file'
        )

    def handle(self, *args, **kwargs):
        related_tool = Related(*args, **kwargs)
        related_tool.run()
