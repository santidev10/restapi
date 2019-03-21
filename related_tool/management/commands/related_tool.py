from django.core.management import BaseCommand
from related_tool.related import Related

class Command(BaseCommand):
    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--ignore_seed',
            nargs='?',
            help='Ignore seed files and retrieve using existing db data'
        )
        parser.add_argument(
            '--seed_type',
        )
        parser.add_argument(
            '--file',
        )
        parser.add_argument(
            '--export',
        )
        parser.add_argument(
            '--title',
        )
        parser.add_argument(
            '--export_force',
            help='Immediately export existing items'
        )

    def handle(self, *args, **kwargs):
        related_tool = Related(*args, **kwargs)
        related_tool.run()
