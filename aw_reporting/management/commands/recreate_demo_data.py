from django.core.management import BaseCommand

from aw_reporting.demo.recreate_demo_data import recreate_demo_data


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--detach",
            "-d",
            dest="detach",
            action="store_true",
            help="Run in background"
        )

    def handle(self, *args, **options):
        if options.get("detach"):
            recreate_demo_data.delay()
        else:
            recreate_demo_data()
