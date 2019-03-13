import logging
from django.core.management import BaseCommand
from blacklist_video.blacklist_video import BlacklistVideos

class Command(BaseCommand):
    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--type',
        )

    def handle(self, *args, **kwargs):
        audit_type = kwargs['type']

        blacklist = BlacklistVideos(audit_type)
        # blacklist.run()
        # blacklist.update_channel_seeds()
        blacklist.export()