import logging
from django.core.management import BaseCommand
from blacklist_video.blacklist_video import BlacklistVideos

class Command(BaseCommand):
    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument(
            '--seed_type',
        )
        parser.add_argument(
            '--file',
        )

    def handle(self, *args, **kwargs):
        blacklist = BlacklistVideos(*args, **kwargs)
        blacklist.run()
        # blacklist.update_channel_seeds()
        # blacklist.export()