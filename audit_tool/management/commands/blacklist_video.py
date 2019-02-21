import logging
from django.core.management import BaseCommand
from blacklist_video.blacklist_video import BlacklistVideos

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        blacklist = BlacklistVideos()
        # blacklist.run()