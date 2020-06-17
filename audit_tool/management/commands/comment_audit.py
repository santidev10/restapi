import logging

from django.core.management.base import BaseCommand

from utils.youtube_api import YoutubeAPIConnector

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Retrieve and audit comments."

    def handle(self, *args, **kwargs):
        connector = YoutubeAPIConnector()

        connector.get_video_comment_replies("UgwRdzPi9YZJ3uvQza14AaABAg")
