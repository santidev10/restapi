from django.core.management.base import BaseCommand
from audit_tool.comment_audit import CommentAudit
import logging
from utils.youtube_api import YoutubeAPIConnector

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        # comment_audit = CommentAudit()
        # comment_audit.run()

        connector = YoutubeAPIConnector()

        # response = connector.get_video_comments('4-k3R6BQjjU')
        response = connector.get_video_comment_replies('UgwRdzPi9YZJ3uvQza14AaABAg')
        print(response)
