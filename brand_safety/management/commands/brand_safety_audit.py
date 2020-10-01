import logging

from django.core.management.base import BaseCommand

from brand_safety.auditors.channel_auditor import ChannelAuditor
from brand_safety.auditors.video_auditor import VideoAuditor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            help="Manual brand safety scoring, should provide ids to update"
        )

    def handle(self, *args, **kwargs):
        ids = kwargs["ids"].strip().split(",")
        if len(ids[0]) < 20:
            auditor = VideoAuditor()
            auditor.process(ids)
        else:
            auditor = ChannelAuditor()
            auditor.process(ids)
