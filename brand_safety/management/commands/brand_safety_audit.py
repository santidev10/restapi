import logging

from django.core.management.base import BaseCommand

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--ids",
            help="Manual brand safety scoring, should provide ids to update"
        )
        parser.add_argument(
            "--score-vetted-channels",
            action="store_true",
            default=False
        )
        parser.add_argument(
            "--score-vetted-videos",
            action="store_true",
            default=False
        )

    def handle(self, *args, **kwargs):
        ids = kwargs["ids"].strip().split(",")
        ignore_vetted_channels = not kwargs["score_vetted_channels"]
        ignore_vetted_videos = not kwargs["score_vetted_videos"]
        auditor = BrandSafetyAudit(ignore_vetted_channels=ignore_vetted_channels,
                                   ignore_vetted_videos=ignore_vetted_videos)
        if len(ids[0]) < 20:
            auditor.process_videos(ids)
        else:
            auditor.process_channels(ids)
