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

    def handle(self, *args, **kwargs):
        ids = kwargs["ids"].strip().split(",")
        auditor = BrandSafetyAudit()
        if len(ids[0]) < 20:
            auditor.process_videos(ids)
        else:
            auditor.process_channels(ids)
