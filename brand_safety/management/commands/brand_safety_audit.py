import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from audit_tool.models import APIScriptTracker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def add_arguments(self, parser):
        parser.add_argument(
            "--manual",
            help="Manual brand safety scoring, video or channel"
        )
        parser.add_argument(
            "--ids",
            help="Manual brand safety scoring, should provide ids to update"
        )

    def handle(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        try:
            if options.get("manual"):
                self._handle_manual(*args, **options)
            else:
                self._handle_standard(*args, **options)
        except Exception as e:
            logger.exception(e)

    def _handle_manual(self, *args, **options):
        manual_type = options["manual"]
        manual_ids = options["ids"].strip().split(",")
        standard_audit = BrandSafetyAudit()
        if manual_type == "video":
            standard_audit.manual_video_audit(manual_ids)
        elif manual_type == "channel":
            standard_audit.manual_channel_audit(manual_ids)
        else:
            raise ValueError("Unsupported manual type: {}".format(manual_type))

    def _handle_standard(self, *args, **options):
        api_tracker = APIScriptTracker.objects.get_or_create(name="BrandSafety")[0]
        standard_audit = BrandSafetyAudit(api_tracker=api_tracker)
        standard_audit.run()
