import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from brand_safety.audit_providers.standard_brand_safety_provider import StandardBrandSafetyProvider
from audit_tool.models import APIScriptTracker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'
    def add_arguments(self, parser):
        parser.add_argument(
            "--manual",
            help="Manual brand safety scoring, should provide channel ids to update"
        )

    def handle(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        try:
            api_tracker = APIScriptTracker.objects.get_or_create(name="BrandSafety")[0]
            standard_audit = StandardBrandSafetyProvider(api_tracker=api_tracker)
            if options.get("manual"):
                channel_ids = options["manual"]
                standard_audit.manual_channel_update(channel_ids)
            else:
                standard_audit.run()
        except Exception as e:
            logger.exception(e)
