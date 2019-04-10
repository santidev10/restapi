import logging
from cProfile import Profile

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from brand_safety.audit_providers.standard_brand_safety_provider import StandardBrandSafetyProvider
from audit_tool.models import APIScriptTracker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        try:
            profiler = Profile()
            profiler.runcall(self.run)
            profiler.print_stats()

        except PidFileAlreadyLockedError:
            print("I am already running")

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        try:
            api_tracker = APIScriptTracker.objects.get(name="StandardAudit")
            standard_audit = StandardBrandSafetyProvider(api_tracker=api_tracker)
            standard_audit.run()
        except Exception as e:
            logger.exception(e)