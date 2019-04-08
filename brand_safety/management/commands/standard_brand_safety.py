import logging
import time

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from brand_safety.standard_brand_safety_provider import StandardBrandSafetyProvider
from audit_tool.models import APIScriptTracker

logger = logging.getLogger("slack_update")


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        try:
            self.run()
        except PidFileAlreadyLockedError:
            print("I am already running")

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        # api_tracker = APIScriptTracker.objects.get(name="StandardAudit")
        # standard_audit = StandardBrandSafetyProvider(api_tracker=api_tracker)
        # standard_audit.run()
        try:
            print('running brand safety...')
            time.sleep(120)
            print('brand safety complete.')
            logger.info('brand safety complete.')
        except Exception as e:
            print(e)