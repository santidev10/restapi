import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from segment.brand_safety_list_generator import BrandSafetyListGenerator
from audit_tool.models import APIScriptTracker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        try:
            self.run()
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        try:
            channel_api_tracker = APIScriptTracker.objects.get_or_create(name="ChannelSegmentGenerator")[0]
            video_api_tracker = APIScriptTracker.objects.get_or_create(name="VideoSegmentGenerator")[0]
            list_generator = BrandSafetyListGenerator(
                channel_api_tracker=channel_api_tracker,
                video_api_tracker=video_api_tracker
            )
            list_generator.run()
        except Exception as e:
            logger.exception(e)
