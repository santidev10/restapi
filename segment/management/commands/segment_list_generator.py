import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

from django.core.management.base import BaseCommand
from segment.segment_list_generator import SegmentListGenerator
from audit_tool.models import APIScriptTracker
import brand_safety.constants as constants

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            help="List generation type: channel or video"
        )

    def handle(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="standard_brand_safety.pid")
    def run(self, *args, **options):
        try:
            list_type = options["type"]
            if list_type == constants.CHANNEL:
                api_script_tracker = APIScriptTracker.objects.get_or_create(name="ChannelListGenerator")[0]
            elif list_type == constants.VIDEO:
                api_script_tracker = APIScriptTracker.objects.get_or_create(name="VideoListGenerator")[0]
            else:
                raise ValueError("Unsupported list generation type: {}".format(list_type))
            list_generator = SegmentListGenerator(
                list_generator_type=list_type,
                api_script_tracker=api_script_tracker,
            )
            list_generator.run()
        except Exception as e:
            logger.exception(e)