import logging

from pid.decorator import pidfile
from pid import PidFile
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
        ),
        parser.add_argument(
            "--finalize",
            help="Finalize existing segments for s3."
        )

    def handle(self, *args, **kwargs):
        list_type = kwargs["type"]
        pid_file = "{}_segment_list_generator.pid".format(list_type)
        with PidFile(pid_file, piddir=".") as pid:
            try:
                self.run(*args, **kwargs)
            except PidFileAlreadyLockedError:
                logger.info("Already running")

    def run(self, *args, **options):
        try:
            list_type = options["type"]
            finalize = options.get("finalize")
            if list_type == constants.CHANNEL:
                script_tracker = APIScriptTracker.objects.get_or_create(name="ChannelListGenerator")[0]
            elif list_type == constants.VIDEO:
                script_tracker = APIScriptTracker.objects.get_or_create(name="VideoListGenerator")[0]
            else:
                raise ValueError("Unsupported list generation type: {}".format(list_type))
            list_generator = SegmentListGenerator(
                list_generator_type=list_type,
                script_tracker=script_tracker,
            )
            if finalize:
                logger.info("Finalizing segments...")
                list_generator._finalize_segments()
            else:
                list_generator.run()
        except Exception as e:
            logger.exception(e)
