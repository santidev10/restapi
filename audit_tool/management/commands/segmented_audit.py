import logging

from django.core.management import BaseCommand
from pid import PidFileAlreadyLockedError
from pid.decorator import pidfile

from audit_tool.segmented_audit import SegmentedAudit

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--infinitely",
                            action="store_true",
                            default=False,
                            help="Run in a infinitely cycle")

    def handle(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="segmented_audit.pid")
    def run(self, *_, **options):
        while True:
            audit = SegmentedAudit()
            logger.info("Started segmented audit for the next batch of channels")
            channels_count, videos_count = audit.run()
            logger.info("Done (channels_count=%s, videos_count=%s)", channels_count, videos_count)

            if not options.get("infinitely", False):
                break
