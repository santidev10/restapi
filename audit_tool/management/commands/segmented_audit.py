import logging
from django.core.management import BaseCommand
from audit_tool.segmented_audit import SegmentedAudit

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('--infinitely',
                            action='store_true',
                            default=False,
                            help="Run in a infinitely cycle")

    def handle(self, *args, **options):
        while True:
            audit = SegmentedAudit()
            logger.info("Started segmented audit for the next batch of channels")
            channels_count, videos_count = audit.run()
            logger.info("Done (channels_count={}, videos_count={})".format(channels_count, videos_count))

            if not options.get("infinitely", False):
                break
