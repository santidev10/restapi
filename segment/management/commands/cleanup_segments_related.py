"""
Command to update segments statistics data
"""
import logging

from django.core.management import BaseCommand

from segment.tasks import cleanup_segments_related

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Clean up segments related records.
    """
    def handle(self, *args, **options):
        logger.info("Start clean up segments related records procedure")
        cleanup_segments_related.delay()
