"""
Command to update segments statistics data
"""
import logging

from django.core.management import BaseCommand

from segment.tasks import cleanup_segments_related

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Update procedure
    """
    def handle(self, *args, **options):
        """
        Segments update depends on their updated_at time
        """

        logger.info("Start update segments statistics procedure")
        cleanup_segments_related.delay()
        logger.info("Segments update statistics procedure finished")