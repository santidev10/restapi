"""
Command to update segments statistics data
"""
import logging

from django.core.management import BaseCommand

from segment.tasks import update_segments_stats

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
        update_segments_stats.delay()
        logger.info("Segments update statistics procedure finished")