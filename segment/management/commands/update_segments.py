"""
Command to update segments data
"""
import logging

from django.core.management import BaseCommand

from segment.utils import total_update_segments

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--force-creation',
                            action='store_true',
                            default=False,
                            help="Force segments creation")

    """
    Update procedure
    """
    def handle(self, *args, **options):
        """
        Segments update depends on their updated_at time
        """

        logger.info("Start update segments procedure")
        force_creation = options.get('force_creation', False)
        if force_creation:
            logger.info('Segment creation requested by --force-creation')

        total_update_segments(force_creation=force_creation)
        logger.info("Segments update procedure finished")
