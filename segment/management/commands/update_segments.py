"""
Command to update segments data
"""
import logging
from django.core.management import BaseCommand

from segment.utils import total_update_segments

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level='INFO')
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Update procedure
    """
    def handle(self, *args, **options):
        """
        Segments update depends on their updated_at time
        """
        logger.info("Start update segments procedure")
        total_update_segments()
        logger.info("Segments update procedure finished")
