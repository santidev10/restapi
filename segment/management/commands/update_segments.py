"""
Command to update segments statistics data
"""
import logging
from django.core.management import BaseCommand

from segment.models import Segment

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
        Segment.objects.update_statistics()
        logger.info("Segments update procedure finished")
