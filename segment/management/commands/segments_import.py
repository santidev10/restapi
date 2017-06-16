import logging
import sys
from django.core.management.base import BaseCommand

from segment.models import get_segment_model_by_type

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level='INFO')
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Import segments"

    def add_arguments(self, parser):
        parser.add_argument('--type',
                            type=str,
                            default='channel')

        parser.add_argument('--category',
                            type=str,
                            default='youtube')

        parser.add_argument('--category-limit',
                            type=int,
                            default=None)

    def handle(self, *args, **options):
        segment_type = options.get('type')
        self.category = options.get('category')
        self.category_limit = options.get('category_limit')
        self.model = get_segment_model_by_type(segment_type)

        if self.category not in dict(self.model.CATEGORIES):
            raise Exception("Invalid category")

        title_prev = None
        related_ids = []
        while True:
            line = sys.stdin.readline().strip()
            if not line:
                break
            related_id, title = tuple(line.split(',', 1))

            if title_prev and title_prev != title:
                self.save_data(title, related_ids)
                related_ids = []

            title_prev = title

            if not self.category_limit or len(related_ids) < self.category_limit:
                related_ids.append(related_id)

        if related_ids and title_prev:
            self.save_data(title, related_ids)

    def save_data(self, title, ids):
        logger.info('Saving {} ids for segment: {}'.format(len(ids), title))
        segment_data = dict(title=title, category=self.category)
        segment, created = self.model.objects.get_or_create(title=title, category=self.category)
        segment.add_related_ids(ids)
        segment.update_statistics(segment)
