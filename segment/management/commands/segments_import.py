import sys
from django.core.management.base import BaseCommand

from segment.models import get_segment_model_by_type


class Command(BaseCommand):
    help = "Import segments"

    def add_arguments(self, parser):
        parser.add_argument('--type',
                            type=str,
                            default='channel')

        parser.add_argument('--category',
                            type=str,
                            default='youtube')


    def handle(self, *args, **options):
        segment_type = options.get('type')
        category = options.get('category')
        model = get_segment_model_by_type(segment_type)
        related_model = model.related.rel.related_model

        if category not in dict(model.CATEGORIES):
            raise Exception("Invalid category")

        i = 0
        while True:
            i += 1
            line = sys.stdin.readline().strip()
            if not line:
                break
            related_id, title = tuple(line.split(',', 1))

            segment_data = dict(title=title, category=category)
            try:
                segment = model.objects.get(**segment_data)
            except model.DoesNotExist:
                segment = model(**segment_data)
                segment.save()

            related, created = related_model.objects.get_or_create(related_id=related_id, segment=segment)
            print( i, 'add', related_id, 'to', title)
