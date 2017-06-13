import sys
from django.core.management.base import BaseCommand

from segment.models import Segment, ChannelRelation


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

        i = 0
        while True:
            i += 1
            line = sys.stdin.readline().strip()
            if not line:
                break
            channel_id, title = tuple(line.split(',', 1))

            segment_data = dict(title=title, segment_type=segment_type, category=category)
            try:
                segment = Segment.objects.get(**segment_data)
            except Segment.DoesNotExist:
                segment = Segment(**segment_data)
                segment.save()

            obj, created = ChannelRelation.objects.get_or_create(pk=channel_id)
            segment.channels.add(obj)
            segment.save()
            print(i, title, segment.channels.all().count())
