import csv

from django.core.management import BaseCommand

from segment.models.persistent.channel import PersistentSegmentChannel
from segment.models.persistent.video import PersistentSegmentVideo


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--file_path",
            help="Path to CSV file"
        )
        parser.add_argument(
            "--model_type",
            help="Segment model type: channel or video"
        )

    def handle(self, *args, **options):
        file_path = options.get("file_path")
        model_type = options.get("model_type")

        if model_type == 'channel':
            model = PersistentSegmentChannel
        elif model_type == 'video':
            model = PersistentSegmentVideo
        else:
            raise Exception("Model type not recognised")

        migrate(file_path, model)


def migrate(file_path, model):
    with open(file_path) as csv_file:
        for row in csv.reader(csv_file):
            segment_id, auth_category_id = row
            try:
                model.objects.get(id=int(segment_id)).update(audit_category_id=int(auth_category_id))
            except (model.DoesNotExist, ValueError):
                continue
