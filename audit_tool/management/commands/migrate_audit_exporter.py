import csv

from django.core.management import BaseCommand
from django.contrib.auth import get_user_model
from audit_tool.models import AuditExporter


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--file_path",
            help="Path to CSV file"
        )

    def handle(self, *args, **options):
        file_path = options.get("file_path")

        migrate(file_path)


def migrate(file_path):
    with open(file_path) as csv_file:

        for row in csv.reader(csv_file):
            id, created, clean, completed, file_name, final, audit_id, owner_id = row
            try:
                owner_email = get_user_model().objects.get(id=int(owner_id))
            except get_user_model.DoesNotExist:
                owner_email = None
            AuditExporter.objects.create(
                id=int(id), created=created, clean=clean, completed=completed, file_name=file_name, final=final,
                audit_id=int(audit_id), owner_email=owner_email
            )

