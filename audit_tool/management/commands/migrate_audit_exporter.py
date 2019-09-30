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
            for index, value in enumerate(row):
                if value in ('\\n', '\\N'):
                    row[index] = None

            id, created, clean, completed, file_name, final, audit_id, owner_id = row

            AuditExporter.objects.create(
                id=id, created=created, clean=clean, completed=completed, file_name=file_name, final=final,
                audit_id=audit_id, owner_id=owner_id
            )

