from django.core.management.base import BaseCommand
from keyword_tool.models import Interest

from collections import namedtuple
import csv

import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):

        bulk_data = []

        with open('keyword_tool/fixtures/productsservices.csv') as f:
            content = f.read()
            reader = csv.reader(content.split('\n'), delimiter=',')
            header = next(reader)
            row = namedtuple('Row', header)

            ids = Interest.objects.values_list('id', flat=True)

            for row_data in reader:
                if row_data:
                    r = row(*row_data)
                    if r.ID not in ids:
                        bulk_data.append(
                            Interest(
                                id=r.ID,
                                name=r.Category,
                            )
                        )

        if bulk_data:
            Interest.objects.bulk_create(bulk_data)
