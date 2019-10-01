import csv
import os
import tempfile

from django.conf import settings


class ExportContextManager(object):
    CHUNK_SIZE = 1000

    def __init__(self, segment):
        self.segment = segment

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)

        with open(self.filename, mode="w+", newline="") as export_file:
            queryset = self.segment.get_queryset()
            field_names = self.segment.serializer.columns
            writer = csv.DictWriter(export_file, fieldnames=field_names)
            writer.writeheader()
            for item in queryset:
                row = self.segment.serializer(item).data
                writer.writerow(row)
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)
