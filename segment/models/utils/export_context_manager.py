import csv
import os
import tempfile

from django.conf import settings


class ExportContextManager(object):
    CHUNK_SIZE = 1000

    def __init__(self, segment, queryset=None):
        self.segment = segment
        if queryset is None:
            self.queryset = self.segment.get_queryset(sort=self.segment.config.SORT_KEY)
        else:
            self.queryset = queryset

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)

        with open(self.filename, mode="w+", newline="") as export_file:
            field_names = self.segment.user_export_serializer.columns
            writer = csv.DictWriter(export_file, fieldnames=field_names)
            writer.writeheader()
            for item in self.queryset:
                row = self.segment.user_export_serializer(item).data
                writer.writerow(row)
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)
