import collections.abc
import csv
import os
import tempfile

from django.conf import settings


class ExportContextManager(object):
    def __init__(self, items, fieldnames):
        self.items = items
        self.fieldnames = fieldnames
        self.filename = None

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)
        with open(self.filename, mode="w+", newline="") as export_file:
            writer = csv.DictWriter(export_file, fieldnames=self.fieldnames)
            writer.writeheader()
            for chunk in self.items:
                # Each chunk may be a sequence itself from a generator
                if isinstance(chunk, collections.abc.Sequence) and not isinstance(chunk, str):
                    writer.writerows(self._get_export_data(chunk, sequence=True))
                else:
                    writer.writerow(self._get_export_data(chunk, sequence=False))
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)

    def _get_export_data(self, chunk, sequence=False):
        if sequence:
            data = [{
                key: value
                for key, value in item.items() if key in self.fieldnames
            } for item in chunk]
        else:
            data = {
                key: value
                for key, value in chunk.items() if key in self.fieldnames
            }
        return data
