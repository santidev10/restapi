import collections.abc
import csv
import os
import tempfile

from django.conf import settings

from utils.utils import chunks_generator
from es_components.managers import ChannelManager
from es_components.constants import Sections
from video.tasks.export_generator import VideoListDataGenerator


class ExportContextManager:
    def __init__(self, items, fieldnames):
        self.items = items
        self.fieldnames = fieldnames
        self.filename = None

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)
        with open(self.filename, mode="w+", newline="") as export_file:
            writer = csv.DictWriter(export_file, fieldnames=self.fieldnames)
            writer.writeheader()

            for batch in chunks_generator(self.items, size=1000):
                chunk = list(batch)
                # Ignore videos if its channel is blocklisted
                if isinstance(self.items, VideoListDataGenerator):
                    chunk = self._clean_blocklist(chunk)
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

    def _clean_blocklist(self, chunk):
        if not isinstance(chunk, collections.abc.Sequence):
            chunk = [chunk]
        channel_manager = ChannelManager([Sections.CUSTOM_PROPERTIES])
        channel_blocklist = {
            channel.main.id: channel.custom_properties.blocklist
            for channel in channel_manager.get([video["channel_id"] for video in chunk])
        }
        data = [
            item for item in chunk if channel_blocklist.get(item["channel_id"]) is not True
        ]
        return data
