import csv

from segment.segment_list_generator import SegmentListGenerator
from brand_safety.audit_providers.standard_brand_safety_provider import StandardBrandSafetyProvider
import brand_safety.constants as constants


class CustomSegmentImporter(object):
    def __init__(self, *args, **kwargs):
        self.data_type = kwargs["type"]
        self.youtube_ids = self._read_csv(kwargs["path"])
        self.config = self._get_config(self.data_type)
        self.brand_safety_provider = StandardBrandSafetyProvider()
        self.list_generator = SegmentListGenerator()

    def run(self):
        # use ids to get data from sdb
        # get reamining ids from youtube api
        # use merged data to score items
        # save data to segment
        # audits = self.config["provider"]()
        for i in self.youtube_ids:
            print(i)


    def _read_csv(self, path):
        with open(path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            ids = list(reader)
        return ids

    def _get_config(self, data_type):
        config = {
            constants.VIDEO: {
                "provider": self.brand_safety_provider.manual_video_update
            },
            constants.CHANNEL: {
                "provider": self.brand_safety_provider.manual_channel_update
            },
        }
        return config[data_type]
