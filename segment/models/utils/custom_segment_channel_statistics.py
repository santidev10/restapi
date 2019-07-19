from aw_reporting.models import YTChannelStatistic
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES
from segment.models.utils.count_segment_adwords_statistics import count_segment_adwords_statistics


class CustomSegmentChannelStatistics(object):
    related_aw_statistics_model = YTChannelStatistic

    @property
    def singledb_method(self):
        return Connector().get_channel_list

    def obtain_singledb_data(self, ids, start=None, end=None):
        """
        Execute call to SDB
        """
        ids = list(ids)[start:end]
        ids_hash = Connector().store_ids(ids)
        params = {
            "ids_hash": ids_hash,
            "fields": "channel_id,title,thumbnail_image_url",
            "sources": DEFAULT_CHANNEL_LIST_SOURCES,
            "sort": "subscribers:desc",
            "size": 3
        }
        top_three_channels_data = self.singledb_method(query_params=params)

        params = {
            "ids_hash": ids_hash,
            "fields": "videos",
            "sources": DEFAULT_VIDEO_LIST_SOURCES,
            "size": 10000
        }
        base_data = self.singledb_method(query_params=params)
        data = {
            "top_three_channels_data": top_three_channels_data,
            "base_data": base_data
        }
        return data

    def get_statistics(self, segment, data):
        """
        Prepare segment adwords statistics
        """
        statistics = {
            "adw_data": count_segment_adwords_statistics(segment),
            "top_three_items": self.get_top_three_items(data),
            "items_count": data.get("base_data").get("items_count"),
        }
        return statistics

    def get_top_three_items(self, data):
        top_three_items = [
            {"id": obj.get("channel_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("top_three_channels_data").get("items")
        ]
        return top_three_items
