from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES


class CustomSegmentChannelStatistics(object):
    @property
    def singledb_method(self):
        return Connector().get_channel_list

    def get_ids_hash(self, ids, start=None, end=None):
        ids = list(ids)[start:end]
        return Connector().store_ids(ids)

    def get_data_by_ids(self, ids, start=None, end=None):
        ids_hash = self.get_ids_hash(ids, start, end)
        return self.obtain_singledb_data(ids_hash)

    def obtain_singledb_data(self, ids_hash):
        """
        Execute call to SDB
        """
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

    def get_adw_statistics(self):
        """
        Prepare segment adwords statistics
        """
        from segment.models.utils import count_segment_adwords_statistics
        # prepare adwords statistics
        adwords_statistics = count_segment_adwords_statistics(self)

        # finalize data
        self.adw_data.update(adwords_statistics)

    def populate_statistics_fields(self, data):
        """
        Update segment statistics fields
        """
        self.channels = data.get("base_data").get("items_count")
        self.set_top_three_items(data)

        self.videos = sum(
            value.get("videos") or 0
            for value in data.get("base_data").get("items"))

    def set_top_three_items(self, data):
        self.top_three_items = [
            {"id": obj.get("channel_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("top_three_channels_data").get("items")
        ]
