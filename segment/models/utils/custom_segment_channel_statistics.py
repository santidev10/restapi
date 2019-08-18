from aw_reporting.models import YTChannelStatistic
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES
from singledb.settings import DEFAULT_VIDEO_LIST_SOURCES


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

    def get_statistics(self, segment, segment_item_ids):
        """
        Prepare segment adwords statistics
        """
        statistics = {
            "adw_data": aggregate_segment_statistics(segment, segment_item_ids),
            "items_count": len(segment_item_ids)
        }
        return statistics
