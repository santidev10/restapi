from aw_reporting.models import YTVideoStatistic
from segment.models.utils.aggregate_segment_statistics import aggregate_segment_statistics
from singledb.connector import SingleDatabaseApiConnector as Connector


class CustomSegmentVideoStatistics(object):
    """
    Compose with CustomSegment model
    """
    related_aw_statistics_model = YTVideoStatistic

    @property
    def singledb_method(self):
        return Connector().get_video_list

    def obtain_singledb_data(self, ids, start=None, end=None):
        """
        Execute call to SDB
        """
        ids = list(ids)[start:end]
        ids_hash = Connector().store_ids(ids)
        params = {
            "ids_hash": ids_hash,
            "fields": "video_id,title,thumbnail_image_url",
            "sources": (),
            "sort": "views:desc",
            "size": 3
        }
        return self.singledb_method(query_params=params)

    def get_statistics(self, segment, data):
        statistics = {
            "adw_data": aggregate_segment_statistics(segment),
            "top_three_items": self.get_top_three_items(data),
            "items_count": segment.related.count()
        }
        return statistics

    def get_top_three_items(self, data):
        top_three_items = [
            {"id": obj.get("video_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("items")
        ]
        return top_three_items
