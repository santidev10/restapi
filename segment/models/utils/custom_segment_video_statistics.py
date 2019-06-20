from aw_reporting.models import YTVideoStatistic
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES


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

    def set_adw_statistics(self):
        """
        Prepare segment adwords statistics
        """
        from segment.models.utils.count_segment_adwords_statistics import count_segment_adwords_statistics
        # prepare adwords statistics
        adwords_statistics = count_segment_adwords_statistics(self)

        # finalize data
        self.adw_data.update(adwords_statistics)

    def populate_statistics_fields(self, data):
        """
        Update segment statistics fields
        """
        self.videos = data.get("items_count")
        self.set_top_three_items(data)

    def set_top_three_items(self, data):
        self.top_three_items = [
            {"id": obj.get("video_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("items")
        ]

    def set_total_for_huge_segment(self, items_count, data):
        self.videos = items_count
        if data is None:
            self.top_three_videos = dict()
            return
        self.set_top_three_items(data)
