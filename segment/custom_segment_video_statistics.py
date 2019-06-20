from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES


class CustomSegmentVideoStatistics(object):
    """
    Compose with CustomSegment model
    """
    @property
    def related_ids(self):
        return self.related.values_list("related_id", flat=True)

    @property
    def singledb_method(self):
        return Connector().get_video_list

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
            "fields": "video_id,title,thumbnail_image_url",
            "sources": (),
            "sort": "views:desc",
            "size": 3
        }
        return self.singledb_method(query_params=params)

    def update_statistics(self):
        """
        Process segment statistics fields
        """
        ids = self.related_ids
        ids_count = ids.count()
        data = self.get_data_by_ids(ids)
        # just return on any fail
        if data is None:
            return
        # populate statistics fields
        self.populate_statistics_fields(data)
        self.get_adw_statistics()
        self.save()
        return "Done"

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
        self.videos = data.get("items_count")
        self.set_top_three_items(data)

    def set_top_three_items(self, data):
        self.set_top_three_items = [
            {"id": obj.get("video_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("items")
        ]

