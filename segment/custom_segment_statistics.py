from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES


class CustomSegmentStatistics(object):
    video_config = {
        "sdb_method": Connector().get_video_list
    }
    channel_config = {
        "sdb_method": Connector().get_channel_list
    }

    def __init__(self, segment_type):
        config = {
            0: self.video_config,
            1: self.channel_config
        }
        self.config = config[segment_type]

    @property
    def singledb_method(self):
        return self.config["sdb_method"]

    @property
    def related_ids(self):
        return self.related.values_list("related_id", flat=True)

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

    def populate_statistics_fields_channel(self, data):
        """
        Update segment statistics fields
        """
        self.channels = data.get("base_data").get("items_count")
        self.set_top_three_channels(data)

        self.videos = sum(
            value.get("videos") or 0
            for value in data.get("base_data").get("items"))

    def populate_statistics_fields_video(self, data):
        """
        Update segment statistics fields
        """
        self.videos = data.get("items_count")
        self.set_top_tree(data)
