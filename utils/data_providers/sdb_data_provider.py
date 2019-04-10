import time

from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from .base import DataProviderMixin


class SDBDataProvider(DataProviderMixin):
    video_fields = "video_id,title,channel_id,channel__title,channel__subscribers,description," \
                   "tags,category,likes,dislikes,views,language,transcript,country,thumbnail_image_url"
    channel_fields = "channel_id,title,description,category,subscribers,likes,dislikes,views,language,url,country,thumbnail_image_url"
    max_retries = 3
    retry_coeff = 1.5
    retry_sleep = 0.2
    video_batch_limit = 10000
    get_channel_data_limit = 10000
    get_channels_videos_limit = 50

    def __init__(self,):
        self.sdb_connector = SingleDatabaseApiConnector()

    def get_channels_videos(self, channel_ids):
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) Channel id strings
        :return: (list) video objects from singledb
        """
        video_data = []
        for batch in self.batch(channel_ids, self.get_channels_videos_limit):
            params = dict(
                fields=self.video_fields,
                sort="video_id",
                size=self.video_batch_limit,
                channel_id__terms=",".join(batch),
            )
            response = self._execute(self.sdb_connector.execute_get_call, "videos/", params)
            video_data.extend(response.get("items", []))
        return video_data

    def es_index_brand_safety_results(self, results, doc_type):
        """
        Send items to singledb for brand safety es indexing
        :param results: list -> Audit brand safety results
        :param doc_type: Index document type to index results in
        :return: singledb response
        """
        response = self._execute(self.sdb_connector.post_brand_safety_results, results, doc_type)
        return response

    def get_channel_data(self, channel_ids):
        """
        Retrieve channel data from singledb with channel ids
        :param channel_ids:
        :return:
        """
        channel_data = []
        for batch in self.batch(channel_ids, self.get_channel_data_limit):
            params = dict(
                fields=self.channel_fields,
                size=self.get_channel_data_limit,
                channel_id__terms=",".join(batch)
            )
            response = self._execute(self.sdb_connector.get_channel_list, params)
            channel_data.extend(response.get("items", []))
        return channel_data

    def _execute(self, method, *args, **kwargs):
        """
        Wrapper to retry api calls with faster retries
        """
        retries = 0
        while retries <= self.max_retries:
            try:
                response = method(*args, **kwargs)
                return response
            except SingleDatabaseApiConnectorException as e:
                print("SDB error", e)
                print("Retrying {} of {}".format(retries, self.max_retries))
                time.sleep(self.retry_coeff ** retries * self.retry_sleep)
                retries += 1
        raise SDBDataProviderException("Unable to retrieve SDB data. args={}, kwargs={}".format(args, kwargs))

    def get_all_channels_batch_generator(self, last_id=None,):
        size = self.get_channel_data_limit + 1 if last_id else self.get_channels_videos_limit
        params = dict(
            fields=self.channel_fields,
            sort="channel_id",
            size=size,
            channel_id__range="{},".format(last_id or ""),
        )
        while True:
            response = self._execute(self.sdb_connector.get_channel_list, params, True)
            if last_id:
                last_id = None
                channels = [item for item in response.get("items", []) if item["channel_id"] != last_id]
                params["channel_id__range"] = "{},"
                params["size"] = self.get_channel_data_limit
            else:
                channels = response.get("items")
            for channel in channels:
                if not channel.get("category"):
                    channel["category"] = "Unclassified"
                if not channel.get("language"):
                    channel["language"] = "Unknown"
            if not channels:
                break
            else:
                yield channels


class SDBDataProviderException(Exception):
    pass
