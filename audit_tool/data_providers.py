import time
from utils.youtube_api import YoutubeAPIConnector, YoutubeAPIConnectorException
from singledb.connector import SingleDatabaseApiConnector, SingleDatabaseApiConnectorException

class YoutubeDataProvider(object):
    youtube_max_channel_list_limit = 50
    youtube_max_video_list_limit = 50

    def __init__(self):
        self.connector = YoutubeAPIConnector()

    def get_channel_data(self, channel_ids, part="statistics,snippet"):
        """
        Gets channel statistics for videos
        :param channel_ids: List of Youtube channel ids
        :param part: Youtube part data to retrieve
        :return: (dict) Mapping of channels and their statistics
        """
        channel_data = []
        cursor = 0

        if type(channel_ids) is str:
            channel_data = self.connector.obtain_channels(channel_ids, part=part)

        else:
            while cursor < len(channel_ids):
                batch = channel_ids[cursor:cursor + self.youtube_max_channel_list_limit]
                batch_ids = ",".join(batch)

                response = self.connector.obtain_channels(batch_ids, part=part)
                channel_data += response["items"]

                cursor += len(batch)

        return channel_data

    def get_channel_video_data(self, channel_ids: list):
        """
        Gets all video metadata for each channel in channel_ids
        :param channel_ids: Youtube channel id strings
        :return: Youtube Video data
        """
        all_results = []

        for id in channel_ids:
            results = self.get_channel_videos(id)
            all_results += results

        return all_results

    def get_channel_videos(self, channel_id):
        """
        Retrieves all videos for given channel id from Youtube Data API
        :param channel_id: (str)
        :param connector: YoutubeAPIConnector instance
        :return: (list) Channel videos
        """
        channel_videos = []
        channel_videos_full_data = []

        channel_metadata = self.get_channel_data(channel_id)["items"][0]
        response = self.connector.obtain_channel_videos(channel_id, part="snippet", order="viewCount", safe_search="strict")

        channel_videos += [video["id"]["videoId"] for video in response.get("items")]
        next_page_token = response.get("nextPageToken")

        while next_page_token and response.get("items"):
            response = self.connector \
                .obtain_channel_videos(channel_id, part="snippet", page_token=next_page_token, order="viewCount",
                                       safe_search="strict")

            channel_videos += [video["id"]["videoId"] for video in response.get("items")]
            next_page_token = response.get("nextPageToken")

        while channel_videos:
            video_full_data_batch = channel_videos[:50]

            response = self.connector.obtain_videos(",".join(video_full_data_batch), part="snippet,statistics")

            channel_videos_full_data += response.get("items")
            channel_videos = channel_videos[50:]

        self.set_channel_metadata_videos(channel_videos_full_data, channel_metadata)

        return channel_videos_full_data

    def set_channel_metadata_videos(self, videos, channel):
        for video in videos:
            video["statistics"] = video.get("statistics", {})
            video["statistics"]["channelSubscriberCount"] = channel["statistics"].get("subscriberCount")
            video["snippet"]["country"] = channel["snippet"].get("country", "Unknown")

    def get_channel_id_for_username(self, username):
        """
        Retrieves channel id for the given youtube username
        :param username: (str) youtube username
        :param connector: YoutubeAPIConnector instance
        :return: (str) channel id
        """
        response = self.connector.obtain_user_channels(username)

        try:
            channel_id = response.get("items")[0].get("id")

        except IndexError:
            raise ValueError("Could not get channel id for: {}".format(username))

        return channel_id

    def get_video_data(self, video_ids):
        all_videos = []
        cursor = 0

        while cursor < len(video_ids):
            batch = video_ids[cursor:cursor + self.youtube_max_video_list_limit]
            batch_ids = ",".join(batch)

            response = self.connector.obtain_videos(batch_ids, part="snippet,statistics")
            items = response["items"]
            channel_ids = [video["snippet"]["channelId"] for video in items]

            channel_statistics_data = self.get_channel_data(channel_ids)

            video_channel_statistics_ref = {
                channel["id"]: channel["statistics"]
                for channel in channel_statistics_data
            }

            for video in items:
                channel_id = video["snippet"]["channelId"]
                video["statistics"]["channelSubscriberCount"] = video_channel_statistics_ref.get(channel_id, {}).get("subscriberCount", 0)
                all_videos.append(video)

            cursor += len(batch)

        return all_videos


class SDBDataProvider(object):
    video_fields = "video_id,channel_id, channel_title,title,description,tags,category,likes,dislikes,views,language,transcript"
    channel_fields = "channel_id,title,description,category,subscribers,likes,dislikes,views,language"
    max_retries = 3
    retry_coeff = 1.5
    retry_sleep = 0.2
    video_batch_limit = 10000
    channel_batch_limit = 100

    def __init__(self,):
        self.sdb_connector = SingleDatabaseApiConnector()

    def get_channels_videos_batch(self, channel_ids):
        """
        Retrieves all videos associated with channel_ids
        :param channel_ids: (list) Channel id strings
        :return: (list) video objects from singledb
        """
        params = dict(
            fields=self.video_fields,
            sort="video_id",
            size=self.video_batch_limit,
            channel_id__terms=",".join(channel_ids),
        )
        response = self._execute(self.sdb_connector.execute_get_call, "videos/", params)
        return response.get("items")

    def get_all_channels_batch_generator(self, last_id=None,):
        size = self.channel_batch_limit + 1 if last_id else self.channel_batch_limit
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
                params["size"] = self.channel_batch_limit
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

    def es_index_brand_safety_results(self, results):
        response = self._execute(self.sdb_connector.post_brand_safety_results, results)
        return response

    def _execute(self, method, *args, **kwargs):
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
        return None


