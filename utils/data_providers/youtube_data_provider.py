import time

from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException
from .base import DataProviderMixin


class YoutubeDataProvider(DataProviderMixin):
    youtube_max_channel_list_limit = 50
    youtube_max_video_list_limit = 50
    max_retries = 3
    retry_coeff = 1.5
    retry_sleep = 0.2

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
        if not channel_ids:
            return channel_data
        if type(channel_ids) is str:
            response = self.connector.obtain_channels(channel_ids, part=part)
            try:
                channel_data = response["items"][0]
            except IndexError:
                print("Could not get data for {}".format(channel_ids))
            return channel_data
        else:
            for batch in self.batch(channel_ids, self.youtube_max_channel_list_limit):
                batch_ids = ",".join(batch)
                response = self.connector.obtain_channels(batch_ids, part=part)
                channel_data.extend(response["items"])
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
        :return: (list) Channel videos
        """
        channel_videos = []
        channel_videos_full_data = []
        channel_metadata = self.get_channel_data(channel_id)
        response = self._execute(self.connector.obtain_channel_videos, channel_id, part="snippet", order="viewCount", safe_search="strict")
        channel_videos += [video["id"]["videoId"] for video in response.get("items")]
        next_page_token = response.get("nextPageToken")

        while next_page_token and response.get("items"):
            response = self.connector \
                .obtain_channel_videos(channel_id, part="snippet", page_token=next_page_token, order="viewCount",
                                       safe_search="strict")
            channel_videos += [video["id"]["videoId"] for video in response.get("items")]
            next_page_token = response.get("nextPageToken")

        for video_full_data_batch in self.batch(channel_videos, 50):
            response = self.connector.obtain_videos(",".join(video_full_data_batch), part="snippet,statistics")
            channel_videos_full_data += response.get("items")
        self.set_channel_metadata_videos(channel_videos_full_data, channel_metadata)
        return channel_videos_full_data

    def set_channel_metadata_videos(self, videos, channel):
        """
        Sets channel metadata on videos
        :param videos: Youtube video data
        :param channel: Youtube channel data
        :return: Youtube video data with channel metadata
        """
        for video in videos:
            video["statistics"] = video.get("statistics", {})
            video["statistics"]["channelSubscriberCount"] = channel["statistics"].get("subscriberCount")
            video["snippet"]["country"] = channel["snippet"].get("country", "Unknown")

    def get_channel_id_for_username(self, username):
        """
        Retrieves channel id for the given youtube username
        :param username: (str) youtube username
        :return: (str) channel id
        """
        response = self._execute(self.connector.obtain_user_channels, username)
        try:
            channel_id = response.get("items")[0]["id"]
        except IndexError:
            raise ValueError("Could not get channel data for: {}".format(username))
        return channel_id

    def get_video_data(self, video_ids):
        """
        Retrieves Youtube Video data for video ids
        :param video_ids: Youtube video ids
        :return: Youtube Video data
        """
        all_videos = []
        if video_ids is None:
            return all_videos
        for batch in self.batch(video_ids, self.youtube_max_video_list_limit):
            batch_ids = ",".join(batch)
            response = self._execute(self.connector.obtain_videos, batch_ids, part="snippet,statistics")
            items = response["items"]
            channel_ids = [video["snippet"]["channelId"] for video in items]
            channel_statistics_data = self.get_channel_data(channel_ids)
            video_channel_statistics_ref = {
                channel["id"]: channel["statistics"]
                for channel in channel_statistics_data
            }
            for video in items:
                channel_id = video["snippet"]["channelId"]
                video["statistics"]["channelSubscriberCount"] = video_channel_statistics_ref.get(channel_id, {}).get(
                    "subscriberCount", 0)
                all_videos.append(video)
        return all_videos

    def _execute(self, method, *args, **kwargs):
        """
        Wrapper to retry api calls with faster retries
        """
        retries = 0
        while retries <= self.max_retries:
            try:
                response = method(*args, **kwargs)
                return response
            except YoutubeAPIConnectorException as e:
                print("SDB error", e)
                print("Retrying {} of {}".format(retries, self.max_retries))
                time.sleep(self.retry_coeff ** retries * self.retry_sleep)
                retries += 1
        raise YoutubeDataProviderException("Unable to retrieve Youtube data. args={}, kwargs={}".format(args, kwargs))


class YoutubeDataProviderException(Exception):
    pass

