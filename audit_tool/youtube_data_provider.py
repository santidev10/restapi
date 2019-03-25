from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException

class YoutubeDataProvider(object):
    youtube_max_channel_list_limit = 50
    youtube_max_video_list_limit = 50

    def __init__(self):
        self.connector = YoutubeAPIConnector()

    def get_channel_data(self, channel_ids):
        """
        Gets channel statistics for videos
        :param channel_ids: List of Youtube channel ids
        :return: (dict) Mapping of channels and their statistics
        """
        channel_data = []
        cursor = 0

        if type(channel_ids) is str:
            channel_data = self.connector.obtain_channels(channel_ids, part='statistics,snippet')

        else:
            while cursor < len(channel_ids):
                batch = channel_ids[cursor:cursor + self.youtube_max_channel_list_limit]
                batch_ids = ','.join(batch)

                response = self.connector.obtain_channels(batch_ids, part='statistics,snippet')
                channel_data += response['items']

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

        channel_metadata = self.get_channel_data(channel_id)['items'][0]
        response = self.connector.obtain_channel_videos(channel_id, part='snippet', order='viewCount', safe_search='strict')

        channel_videos += [video['id']['videoId'] for video in response.get('items')]
        next_page_token = response.get('nextPageToken')

        while next_page_token and response.get('items'):
            response = self.connector \
                .obtain_channel_videos(channel_id, part='snippet', page_token=next_page_token, order='viewCount',
                                       safe_search='strict')

            channel_videos += [video['id']['videoId'] for video in response.get('items')]
            next_page_token = response.get('nextPageToken')

        while channel_videos:
            video_full_data_batch = channel_videos[:50]

            response = self.connector.obtain_videos(','.join(video_full_data_batch), part='snippet,statistics')

            channel_videos_full_data += response.get('items')
            channel_videos = channel_videos[50:]

        self.set_channel_metadata_videos(channel_videos_full_data, channel_metadata)

        return channel_videos_full_data

    def set_channel_metadata_videos(self, videos, channel):
        for video in videos:
            video['statistics'] = video.get('statistics', {})
            video['statistics']['channelSubscriberCount'] = channel['statistics'].get('subscriberCount')
            video['snippet']['country'] = channel['snippet'].get('country', 'Unknown')

    def get_channel_id_for_username(self, username, connector):
        """
        Retrieves channel id for the given youtube username
        :param username: (str) youtube username
        :param connector: YoutubeAPIConnector instance
        :return: (str) channel id
        """
        response = connector.obtain_user_channels(username)

        try:
            channel_id = response.get('items')[0].get('id')

        except IndexError:
            raise ValueError('Could not get channel id for: {}'.format(username))

        return channel_id

    def get_video_data(self, video_ids):
        all_videos = []
        cursor = 0

        while cursor < len(video_ids):
            batch = video_ids[cursor:cursor + self.youtube_max_video_list_limit]
            batch_ids = ','.join(batch)

            response = self.connector.obtain_videos(batch_ids, part='snippet,statistics')
            items = response['items']
            channel_ids = [video['snippet']['channelId'] for video in items]

            channel_statistics_data = self.get_channel_data(channel_ids)

            video_channel_statistics_ref = {
                channel['id']: channel['statistics']
                for channel in channel_statistics_data
            }

            for video in items:
                channel_id = video['snippet']['channelId']
                video['statistics']['channelSubscriberCount'] = video_channel_statistics_ref[channel_id]['subscriberCount']
                all_videos.append(video)

            cursor += len(batch)

        return all_videos
