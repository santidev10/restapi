"""
Youtube api connector module
"""
import logging
import time

from apiclient import discovery
from django.conf import settings

logger = logging.getLogger(__name__)


class YoutubeAPIConnectorException(Exception):
    """
    Exception for youtube connector
    """
    pass


class YoutubeAPIConnector(object):
    """
    Connector to youtube api
    """
    MAX_RETRIES = 5
    RETRY_DELAY = 30

    def __init__(self, developer_key=settings.YOUTUBE_API_DEVELOPER_KEY,
                 service_name='youtube', api_version='v3'):
        """
        Make default setting
        """
        self.developer_key = developer_key
        self.service_name = service_name
        self.api_version = api_version
        self.max_connect_retries = 5
        self.retry_sleep_coefficient = 4
        self.token_revoked = False
        self.credentials = None
        self.youtube = discovery.build(
            self.service_name,
            self.api_version,
            developerKey=self.developer_key
        )

    def keyword_search(self, keyword, part="snippet", search_type="channel",
                       max_results=50, safe_search="none", page_token=None):
        """
        Make search by keyword
        """
        options = {
            "q": keyword,
            "part": part,
            "type": search_type,
            "maxResults": max_results,
            "safeSearch": safe_search
        }
        if page_token:
            options["pageToken"] = page_token
        return self.__execute_call(self.youtube.search().list(**options))

    def obtain_channels(self,
                        channels_ids,
                        part="id,snippet,statistics,"
                             "contentDetails,brandingSettings",
                        max_results=50):
        """
        Obtain channels by ids
        """
        options = {
            'part': part,
            'maxResults': max_results,
            'id': channels_ids
        }
        return self.__execute_call(self.youtube.channels().list(**options))

    def obtain_user_channels(self, username, part="id"):
        """
        Get channels for authorized user
        """
        options = {
            "part": part,
            "forUsername": username
        }
        return self.__execute_call(self.youtube.channels().list(**options))

    def obtain_channel_videos(self,
                              channel_id,
                              part="id",
                              search_type="video",
                              max_results=50,
                              safe_search='none',
                              order="date",
                              page_token=None,
                              published_after=None):
        """
        Obtain videos from channel
        """
        options = {
            'part': part,
            'maxResults': max_results,
            'channelId': channel_id,
            'type': search_type,
            'safeSearch': safe_search,
            'order': order
        }
        if page_token:
            options["pageToken"] = page_token
        if published_after:
            options["publishedAfter"] = '{:%Y-%m-%dT00:00:00Z}'.format(
                published_after)
        return self.__execute_call(self.youtube.search().list(**options))

    def obtain_videos(self,
                      videos_ids,
                      part="id,statistics",
                      max_results=50,
                      page_token=None):
        """
        Obtain video by ids
        """
        options = {
            "part": part,
            "maxResults": max_results,
            "id": videos_ids
        }
        if page_token:
            options["pageToken"] = page_token
        return self.__execute_call(self.youtube.videos().list(**options))

    def __execute_call(self, method):
        """
        Call YT api
        """
        tries_count = 0
        while tries_count <= self.max_connect_retries:
            try:
                result = method.execute()
            except Exception:
                tries_count += 1
                if tries_count <= self.max_connect_retries:
                    sleep_seconds_count = self.max_connect_retries \
                                          ** self.retry_sleep_coefficient
                    time.sleep(sleep_seconds_count)
            else:
                return result
        raise YoutubeAPIConnectorException(
            "Unable to obtain data from YouTube")