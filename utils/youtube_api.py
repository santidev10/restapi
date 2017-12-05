"""
Youtube api connector module
"""
import json
import logging
import time
from urllib.request import urlopen, URLError
from xml.etree import ElementTree

import httplib2
from apiclient.discovery import build
from django.conf import settings
from googleapiclient.http import HttpError as GoogleHttpError
from oauth2client.client import AccessTokenCredentialsError
from oauth2client.client import GoogleCredentials
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.status import HTTP_403_FORBIDDEN

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
        # self.access_token = access_token
        # self.refresh_token = refresh_token
        # self.token_expiry = token_expiry

        self.token_revoked = False
        self.credentials = None

        # if access_token or refresh_token:
        #     credentials = GoogleCredentials(access_token=access_token,
        #                                     client_id=settings.GOOGLE_APP_AUD,
        #                                     client_secret=settings.GOOGLE_APP_SECRET,
        #                                     refresh_token=refresh_token,
        #                                     token_expiry=self.token_expiry,
        #                                     token_uri="https://accounts.google.com/o/oauth2/token",
        #                                     user_agent='my-user-agent/1.0')
        #     http = credentials.authorize(httplib2.Http())
        #     self.credentials = credentials
        #     self.youtube = build(self.service_name,
        #                          self.api_version,
        #                          http=http)
        # else:
        self.youtube = build(self.service_name,
                             self.api_version,
                             developerKey=self.developer_key)

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
    #
    # def playlist_search(self,
    #                     playlist_ids,
    #                     part="snippet",
    #                     max_results=50):
    #     """
    #     Make channels search by channels ids
    #     """
    #     options = {
    #         'part': part,
    #         'maxResults': max_results,
    #         'id': playlist_ids
    #     }
    #     return self.make_call(self.youtube.playlists().list(**options))
    #
    # def playlistitems_search(self,
    #                          playlist_id,
    #                          part="snippet",
    #                          max_results=50,
    #                          page_token=None):
    #     """
    #     Make channels search by channels ids
    #     """
    #     options = {
    #         'part': part,
    #         'maxResults': max_results,
    #         'playlistId': playlist_id
    #     }
    #     if page_token:
    #         options["pageToken"] = page_token
    #     return self.make_call(self.youtube.playlistItems().list(**options))
    #
    # def own_channels(self, part='id'):
    #     """
    #     Get channels for authorized user
    #     """
    #     assert self.access_token, "No access token"
    #     options = {
    #         'part': part,
    #         'mine': True
    #     }
    #     return self.make_call(self.youtube.channels().list(**options))
    #

    def obtain_user_channels(self, username, part="id"):
        """
        Get channels for authorized user
        """
        options = {
            "part": part,
            "forUsername": username
        }
        return self.__execute_call(self.youtube.channels().list(**options))
    #
    # def channel_videos_search(self,
    #                           channel_id,
    #                           part="id",
    #                           search_type="video",
    #                           max_results=50,
    #                           safe_search='none',
    #                           order="date",
    #                           page_token=None,
    #                           published_after=None):
    #     """
    #     Make video search by channel id
    #     """
    #     options = {
    #         'part': part,
    #         'maxResults': max_results,
    #         'channelId': channel_id,
    #         'type': search_type,
    #         'safeSearch': safe_search,
    #         'order': order
    #     }
    #     if page_token:
    #         options["pageToken"] = page_token
    #     if published_after:
    #         options["publishedAfter"] = '{:%Y-%m-%dT00:00:00Z}'.format(published_after)
    #     return self.make_call(self.youtube.search().list(**options))
    #

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
    #
    # def reports(self, *args, **kwargs):
    #     return self.make_call(self.youtube.reports().query(*args, **kwargs))
    #
    # def video_category_search(self, categories):
    #     options = {
    #         "part": "snippet",
    #         "id": categories
    #     }
    #     return self.make_call(self.youtube.videoCategories().list(**options))
    #
    # def get_comment_threads(self, video_id, limit=50):
    #     command = self.youtube.commentThreads().list(
    #         videoId=video_id,
    #         part='snippet',
    #         maxResults=limit,
    #     )
    #     res = None
    #     retries = 0
    #     while res is None:
    #         try:
    #             res = command.execute()
    #         except GoogleHttpError as e:
    #             logger.debug(e)
    #             res = {}
    #         except OSError as e:
    #             logger.error(e)
    #             if e.errno != 101 or retries > 1:
    #                 res = {}
    #             else:
    #                 # fix OSError: [Errno 101] Network is unreachable
    #                 time.sleep(self.RETRY_DELAY)
    #                 retries += 1
    #     return res
    #
    # @staticmethod
    # def get_caption_languages(video_id):
    #     default = None
    #     lang_ids = []
    #     try:
    #         response = urlopen(
    #             "http://video.google.com/timedtext?type=list&v={video_id}"
    #             .format(video_id=video_id)
    #         )
    #     except URLError as e:
    #         logger.debug(e)
    #     else:
    #         tree = ElementTree.parse(response)
    #         root = tree.getroot()
    #         for el in root:
    #             lang_code = el.attrib.get('lang_code')
    #             if "-" in lang_code:
    #                 lang_code = lang_code.split("-")[0]
    #             lang_ids.append(lang_code)
    #             if el.attrib.get('lang_default'):
    #                 default = lang_code
    #
    #     return default, lang_ids
    #
    # @staticmethod
    # def get_transcript(video_id, lang='en'):
    #     try:
    #         response = urlopen(
    #             "http://video.google.com/timedtext?lang={lang}&v={video_id}"
    #             .format(video_id=video_id, lang=lang)
    #         )
    #     except URLError as e:
    #         logger.debug(e)
    #     else:
    #         captions = response.read()
    #         return captions.decode('utf-8')
    #

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

    # def is_quota_exeeded(self, exception):
    #     quota_exceeded = False
    #     if exception.resp.status == HTTP_403_FORBIDDEN:
    #         try:
    #             error_info = json.loads(exception.content.decode())
    #             error_reasons = [exception['reason'] for exception in error_info['error']['errors']]
    #             quota_exceeded = set(['quotaExceeded', 'dailyLimitExceeded']) & set(error_reasons)
    #         except Exception:
    #             pass
    #     return quota_exceeded
