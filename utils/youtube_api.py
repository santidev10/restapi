"""
Youtube api connector module
"""
import isodate
import logging
import requests
import time
from typing import Dict
from typing import List

from apiclient import discovery

from django.conf import settings
from django.core.cache import cache
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

    def get_related_videos(self, video_id, page_token=None, max_results=50):
        """
        Get related videos
        """
        options = {
            'part': 'snippet',
            'type': 'video',
            'relatedToVideoId': video_id,
            "maxResults": max_results,
        }
        if page_token is not None:
            options['pageToken'] = page_token

        return self.youtube.search().list(**options).execute()

    def obtain_video_categories(self, region_code='US'):
        options = {
            'part': 'snippet',
            'regionCode': region_code
        }
        return self.__execute_call(self.youtube.videoCategories().list(**options))

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

    def get_video_comments(self, video_id: str, max_results=100, page_token=None):
        options = {
            'part': 'snippet',
            'maxResults': max_results,
            'videoId': video_id,
            'textFormat': 'plainText',
        }
        if page_token:
            options['pageToken'] = page_token

        return self.__execute_call(self.youtube.commentThreads().list(**options))

    def get_video_comment_replies(self, parent_id: str, max_results=100):
        options = {
            'part': 'snippet',
            'maxResults': max_results,
            'parentId': parent_id,
            'textFormat': 'plainText',
        }
        return self.__execute_call(self.youtube.comments().list(**options))

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


def resolve_videos_info(ids: List[int],
                        cache_timeout: int=86400,
                        request_timeout: float=2,
                        api_key: str=settings.YOUTUBE_API_ALTERNATIVE_DEVELOPER_KEY) -> Dict[str, Dict[str, str]]:
    """
    Non-guaranteed gathering of meta-data for a list of videos.
    Data gathering is immediately ignored in the event of any error or timeout.

    :param ids: List of video IDs
    :param cache_timeout:  timeout for storing data in the cache
    :param request_timeout: timeout for data request
    :param api_key: Youtube API Developer Key

    :return:
        {
            video_id: {
                "title": title,
                "thumbnail_image_url": thumbnail_image_url,
                "duration": duration,
            },
            video_id: {
                "title": title,
                "thumbnail_image_url": thumbnail_image_url,
                "duration": duration,
            },
            ...
        }
    """

    details = {}

    try:
        cache_key_template = "video_title_thumbnail_{}"
        api_url = "https://www.googleapis.com/youtube/v3/videos"

        unresolved_ids = []

        for video_id in ids:
            info = cache.get(cache_key_template.format(video_id))
            if info:
                details[video_id] = info
            else:
                unresolved_ids.append(video_id)

        if unresolved_ids:
            ids_string = ",".join(unresolved_ids)

            max_results = len(ids)

            params = dict(key=api_key,
                          id=ids_string,
                          part="snippet,contentDetails",
                          maxResults=max_results)

            response = requests.get(url=api_url,
                                    params=params,
                                    timeout=request_timeout)

            items = response.json().get("items", [])

            for item in items:
                video_id = item.get("id")
                snippet = item.get("snippet", {})

                title = snippet.get("title")

                thumbnails = snippet.get("thumbnails", {})
                thumbnail_image_url = thumbnails.get("default", {}).get("url")

                iso_duration = item.get("contentDetails", {}).get("duration")
                duration = isodate.parse_duration(iso_duration).total_seconds() if iso_duration else 0

                info = dict(
                    title=title,
                    thumbnail_image_url=thumbnail_image_url,
                    duration=duration,
                )

                cache.set(cache_key_template.format(video_id), info, cache_timeout)
                details[video_id] = info
    except Exception as e:
        logger.error(e)

    return details
