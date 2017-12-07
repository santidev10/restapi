"""
Channel api mixins module
"""
import re

from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_503_SERVICE_UNAVAILABLE

from utils.youtube_api import YoutubeAPIConnector, YoutubeAPIConnectorException


class ChannelYoutubeSearchMixin(object):
    """
    Mixin class for searching channels on youtube
    """
    def __initialize_youtube_connector(self):
        """
        Set up youtube_connector
        """
        self.youtube_connector = YoutubeAPIConnector()

    def __parse_link(self):
        """
        Get channel id or video channel id from sent link
        """
        link = self.request.query_params.get("youtube_link")
        if "user/" in link:
            user_id = link.split("user/")[1].split("/")[0]
            self.__initialize_youtube_connector()
            channels_ids = self.youtube_connector.obtain_user_channels(
                user_id).get("items", [])
            if not channels_ids:
                return
            return channels_ids[0].get("id")
        try:
            channel_id = re.findall(
                r"\w+?:?/?/?\w+?.?\w+.\w+/\w+/([0-9a-zA-Z_/-]+)/?", link)
        except TypeError:
            channel_id = None
        if channel_id:
            self.__initialize_youtube_connector()
            return channel_id[0]
        try:
            video_id = re.findall(
                r"\w+?:?/?/?\w+?.?\w+.\w+\w+/\w+\?v=([0-9a-zA-Z_/-]+)/?", link)
        except TypeError:
            video_id = None
        if not video_id:
            return
        return self.__get_video_youtube_channel_id(video_id[0])

    def __get_video_youtube_channel_id(self, video_id):
        """
        Get video channel youtube id procedure
        """
        self.__initialize_youtube_connector()
        try:
            details = self.youtube_connector.obtain_videos(
                videos_ids="{}".format(video_id),
                part="id,statistics,snippet").get("items")[0]
        except YoutubeAPIConnectorException:
            return None
        channel_youtube_id = details.get("snippet", {}).get("channelId")
        return channel_youtube_id

    def __get_channels_ids(self, channels):
        """
        Collect channels ids
        """
        ids = []
        channel_kind = "youtube#channel"
        for channel in channels:
            if channel.get("id", {}).get("kind") == channel_kind:
                ids.append(channel.get("id", {}).get("channelId"))
        return ids

    def __get_channels_details(self, channels_ids):
        """
        Collect channels statistic
        """
        ids_chunks = list(chunks(channels_ids, 50))
        channels_info = []
        for chunk in ids_chunks:
            channels_ids_string = ",".join(chunk)
            try:
                channels_data = self.youtube_connector.obtain_channels(
                    channels_ids_string,
                    part="id,snippet,statistics").get("items")
            except YoutubeAPIConnectorException:
                return channels_info
            else:
                channels_info = channels_info + channels_data
        return channels_info

    def __get_response_data(
            self, next_page_token=None, channels=None, channels_ids=None):
        """
        Prepare response json
        """
        response_data = {
            "next_page_token": None,
            "items": None,
            "items_count": 0
        }
        if channels_ids is None:
            if not channels:
                return response_data
            channels_ids = self.__get_channels_ids(channels)
        channels_details = self.__get_channels_details(channels_ids)
        if not channels_details:
            return response_data
        items = []
        for channel in channels_details:
            channel_snippet = channel.get("snippet", {})
            channel_statistics = channel.get("statistics", {})
            description = channel_snippet.get(
                "description", "No description available")
            youtube_id = channel.get("id")
            thumbnail_image_url = channel_snippet.get("thumbnails", {}).get(
                "default", {}).get("url")
            title = channel_snippet.get("title", "No title available")
            views = channel_statistics.get("viewCount")
            videos = channel_statistics.get("videoCount")
            subscribers = channel_statistics.get("subscriberCount")
            items.append({
                "id": youtube_id,
                "thumbnail_image_url": thumbnail_image_url,
                "title": title,
                "description": description,
                "videos": videos,
                "subscribers": subscribers,
                "views": views
            })
        response_data["items"] = items
        response_data["items_count"] = len(items)
        response_data["next_page_token"] = next_page_token
        return response_data

    def __keyword_search(self, keyword):
        """
        Search channels by keyword
        """
        next_page_token = self.request.query_params.get("next_page_token")
        self.__initialize_youtube_connector()
        try:
            channels_data = self.youtube_connector.keyword_search(
                keyword=keyword, part="id", page_token=next_page_token)
        except YoutubeAPIConnectorException:
            return Response(
                status=HTTP_503_SERVICE_UNAVAILABLE,
                data={"error": "Youtube API unreachable"})
        channels = channels_data.get("items")
        next_page_token = channels_data.get("nextPageToken")
        return Response(data=self.__get_response_data(
            next_page_token, channels=channels))

    def search_channels(self):
        """
        Search channels by keyword or link and return json response obj
        """
        link = self.request.query_params.get("youtube_link")
        keyword = self.request.query_params.get("youtube_keyword")
        if keyword:
            return self.__keyword_search(keyword)
        if link:
            channel_id = self.__parse_link()
            if not channel_id:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={"error": "not valid youtube link"}
                )
            return Response(
                data=self.__get_response_data(channels_ids=[channel_id]))
        return Response(
            status=HTTP_400_BAD_REQUEST,
            data={"error": "search params not specified"}
        )


class ChannelYoutubeStatisticsMixin(object):
    """
    Obtain channel stats from youtube
    """
    def parse_videos_info(self, videos_details):
        """
        Get parsed videos details
        """
        tags = []
        views = 0
        likes = 0
        dislikes = 0
        comments = 0
        for details in videos_details:
            video_tags = details.get("snippet", {}).get("tags", [])
            tags = tags + video_tags
            video_views = details.get("statistics", {}).get("viewCount", 0)
            views += int(video_views)
            video_likes = details.get("statistics", {}).get("likeCount", 0)
            likes += int(video_likes)
            video_dislikes = details.get(
                "statistics", {}).get("dislikeCount", 0)
            dislikes += int(video_dislikes)
            video_comments = details.get(
                "statistics", {}).get("commentCount", 0)
            comments += int(video_comments)
        return {
            "tags": tags,
            "views": views,
            "likes": likes,
            "dislikes": dislikes,
            "comments": comments,
            "videos": len(videos_details),
        }

    def parse_videos(self, videos_details):
        """
        Get parsed videos
        """
        videos = []
        for detail in videos_details:
            videos.append({
                "title": detail.get("snippet", {}).get("title"),
                "thumbnail_image_url": detail.get("snippet", {}).get(
                    "thumbnails", {}).get("default", {}).get("url"),
                "description": detail.get("snippet", {}).get("description"),
                "id": detail.get("id"),
                "youtube_published_at": detail.get(
                    "snippet", {}).get("publishedAt"),
                "tags": detail.get("snippet", {}).get("tags"),
                "youtube_link": "https://www.youtube.com/watch?v={}".format(
                    detail.get("id")),
                "views": detail.get("statistics", {}).get(
                    "viewCount", 0),
                "likes": detail.get("statistics", {}).get(
                    "likeCount", 0),
                "dislikes": detail.get("statistics", {}).get(
                    "dislikeCount", 0),
                "comments": detail.get("statistics", {}).get(
                    "commentCount", 0)
            })
        return videos

    def obtain_youtube_statistics(self):
        """
        Return extended channel statistics
        """
        channel_id = self.kwargs.get("pk")
        if not channel_id:
            return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data={"error": "no channel id were submitted"}
                )
        return self.__get_statistics()

    def __initialize_youtube_connector(self):
        """
        Set up youtube_connector
        """
        self.youtube_connector = YoutubeAPIConnector()

    def __get_statistics(self):
        """
        Prepare channel statistics
        """
        channel_id = self.kwargs.get("pk")
        self.__initialize_youtube_connector()
        try:
            channel_info = self.youtube_connector.obtain_channels(
                channels_ids=channel_id).get("items")[0]
        except YoutubeAPIConnectorException:
            return Response(
                status=HTTP_503_SERVICE_UNAVAILABLE,
                data={"error": "Youtube API unreachable"})
        snippet = channel_info.get("snippet", {})
        content_details = channel_info.get("contentDetails", {})
        statistics = channel_info.get("statistics")
        title = snippet.get("title", "No title available")
        youtube_published_at = snippet.get("publishedAt")
        thumbnail_image_url = snippet.get("thumbnails", {}).get(
            "default", {}).get("url")
        youtube_link = "https://www.youtube.com/channel/{}".format(channel_id)
        description = snippet.get("description", "No description available")
        content_owner = content_details.get("googlePlusUserId")
        subscribers = int(statistics.get("subscriberCount", 0))
        videos_count = int(statistics.get("videoCount", 0))
        country = snippet.get("country")
        response_data = {
            "title": title,
            "youtube_published_at": youtube_published_at,
            "thumbnail_image_url": thumbnail_image_url,
            "youtube_link": youtube_link,
            "id": channel_id,
            "country": country,
            "content_owner": content_owner,
            "description": description,
            "tags": None,
            "videos_count": videos_count,
            "videos": None,
            "subscribers": subscribers,
            "likes_per_video": None,
            "dislikes_per_video": None,
            "comments_per_video": None,
            "views_per_video": None,
        }
        if not videos_count:
            return Response(response_data)
        try:
            videos = self.youtube_connector.obtain_channel_videos(
                channel_id=channel_id)
        except YoutubeAPIConnectorException:
            return Response(
                status=HTTP_503_SERVICE_UNAVAILABLE,
                data={"error": "Youtube API unreachable"})
        videos_ids = [video.get("id", {}).get(
            "videoId") for video in videos.get("items")]
        try:
            videos_details = self.youtube_connector.obtain_videos(
                videos_ids=",".join(videos_ids),
                part="id,statistics,snippet").get("items")
        except YoutubeAPIConnectorException:
            return Response(
                status=HTTP_503_SERVICE_UNAVAILABLE,
                data={"error": "Youtube API unreachable"})
        parsed_videos_data = self.parse_videos_info(videos_details)
        videos = parsed_videos_data.get("videos")
        videos_views = parsed_videos_data.get("views")
        views_per_video = videos_views / max(videos, 1)
        response_data["views_per_video"] = views_per_video
        tags = parsed_videos_data.get("tags")
        response_data["tags"] = tags
        videos_likes = parsed_videos_data.get("likes")
        videos_dislikes = parsed_videos_data.get("dislikes")
        videos_comments = parsed_videos_data.get("comments")
        likes_per_video = videos_likes / max(videos, 1)
        response_data["likes_per_video"] = likes_per_video
        dislikes_per_video = videos_dislikes / max(videos, 1)
        response_data["dislikes_per_video"] = dislikes_per_video
        comments_per_video = videos_comments / max(videos, 1)
        response_data["comments_per_video"] = comments_per_video
        response_data["videos"] = self.parse_videos(videos_details)
        last_video_published_at = None
        if videos_details:
            last_video_published_at = videos_details[0].get(
                "snippet").get("publishedAt")
        response_data["last_video_published_at"] = last_video_published_at
        return Response(response_data)


def chunks(l, n):
    """
    Yield successive n-sized chunks from l
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]
