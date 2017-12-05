"""
Channel api mixins module
"""
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

    def get_channels_ids(self, channels):
        """
        Collect channels ids
        """
        ids = []
        channel_kind = "youtube#channel"
        for channel in channels:
            if channel.get("id", {}).get("kind") == channel_kind:
                ids.append(channel.get("id", {}).get("channelId"))
        return ids

    def get_channels_details(self, channels_ids):
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

    def get_response_data(self, channels, next_page_token):
        """
        Prepare response json
        """
        response_data = {
            "next_page_token": None,
            "items": None,
            "items_count": 0
        }
        if not channels:
            return response_data
        channels_ids = self.get_channels_ids(channels)
        channels_details = self.get_channels_details(channels_ids)
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

    def keyword_search(self, keyword):
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
        return Response(data=self.get_response_data(
            channels, next_page_token))

    def search_channels(self):
        """
        Search channels by keyword or link and return json response obj
        """
        link = self.request.query_params.get("youtube_link")
        keyword = self.request.query_params.get("youtube_keyword")
        if link:
            # TODO add functionality
            pass
        if keyword:
            return self.keyword_search(keyword)
        return Response(
            status=HTTP_400_BAD_REQUEST,
            data={"error": "search params not specified"}
        )


def chunks(l, n):
    """
    Yield successive n-sized chunks from l
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]
