"""
Channel api mixins module
"""
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from utils.youtube_api import YoutubeAPIConnector


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
        for channel in channels:
            if channel.get('id', {}).get('kind') == 'youtube#channel':
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
                channels_data = self.youtube_connector.channels_search(
                    channels_ids_string,
                    part="id,snippet,statistics").get("items")
            except Exception as e:
                logger.error(e)
            else:
                channels_info = channels_info + channels_data
        return channels_info

    def get_response_data(self, channels, full_info=False,
                          next_page_token=None):
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
        items = []
        if full_info:
            for channel in channels_details:
                if not self.channel_rate_view:
                    self.channel_rate_view = ChannelStatisticsApiView()
                channel = self.channel_rate_view.get_statistics(
                    str(channel.get("id"))).data
                items.append(channel)
        else:
            for channel in channels_details:
                channel_snippet = channel.get("snippet", {})
                channel_statistics = channel.get("statistics", {})
                description = channel_snippet.get(
                    'description', "No description available")
                youtube_id = channel.get("id")
                thumbnail_image_url = channel_snippet.get("thumbnails", {}).get(
                    "default", {}).get("url")
                title = channel_snippet.get("title", "No title available")
                views = channel_statistics.get("viewCount")
                videos = channel_statistics.get("videoCount")
                subscribers = channel_statistics.get("subscriberCount")
                country = channel_snippet.get("country")
                items.append({
                    "youtube_id": youtube_id,
                    "thumbnail_image_url": thumbnail_image_url,
                    "title": title,
                    "description": description,
                    "videos_count": videos,
                    "country": country,
                    "details": {
                        "subscribers": subscribers,
                        "videos_count": videos,
                        "views": views
                    }
                })
        response_data["items"] = items
        response_data["items_count"] = len(channels_details)
        if next_page_token:
            response_data["next_page_token"] = next_page_token
        return response_data

    def keyword_search(self, keyword):
        """
        Search channels by keyword
        """
        page_token = self.request.query_params.get("next_page")
        self.__initialize_youtube_connector()
        try:
            channels_data = self.youtube_connector.keywords_list_search(
                key_words=keywords, part='id', page_token=next_page_token)
        except QuotaExceededException:
            logger.error('Youtube API Quota Exceeded')
            return Response(status=HTTP_503_SERVICE_UNAVAILABLE, data={"error": ["Youtube Data API Quota Exceeded"]})
        except Exception as e:
            logger.error(e)
            return Response(status=HTTP_408_REQUEST_TIMEOUT)
        channels = channels_data.get("items")
        next_page_token = channels_data.get("nextPageToken")
        if full_info:
            return Response(data=self.get_response_data(
                channels, full_info=True, next_page_token=next_page_token))
        return Response(data=self.get_response_data(
            channels, next_page_token=next_page_token))

    def search_channels(self, request):
        """
        Search channels by keyword or link and return json response obj
        """
        link = request.query_params.get("youtube_link")
        keyword = request.query_params.get("youtube_keyword")
        if link:
            return self.keyword_search(keyword)
        if keyword:
            # TODO add search by keyword
            return
        return Response(
            status=HTTP_400_BAD_REQUEST,
            data={"error": "search params not specified"}
        )
