import itertools

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Audience
from aw_reporting.models import Topic
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.permissions import MediaBuyingAddOnPermission


class TargetingItemsSearchApiView(APIView):
    permission_classes = (MediaBuyingAddOnPermission,)

    def get(self, request, list_type, query, **_):
        method = "search_{}_items".format(list_type)
        items = getattr(self, method)(query)
        # items = [dict(criteria=uid) for uid in item_ids]
        # add_targeting_list_items_info(items, list_type)

        return Response(data=items)

    @staticmethod
    def search_video_items(query):
        manager = VideoManager(sections=(Sections.GENERAL_DATA,))
        videos = manager.search(
            limit=10,
            query={"match": {"general_data.title": query}},
            sort=[{"stats.views": {"order": "desc"}}]
        ).execute()
        items = [
            dict(id=video.main.id, criteria=video.main.id, name=video.general_data.title,
                 thumbnail=video.general_data.thumbnail_image_url)
            for video in videos
        ]
        return items

    @staticmethod
    def search_channel_items(query):
        manager = ChannelManager(sections=(Sections.GENERAL_DATA,))
        channels = manager.search(
            limit=10,
            query={"match": {f"{Sections.GENERAL_DATA}.title": query.lower()}},
            sort=[{f"{Sections.STATS}.subscribers": {"order": "desc"}}]
        ).execute()
        items = [
            dict(id=channel.main.id, criteria=channel.main.id, name=channel.general_data.title,
                 thumbnail=channel.general_data.thumbnail_image_url)
            for channel in channels
        ]
        return items

    @staticmethod
    def search_keyword_items(query):
        # pylint: disable=import-outside-toplevel
        from keyword_tool.models import KeyWord
        # pylint: enable=import-outside-toplevel
        keywords = KeyWord.objects.filter(
            text__icontains=query,
        ).exclude(text=query).values_list("text", flat=True).order_by("text")
        items = [
            dict(criteria=k, name=k)
            for k in itertools.chain((query,), keywords)
        ]
        return items

    @staticmethod
    def search_interest_items(query):
        audiences = Audience.objects.filter(
            name__icontains=query,
            type__in=[Audience.AFFINITY_TYPE, Audience.IN_MARKET_TYPE],
        ).values("name", "id").order_by("name", "id")

        items = [
            dict(criteria=a["id"], name=a["name"])
            for a in audiences
        ]
        return items

    @staticmethod
    def search_topic_items(query):
        topics = Topic.objects.filter(
            name__icontains=query,
        ).values("id", "name").order_by("name")
        items = [
            dict(criteria=k["id"], name=k["name"])
            for k in topics
        ]
        return items
