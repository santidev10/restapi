from rest_framework.fields import SerializerMethodField

from audit_tool.models import BlacklistItem
from es_components.constants import Sections
from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin
from utils.serializers.fields import ParentDictValueField


class ChannelSerializer(ESDictSerializer):
    chart_data = SerializerMethodField()
    brand_safety_data = SerializerMethodField()

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data.pop(Sections.TASK_US_DATA, None)
        return data

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def get_chart_data(self, channel):
        return get_chart_data(channel)

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)


# todo: duplicates VideoWithBlackListSerializer
class ChannelWithBlackListSerializer(ChannelSerializer):
    blacklist_data = ParentDictValueField("blacklist_data", source="main.id")

    def __init__(self, instance, *args, **kwargs):
        super(ChannelWithBlackListSerializer, self).__init__(instance, *args, **kwargs)
        self.blacklist_data = {}
        if instance:
            channels = instance if isinstance(instance, list) else [instance]
            self.blacklist_data = self.fetch_blacklist_items(channels)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def fetch_blacklist_items(self, channels):
        doc_ids = [doc.meta.id for doc in channels]
        blacklist_items = BlacklistItem.get(doc_ids, BlacklistItem.CHANNEL_ITEM)
        blacklist_items_by_id = {
            item.item_id: {
                "blacklist_data": item.to_dict()
            } for item in blacklist_items
        }
        return blacklist_items_by_id


class ChannelWithVettedStatusSerializer(VettedStatusSerializerMixin, ChannelSerializer):
    vetted_status = SerializerMethodField()


class ChannelAdminSerializer(VettedStatusSerializerMixin, ChannelWithBlackListSerializer):
    vetted_status = SerializerMethodField()


def get_chart_data(channel):
    if not hasattr(channel, "stats"):
        return None

    items = []
    subscribers_raw_history = channel.stats.subscribers_raw_history.to_dict()
    views_raw_history = channel.stats.views_raw_history.to_dict()
    history_dates = set(list(subscribers_raw_history.keys()) + list(views_raw_history.keys()))

    for history_date in sorted(list(history_dates)):
        items.append({
            "created_at": date_to_chart_data_str(history_date),
            "subscribers": subscribers_raw_history.get(history_date),
            "views": views_raw_history.get(history_date)
        })
    return items
