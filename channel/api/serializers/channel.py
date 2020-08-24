from rest_framework.fields import SerializerMethodField

from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin


class ChannelSerializer(VettedStatusSerializerMixin, ESDictSerializer):
    chart_data = SerializerMethodField()
    brand_safety_data = SerializerMethodField()
    vetted_status = SerializerMethodField()

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def get_chart_data(self, channel):
        return get_chart_data(channel)

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)


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
