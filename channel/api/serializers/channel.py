from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

from rest_framework.fields import SerializerMethodField

from utils.brand_safety import get_brand_safety_data
from utils.es_components_api_utils import ESDictSerializer


class ChannelSerializer(ESDictSerializer):
    chart_data = SerializerMethodField()
    brand_safety_data = SerializerMethodField()

    def get_chart_data(self, channel):
        return get_chart_data(channel)

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)


def get_chart_data(channel):
    if not hasattr(channel, "stats"):
        return None

    items = []
    items_count = 0
    history = zip_longest(
        reversed(channel.stats.subscribers_history or []),
        reversed(channel.stats.views_history or [])
    )
    for subscribers, views in history:
        timestamp = channel.stats.historydate - timedelta(
            days=len(channel.stats.subscribers_history) - items_count - 1)
        timestamp = datetime.combine(timestamp, datetime.max.time())
        items_count += 1
        if any((subscribers, views)):
            items.append(
                {"created_at": str(timestamp) + "Z",
                 "subscribers": subscribers,
                 "views": views}
            )
    return items
