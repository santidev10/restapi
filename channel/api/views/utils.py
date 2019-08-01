from datetime import datetime
from datetime import timedelta


from utils.api.research import ESQuerysetResearchAdapter


class ESQuerysetResearchChannelAdapter(ESQuerysetResearchAdapter):
    @staticmethod
    def add_chart_data(channel):
        """ Generate and add chart data for channel """
        if not channel.stats:
            return channel

        items = []
        items_count = 0
        history = zip(
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
        channel.chart_data = items
        return channel

    @property
    def add_extra_fields_func(self, start=0, end=None):
        return (self.add_chart_data,)
