from datetime import datetime
from datetime import timedelta

from keywords.api.utils import get_keywords_aw_stats
from keywords.api.utils import get_keywords_aw_top_bottom_stats


def add_aw_stats(items):
    from aw_reporting.models import dict_norm_base_stats, dict_add_calculated_stats

    keywords = set(item.main.id for item in items)
    stats = get_keywords_aw_stats(keywords)
    top_bottom_stats = get_keywords_aw_top_bottom_stats(keywords)

    for item in items:
        item_stats = stats.get(item.main.id)
        if item_stats:
            dict_norm_base_stats(item_stats)
            dict_add_calculated_stats(item_stats)
            del item_stats['keyword']
            item.aw_stats = item_stats

            item_top_bottom_stats = top_bottom_stats.get(item.main.id, {})
            for key, value in item_top_bottom_stats.items():
                setattr(item.aw_stats, key, value)
    return items


def add_views_history_chart(keywords):
    for keyword in keywords:
        items = []
        items_count = 0
        today = datetime.now()
        if keyword.stats and keyword.stats.views_history:
            history = reversed(keyword.stats.views_history)
            for views in history:
                timestamp = today - timedelta(days=len(keyword.stats.views_history) - items_count - 1)
                timestamp = datetime.combine(timestamp, datetime.max.time())
                items_count += 1
                if views:
                    items.append(
                        {"created_at": timestamp.strftime('%Y-%m-%d'),
                         "views": views}
                    )
        keyword.views_history_chart = items
    return keywords
