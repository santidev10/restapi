from datetime import datetime
from datetime import timedelta


def get_views_keyword_history_chart(keyword):
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
    return items
