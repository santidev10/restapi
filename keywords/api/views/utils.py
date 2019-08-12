from datetime import datetime
from datetime import timedelta


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
