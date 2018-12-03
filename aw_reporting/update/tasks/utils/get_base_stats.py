from aw_reporting.update.tasks.utils.quart_views import quart_views


def get_base_stats(row, quartiles=False):
    stats = dict(
        impressions=int(row.Impressions),
        video_views=int(row.VideoViews),
        clicks=int(row.Clicks),
        cost=float(row.Cost) / 1000000,
        conversions=float(row.Conversions.replace(",", "")),
        all_conversions=float(row.AllConversions.replace(",", ""))
        if hasattr(row, "AllConversions") else 0,
        view_through=int(row.ViewThroughConversions),
    )
    if quartiles:
        stats.update(
            video_views_25_quartile=quart_views(row, 25),
            video_views_50_quartile=quart_views(row, 50),
            video_views_75_quartile=quart_views(row, 75),
            video_views_100_quartile=quart_views(row, 100),
        )
    return stats
