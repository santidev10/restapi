from datetime import timedelta

from django.db.models import Max

from aw_reporting.models.ad_words.constants import get_device_id_by_name
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from aw_reporting.update.tasks.utils.quart_views import quart_views

from utils.utils import chunks_generator


def _generate_stat_instances(model, contains, report):
    for row_obj in report:
        display_name = row_obj.DisplayName

        if contains not in display_name:
            continue

        criteria = row_obj.Criteria.strip()

        # only youtube ids we need in criteria
        if "youtube.com/" in criteria:
            criteria = criteria.split("/")[-1]

        stats = {
            "yt_id": criteria,
            "date": row_obj.Date,
            "ad_group_id": row_obj.AdGroupId,
            "device_id": get_device_id_by_name(row_obj.Device),
            "video_views_25_quartile": quart_views(row_obj, 25),
            "video_views_50_quartile": quart_views(row_obj, 50),
            "video_views_75_quartile": quart_views(row_obj, 75),
            "video_views_100_quartile": quart_views(row_obj, 100),
        }
        stats.update(get_base_stats(row_obj))

        yield model(**stats)


def get_placements(client, account, today):
    from aw_reporting.models import YTChannelStatistic
    from aw_reporting.models import YTVideoStatistic
    from aw_reporting.adwords_reports import placement_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    channel_stats_queryset = YTChannelStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    video_stats_queryset = YTVideoStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(channel_stats_queryset, today)
    drop_latest_stats(video_stats_queryset, today)

    channel_saved_max_date = channel_stats_queryset.aggregate(
        max_date=Max("date"),
    ).get("max_date")
    video_saved_max_date = video_stats_queryset.aggregate(
        max_date=Max("date"),
    ).get("max_date")

    if channel_saved_max_date and video_saved_max_date:
        saved_max_date = max(channel_saved_max_date, video_saved_max_date)
    else:
        saved_max_date = channel_saved_max_date or video_saved_max_date

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        chunk_size = 10000
        # As Out-Of-Memory errors prevention we have to download this report twice
        # and save to the DB only the necessary records at each load.
        # Any accumulation of such a report in memory is unacceptable because it may cause Out-Of-Memory error.
        report = placement_performance_report(client, dates=(min_date, max_date))
        generator = _generate_stat_instances(YTChannelStatistic, "/channel/", report)
        for chunk in chunks_generator(generator, chunk_size):
            YTChannelStatistic.objects.safe_bulk_create(chunk)

        report = placement_performance_report(client, dates=(min_date, max_date))
        generator = _generate_stat_instances(YTVideoStatistic, "/video/", report)
        for chunk in chunks_generator(generator, chunk_size):
            YTVideoStatistic.objects.safe_bulk_create(chunk)
