from datetime import timedelta

from django.db.models import Max

from aw_reporting.adwords_reports import parent_performance_report
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from aw_reporting.update.tasks.utils.quart_views import quart_views
from aw_reporting.update.tasks.utils.reset_denorm_flag import reset_denorm_flag


def get_parents(client, account, today):
    from aw_reporting.models import ParentStatistic
    from aw_reporting.models import ParentStatuses

    stats_queryset = ParentStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max("date"),
    ).get("max_date")

    ad_group_ids = set()

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(
            days=1) if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = parent_performance_report(
            client, dates=(min_date, max_date),
        )
        bulk_data = []
        for row_obj in report:
            ad_group_id = row_obj.AdGroupId
            stats = {
                "parent_status_id": ParentStatuses.index(row_obj.Criteria),
                "date": row_obj.Date,
                "ad_group_id": ad_group_id,

                "video_views_25_quartile": quart_views(row_obj, 25),
                "video_views_50_quartile": quart_views(row_obj, 50),
                "video_views_75_quartile": quart_views(row_obj, 75),
                "video_views_100_quartile": quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            bulk_data.append(ParentStatistic(**stats))

            ad_group_ids.add(row_obj.AdGroupId)

        if bulk_data:
            ParentStatistic.objects.safe_bulk_create(bulk_data)

    reset_denorm_flag(ad_group_ids=ad_group_ids)
