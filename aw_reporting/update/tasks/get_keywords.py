from datetime import timedelta

from django.db.models import Max

from aw_reporting.update.tasks.utils.cta import DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS
from aw_reporting.update.tasks.utils.cta import DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME
from aw_reporting.update.tasks.utils.cta import format_click_types_report
from aw_reporting.update.tasks.utils.cta import update_stats_with_click_type_data
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from aw_reporting.update.tasks.utils.quart_views import quart_views


def _generate_stat_instances(model, report, click_type_data):
    for row_obj in report:
        keyword = row_obj.Criteria
        stats = {
            "keyword": keyword,
            "date": row_obj.Date,
            "ad_group_id": row_obj.AdGroupId,
            "video_views_25_quartile": quart_views(row_obj, 25),
            "video_views_50_quartile": quart_views(row_obj, 50),
            "video_views_75_quartile": quart_views(row_obj, 75),
            "video_views_100_quartile": quart_views(row_obj, 100),
        }
        stats.update(get_base_stats(row_obj))
        update_stats_with_click_type_data(
            stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        yield model(**stats)


def get_keywords(client, account, today, **kwargs):
    from aw_reporting.models import KeywordStatistic
    from aw_reporting.adwords_reports import keywords_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = KeywordStatistic.objects.filter(
        ad_group__campaign__account=account)
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max("date")).get("max_date")

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = keywords_performance_report(
            client,
            dates=(min_date, max_date),
        )
        click_type_report = keywords_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
        click_type_data = format_click_types_report(click_type_report,
                                                    DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        generator = _generate_stat_instances(KeywordStatistic, report, click_type_data)
        KeywordStatistic.objects.safe_bulk_create(generator)
