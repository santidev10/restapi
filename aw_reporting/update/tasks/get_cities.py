import logging
from datetime import timedelta, datetime

from django.db.models import Max

from aw_reporting.update.tasks.get_top_cities import get_top_cities
from aw_reporting.update.tasks.utils.constants import GET_DF
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats

logger = logging.getLogger(__name__)


def get_cities(client, account, today):
    from aw_reporting.models import CityStatistic, GeoTarget
    from aw_reporting.adwords_reports import geo_performance_report, \
        MAIN_STATISTICS_FILEDS

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = CityStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max("date")).get("max_date")

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        # getting top cities
        report = geo_performance_report(
            client, additional_fields=("Cost",))

        top_cities = get_top_cities(report)
        existed_top_cities = set(GeoTarget.objects.filter(
            id__in=top_cities
        ).values_list("id", flat=True))

        if len(top_cities) != len(existed_top_cities):
            logger.error(
                "Missed geo targets with ids "
                "%r" % (top_cities - existed_top_cities)
            )
            top_cities = existed_top_cities

        # map of latest dates for cities
        latest_dates = stats_queryset.filter(
            city_id__in=top_cities,
        ).values("city_id", "ad_group__campaign_id").order_by(
            "city_id", "ad_group__campaign_id"
        ).annotate(max_date=Max("date"))
        latest_dates = {
            (d["city_id"], d["ad_group__campaign_id"]): d["max_date"]
            for d in latest_dates
        }

        # recalculate min date
        #  check if we already have stats for every city
        if latest_dates and len(latest_dates) == len(top_cities):
            min_saved_date = min(latest_dates.values())

            # we don"t have to load stats earlier min_saved_date
            if min_saved_date > min_date:
                min_date = min_saved_date

            # all campaigns are finished,
            # and we have already saved all possible stats
            if min_saved_date >= max_date:
                return

        report = geo_performance_report(
            client, dates=(min_date, max_date),
            additional_fields=tuple(MAIN_STATISTICS_FILEDS) +
                              ("Date", "AdGroupId")
        )

        bulk_data = []
        for row_obj in filter(
                lambda i: i.CityCriteriaId.isnumeric()
                          and int(i.CityCriteriaId) in top_cities, report):

            city_id = int(row_obj.CityCriteriaId)
            date = latest_dates.get((city_id, row_obj.CampaignId))
            row_date = datetime.strptime(row_obj.Date, GET_DF).date()
            if date and row_date <= date:
                continue
            stats = {
                "city_id": city_id,
                "date": row_date,
                "ad_group_id": row_obj.AdGroupId,
            }
            stats.update(get_base_stats(row_obj))
            bulk_data.append(CityStatistic(**stats))
        if bulk_data:
            CityStatistic.objects.bulk_create(bulk_data)
