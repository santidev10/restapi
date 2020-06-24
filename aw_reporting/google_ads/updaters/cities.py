import heapq
import logging
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

from django.db.models import Max

from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS
from aw_reporting.adwords_reports import geo_performance_report
from aw_reporting.google_ads.constants import GET_DF
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import CityStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.update.adwords_utils import get_base_stats
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class CityUpdater(UpdateMixin):
    RESOURCE_NAME = "geographic_view"

    def __init__(self, account):
        self.account = account
        self.today = now_in_default_tz().date()

    def update(self, client):
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        stats_queryset = CityStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        self.drop_latest_stats(stats_queryset, self.today)

        saved_max_date = stats_queryset.aggregate(
            max_date=Max("date")).get("max_date")

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) \
                if saved_max_date else min_acc_date
            max_date = max_acc_date

            # getting top cities
            report = geo_performance_report(
                client, additional_fields=("Cost",))

            top_cities = self.get_top_cities(report)
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
                additional_fields=tuple(MAIN_STATISTICS_FILEDS) + ("Date", "AdGroupId"))
            generator = self._generate_stat_instances(CityStatistic, top_cities, report, latest_dates)
            CityStatistic.objects.safe_bulk_create(generator)

    def _generate_stat_instances(self, model, top_cities, report, latest_dates):
        for row_obj in filter(
            lambda i: i.CityCriteriaId.isnumeric() and int(i.CityCriteriaId) in top_cities, report):
            city_id = int(row_obj.CityCriteriaId)
            date = latest_dates.get((city_id, int(row_obj.CampaignId)))
            row_date = datetime.strptime(row_obj.Date, GET_DF).date()
            if date and row_date <= date:
                continue
            stats = {
                "city_id": city_id,
                "date": row_date,
                "ad_group_id": int(row_obj.AdGroupId),
            }
            stats.update(get_base_stats(row_obj))
            yield model(**stats)

    def get_top_cities(self, report):
        top_cities = []
        top_number = 10

        summary_cities_costs = defaultdict(int)
        report_by_campaign = defaultdict(list)
        for r in report:
            report_by_campaign[int(r.CampaignId)].append(r)
            summary_cities_costs[r.CityCriteriaId] += int(r.Cost)

        # top for every campaign
        for camp_rep in report_by_campaign.values():
            top = heapq.nlargest(
                top_number, camp_rep,
                lambda i: int(i.Cost) if i.CityCriteriaId.isnumeric() else 0
            )
            for s in top:
                top_cities.append(s.CityCriteriaId)

        # global top
        global_top = heapq.nlargest(
            top_number,
            summary_cities_costs.items(),
            lambda i: i[1] if i[0].isnumeric() else 0
        )
        for item in global_top:
            top_cities.append(item[0])
        return set(int(i) for i in top_cities if i.isnumeric())
