from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import heapq
import logging

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from utils.datetime import now_in_default_tz

from aw_reporting.models import CityStatistic, GeoTarget

logger = logging.getLogger(__name__)


class CityUpdater(UpdateMixin):
    RESOURCE_NAME = "geographic_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = CityStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        self.drop_latest_stats(self.existing_statistics, self.today)
        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")

        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            # Finally query and generate for city statistics and merge statistical data with city type data
            geo_location_cities_metrics = self._get_city_performance()

            top_cities = self._get_top_cities(geo_location_cities_metrics)
            existing_top_cities = set(GeoTarget.objects.filter(id__in=top_cities).values_list("id", flat=True))

            if len(top_cities) != len(existing_top_cities):
                logger.error(f"Missed geo targets with ids: {top_cities - existing_top_cities}")
                top_cities = existing_top_cities

            # Recheck min and max dates to check if we already have statistics for every city
            min_date, max_date, latest_dates = self._check_latest(top_cities, min_date, max_date)
            if not min_date:
                return
            cities_statistics = self._get_cities_statistics(min_date, max_date)
            statistics_generator = self._instance_generator(cities_statistics, top_cities, latest_dates)
            CityStatistic.objects.safe_bulk_create(statistics_generator)

    def _get_city_performance(self):
        """
        Query Google ads geographic_view resource for ad_group metrics by location
        :return: list -> Google ads ad_group search response
        """
        formatted = self.format_query(constants.CITY_PERFORMANCE_FIELDS)
        query = f"SELECT {formatted} FROM {self.RESOURCE_NAME}"
        city_performance = self.ga_service.search(self.account.id, query=query)
        return city_performance

    def _get_cities_statistics(self, min_date, max_date):
        """
        Query Google ads geographic_view resource for statistics
        :param min_date: str -> ISO 8601(YYYY-MM-DD) date
        :param max_date: str -> ISO 8601(YYYY-MM-DD) date
        :return: iter
        """
        formatted = self.format_query(constants.CITY_MAIN_METRICS_PERFORMANCE_FIELDS)
        query = f"SELECT {formatted} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        cities_statistics = self.ga_service.search(self.account.id, query=query)
        return cities_statistics

    def _check_latest(self, top_cities, min_date, max_date):
        """
        Check latest dates of top cities to check if we already have latest city statistics
        :param top_cities: set -> Google ads geo_target_city ids
        :param min_date: str
        :param max_date: str
        :return: str, str
        """
        # Map of latest dates for cities
        latest_dates = self.existing_statistics\
            .filter(city_id__in=top_cities)\
            .values("city_id", "ad_group__campaign_id")\
            .order_by("city_id", "ad_group__campaign_id")\
            .annotate(max_date=Max("date"))
        latest_dates = {
            (d["city_id"], d["ad_group__campaign_id"]): d["max_date"]
            for d in latest_dates
        }
        #  recalculate min date and check if we already have stats for every city
        if latest_dates and len(latest_dates) == len(top_cities):
            min_saved_date = min(latest_dates.values())

            # Don't have to load stats earlier min_saved_date
            if min_saved_date > min_date:
                min_date = min_saved_date
            # all campaigns are finished, and we have already saved all possible stats
            if min_saved_date >= max_date:
                min_date = max_date = None
        return min_date, max_date, latest_dates

    def _get_top_cities(self, cities_metrics):
        """
        Gets the top 10 performant cities to use for filtering of new statistics to create
        :param geo_location_performance: iter -> Google ads search response
        :return: set -> Google ads geo_target_city ids 
        """
        top_cities = []
        top_number = 10

        summary_cities_costs = defaultdict(int)
        by_campaign = defaultdict(list)
        for row in cities_metrics:
            by_campaign[row.campaign.id.value].append(row)
            summary_cities_costs[row.segments.geo_target_city.value] += int(row.metrics.cost_micros.value)

        # top for every campaign
        for campaign in by_campaign.values():
            top = heapq.nlargest(
                top_number, campaign,
                lambda r: int(r.metrics.cost_micros.value) if r.segments.geo_target_city.value.isnumeric() else 0
            )
            for _campaign in top:
                city_resource = _campaign.segments.geo_target_city.value
                city_id = self._extract_city_id(city_resource)
                top_cities.append(city_id)

        # global top
        global_top = heapq.nlargest(
            top_number,
            summary_cities_costs.items(),
            lambda r: r[1] if r[0].isnumeric() else 0
        )
        for item in global_top:
            resource_name = item[0]
            city_id = self._extract_city_id(resource_name)
            top_cities.append(city_id)
        return set(top_cities)

    def _instance_generator(self, cities_statistics, top_cities, latest_dates):
        """
        Generator to yield CityStatistic instances
        :param cities_statistics: iter -> Google ads search response
        :param top_cities: set ->
        :return:
        """
        for row in cities_statistics:
            ad_group_id = row.ad_group.id.value
            city_id = self._extract_city_id(row.segments.geo_target_city.value)
            if city_id not in top_cities:
                continue
            date = latest_dates.get((city_id, row.campaign.id.value))
            row_date = datetime.strptime(row.segments.date.value, "%Y-%m-%d").date()
            if date and row_date <= date:
                continue
            stats = {
                "city_id": int(city_id),
                "date": row.segments.date.value,
                "ad_group_id": ad_group_id
            }
            stats.update(self.get_base_stats(row))
            yield CityStatistic(**stats)

    def _extract_city_id(self, geo_target_city_resource):
        city_id = geo_target_city_resource.split("/")[-1]
        return city_id
