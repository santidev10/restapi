from datetime import timedelta
import logging

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models.ad_words.constants import AgeRange
from aw_reporting.google_ads.constants import AGE_RANGE_ENUM_TO_ID
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class AgeRangeUpdater(UpdateMixin):
    RESOURCE_NAME = "age_range_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.age_range_enum = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = AgeRangeStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.age_range_enum = self.client.get_type("AgeRangeTypeEnum", version="v2").AgeRangeType

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return
        self.drop_latest_stats(self.existing_statistics, self.today)

        saved_max_date = self.existing_statistics.aggregate(
            max_date=Max("date")).get("max_date")
        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            click_type_data = self.get_clicks_report(
                self.client, self.ga_service, self.account,
                min_date, max_date,
                resource_name=self.RESOURCE_NAME
            )
            age_range_performance = self._get_age_range_performance(min_date, max_date)
            age_range_statistics_generator = self._instance_generator(age_range_performance, click_type_data)
            AgeRangeStatistic.objects.safe_bulk_create(age_range_statistics_generator)

    def _get_age_range_performance(self, min_date, max_date):
        """
        Retrieve age range performance
        :param min_date: str -> 2012-01-01
        :param max_date: str -> 2012-12-31
        :return: Google Ads search response
        """
        age_range_fields = {
            "ad_group_criterion": ("age_range.type",),
            **constants.DAILY_STATISTIC_PERFORMANCE_FIELDS
        }
        age_range_statistics_fields = self.format_query(age_range_fields)
        age_range_statistics_query = f"SELECT {age_range_statistics_fields} FROM age_range_view WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        age_range_performance = self.ga_service.search(self.account.id, query=age_range_statistics_query, page_size=10)
        return age_range_performance

    def _instance_generator(self, age_range_metrics, click_type_data):
        """
        Generator to yield AgeRangeStatistic instances
        :param age_range_metrics: iter -> Google ads ad_group resource search response
        :param click_type_data: dict -> Google ads click data by ad_group
        :return:
        """
        for row in age_range_metrics:
            ad_group_id = row.ad_group.id.value
            statistics = {
                "ad_group_id": ad_group_id,
                "age_range_id": AGE_RANGE_ENUM_TO_ID.get(row.ad_group_criterion.age_range.type, AgeRange.UNDETERMINED),
                "date": row.segments.date.value,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row,
                                                             resource_name=self.RESOURCE_NAME)
            statistics.update(click_data)
            yield AgeRangeStatistic(**statistics)
