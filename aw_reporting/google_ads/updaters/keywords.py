import logging
from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import KeywordStatistic
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class KeywordUpdater(UpdateMixin):
    RESOURCE_NAME = "display_keyword_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = KeywordStatistic.objects.filter(ad_group__campaign__account=account)

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

            click_type_data = self.get_clicks_report(
                self.client, self.ga_service, self.account,
                min_date, max_date,
                resource_name=self.RESOURCE_NAME
            )
            keyword_performance = self._get_keyword_performance(min_date, max_date)
            generator = self._instance_generator(keyword_performance, click_type_data)
            KeywordStatistic.objects.safe_bulk_create(generator)

    def _get_keyword_performance(self, min_date, max_date):
        """
        Retrieve keyword performance
        :return: Google ads keyword_view resource search response
        """
        query_fields = self.format_query(constants.KEYWORD_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        keyword_performance = self.ga_service.search(self.account.id, query=query)
        return keyword_performance

    def _instance_generator(self, keyword_performance, click_type_data):
        for row in keyword_performance:
            keyword = row.ad_group_criterion.keyword.text.value
            statistics = {
                "keyword": keyword,
                "date": row.segments.date.value,
                "ad_group_id": row.ad_group.id.value,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name="ad_group")
            statistics.update(click_data)
            yield KeywordStatistic(**statistics)
