import logging

from django.db.models import Max

from aw_reporting.google_ads.constants import DAILY_STATISTIC_PERFORMANCE_FIELDS
from aw_reporting.google_ads.constants import GENDER_ENUM_TO_ID
from aw_reporting.google_ads.constants import STATS_MODELS_COMBINED_UPDATE_FIELDS
from aw_reporting.google_ads.utils import calculate_min_date_to_update
from aw_reporting.models import GenderStatistic
from aw_reporting.models.ad_words.constants import Gender
from aw_reporting.google_ads.update_mixin import UpdateMixin
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class GenderUpdater(UpdateMixin):
    RESOURCE_NAME = "gender_view"
    UPDATE_FIELDS = STATS_MODELS_COMBINED_UPDATE_FIELDS

    def __init__(self, account):
        """
        :param account: Google Ads Account
        """
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_statistics = GenderStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")
        if saved_max_date is None or saved_max_date <= max_acc_date:
            max_date = max_acc_date
            min_date = calculate_min_date_to_update(saved_max_date, self.today, limit=max_date) if saved_max_date else min_acc_date

            click_type_data = self.get_clicks_report(
                self.client, self.ga_service, self.account,
                min_date, max_date,
                resource_name=self.RESOURCE_NAME
            )
            gender_performance = self._get_gender_performance(min_date, max_date)
            self._create_instances(gender_performance, click_type_data, min_date)

    def _get_gender_performance(self, min_date, max_date):
        """
        Query Google ads for gender_view metrics
        :return: list -> Google ads ad_group resource search response
        """
        gender_fields = {
            "ad_group_criterion": ("gender.type",),
            **DAILY_STATISTIC_PERFORMANCE_FIELDS
        }
        formatted = self.format_query(gender_fields)
        gender_statistics_query = f"SELECT {formatted} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        gender_performance = self.ga_service.search(self.account.id, query=gender_statistics_query)
        return gender_performance

    def _create_instances(self, gender_performance, click_type_data, min_date):
        """
        Generator that yields GenderStatistic instances to be created
        :param gender_performance: iter -> Google Ads ad_group resource search response
        :return: GenderStatistic instance
        """
        existing_stats_from_min_date = {
            (s.gender_id, s.ad_group_id, str(s.date)): s.id for s
            in self.existing_statistics.filter(date__gte=min_date)
        }
        stats_to_create = []
        stats_to_update = []
        for row in gender_performance:
            ad_group_id = str(row.ad_group.id.value)
            statistics = {
                "gender_id": GENDER_ENUM_TO_ID.get(row.ad_group_criterion.gender.type, Gender.UNDETERMINED),
                "date": row.segments.date.value,
                "ad_group_id": ad_group_id,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME)
            statistics.update(click_data)

            stat_obj = GenderStatistic(**statistics)
            stat_unique_constraint = (stat_obj.gender_id, stat_obj.ad_group_id, stat_obj.date)
            stat_id = existing_stats_from_min_date.get(stat_unique_constraint)

            if stat_id is None:
                stats_to_create.append(stat_obj)
            else:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
        GenderStatistic.objects.safe_bulk_create(stats_to_create)
        GenderStatistic.objects.bulk_update(stats_to_update, fields=self.UPDATE_FIELDS)
