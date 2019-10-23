import logging
from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import ParentStatistic
from aw_reporting.models.ad_words.constants import Parent
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class ParentUpdater(UpdateMixin):
    RESOURCE_NAME = "parental_status_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.parental_status_enum = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.ad_group_ids = set()
        self.existing_statistics = ParentStatistic.objects.filter(ad_group__campaign__account=account)

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.parental_status_enum = client.get_type("ParentalStatusTypeEnum", version="v2").ParentalStatusType
        self.drop_latest_stats(self.existing_statistics, self.today)

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")
        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
            max_date = max_acc_date

            parent_performance = self._get_parent_performance(min_date, max_date)
            generator = self._instance_generator(parent_performance)
            ParentStatistic.objects.safe_bulk_create(generator)
        self.reset_denorm_flag(ad_group_ids=self.ad_group_ids)

    def _instance_generator(self, parent_metrics):
        for row in parent_metrics:
            ad_group_id = row.ad_group.id.value
            self.ad_group_ids.add(ad_group_id)
            statistics = {
                "parent_status_id": constants.PARENT_ENUM_TO_ID.get(row.ad_group_criterion.parental_status.type, Parent.PARENT),
                "date": row.segments.date.value,
                "ad_group_id": ad_group_id,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            yield ParentStatistic(**statistics)

    def _get_parent_performance(self, min_date, max_date):
        """
        Retrieve Parent status performance
        :return: Google ads parental_view resource search response
        """
        query_fields = self.format_query(constants.PARENT_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM parental_status_view WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        parent_performance = self.ga_service.search(self.account.id, query=query)
        return parent_performance

