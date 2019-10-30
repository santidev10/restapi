import logging
from datetime import timedelta

from django.db.models import Max

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models import ParentStatistic
from aw_reporting.models.ad_words.constants import Parent
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class ParentUpdater(UpdateMixin):
    RESOURCE_NAME = "parental_status_view"
    UPDATE_FIELDS = constants.BASE_STATISTIC_MODEL_UPDATE_FIELDS

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
        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        if max_acc_date is None:
            return

        saved_max_date = self.existing_statistics.aggregate(max_date=Max("date")).get("max_date")
        if saved_max_date is None or saved_max_date < max_acc_date:
            min_date = (saved_max_date if saved_max_date else min_acc_date) - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT)
            max_date = max_acc_date

            parent_performance = self._get_parent_performance(min_date, max_date)
            self._instance_generator(parent_performance, min_date)
        self.reset_denorm_flag(ad_group_ids=self.ad_group_ids)

    def _instance_generator(self, parent_metrics, min_date):
        existing_stats_from_min_date = {
            (s.parent_status_id, int(s.ad_group_id), str(s.date)): s.id for s
            in self.existing_statistics.filter(date__gte=min_date)
        }
        stats_to_create = []
        stats_to_update = []
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

            stat_obj = ParentStatistic(**statistics)
            stat_unique_constraint = (stat_obj.parent_status_id, stat_obj.ad_group_id, stat_obj.date)
            stat_id = existing_stats_from_min_date.get(stat_unique_constraint)
            if stat_id is None:
                stats_to_create.append(stat_obj)
            else:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
        ParentStatistic.objects.safe_bulk_create(stats_to_create)
        ParentStatistic.objects.bulk_update(stats_to_update, fields=self.UPDATE_FIELDS)

    def _get_parent_performance(self, min_date, max_date):
        """
        Retrieve Parent status performance
        :return: Google ads parental_view resource search response
        """
        query_fields = self.format_query(constants.PARENT_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM parental_status_view WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{max_date}'"
        parent_performance = self.ga_service.search(self.account.id, query=query)
        return parent_performance

