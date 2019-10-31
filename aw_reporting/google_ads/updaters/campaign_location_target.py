from datetime import timedelta
from datetime import date

from django.conf import settings

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.constants import BASE_STATISTIC_MODEL_UPDATE_FIELDS
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Campaign
from aw_reporting.models import GeoTargeting
from utils.datetime import now_in_default_tz


class CampaignLocationTargetUpdater(UpdateMixin):
    RESOURCE_NAME = "location_view"
    UPDATE_FIELDS = BASE_STATISTIC_MODEL_UPDATE_FIELDS

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_campaign_ids = set(Campaign.objects.filter(account=self.account).values_list("id", flat=True))
        self.existing_statistics = {
            (s.campaign_id, s.geo_target_id): s.id for s in
            GeoTargeting.objects.filter(campaign__account=self.account)
        }

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        min_date = min_acc_date or settings.MIN_AW_FETCH_DATE
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        week_ago = yesterday - timedelta(days=7)
        if self.existing_statistics and (max_acc_date is None or max_acc_date < week_ago):
            # Don't update if there is no data or the data is old
            return
        campaign_loc_target_performance = self._get_campaign_location_target_performance(min_date)
        self._create_instances(campaign_loc_target_performance)

    def _get_campaign_location_target_performance(self, min_date):
        """
        Retrieve campaign location targeting performance
        :return: Google Ads search response
        """
        query_fields = self.format_query(constants.CAMPAIGN_LOCATION_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{date.today()}'"
        campaign_loc_target_metrics = self.ga_service.search(self.account.id, query=query)
        return campaign_loc_target_metrics

    def _create_instances(self, campaign_location_performance):
        """
        Generate or update GeoTargeting instances
        :param campaign_location_performance: Google ads search response
        :return: GeoTargeting instance
        """
        stats_to_update = []
        stats_to_create = []
        for row in campaign_location_performance:
            campaign_id = str(row.campaign.id.value)
            criterion_id = row.campaign_criterion.criterion_id.value
            criterion_negative = row.campaign_criterion.negative

            if len(str(criterion_id)) > 7:  # Skip custom locations
                continue
            if campaign_id not in self.existing_campaign_ids:
                continue

            # PRESENCE_OR_INTEREST or PRESENCE negative placement
            statistics = {
                "is_negative": criterion_negative == 2 or criterion_negative == 3,
                "campaign_id": campaign_id,
                "geo_target_id": criterion_id,
                **self.get_base_stats(row)
            }

            stat_obj = GeoTargeting(**statistics)
            stat_unique_constraint = (stat_obj.campaign_id, stat_obj.geo_target_id)
            stat_id = self.existing_statistics.get(stat_unique_constraint)

            if stat_id is None:
                stats_to_create.append(stat_obj)
            else:
                stat_obj.id = stat_id
                stats_to_update.append(stat_obj)
        GeoTargeting.objects.safe_bulk_create(stats_to_create)
        GeoTargeting.objects.bulk_update(stats_to_create, fields=self.UPDATE_FIELDS)
