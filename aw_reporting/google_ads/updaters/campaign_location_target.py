from datetime import timedelta
from datetime import date

from django.conf import settings

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Campaign
from aw_reporting.models import GeoTargeting
from utils.datetime import now_in_default_tz


class CampaignLocationTargetUpdater(UpdateMixin):
    RESOURCE_NAME = "location_view"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.account = account
        self.today = now_in_default_tz().date()
        self.existing_campaign_ids = set(Campaign.objects.filter(account=self.account).values_list("id", flat=True))
        # List of two item tuples (campaign_id, geo_target_id) of existing GeoTargeting objects of current account being processed
        self.existing_targeting = set(GeoTargeting.objects.filter(campaign__account=self.account).values_list("campaign_id", "geo_target_id"))

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        min_acc_date, max_acc_date = self.get_account_border_dates(self.account)
        min_date = min_acc_date or settings.MIN_AW_FETCH_DATE
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        week_ago = yesterday - timedelta(days=7)

        if self.existing_targeting and (max_acc_date is None or max_acc_date < week_ago):
            # Don't update if there is no data or the data is old
            return
        campaign_loc_target_performance = self._get_campaign_location_target_performance(min_date)
        generator = self._instance_generator(campaign_loc_target_performance)
        GeoTargeting.objects.safe_bulk_create(generator)

    def _get_campaign_location_target_performance(self, min_date):
        """
        Retrieve campaign location targeting performance
        :return: Google Ads search response
        """
        query_fields = self.format_query(constants.CAMPAIGN_LOCATION_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE metrics.impressions > 0 AND segments.date BETWEEN '{min_date}' AND '{date.today()}'"
        campaign_loc_target_metrics = self.ga_service.search(self.account.id, query=query)
        return campaign_loc_target_metrics

    def _instance_generator(self, campaign_location_performance):
        """
        Generate or update GeoTargeting instances
        :param campaign_location_performance: Google ads search response
        :return: GeoTargeting instance
        """
        for row in campaign_location_performance:
            campaign_id = str(row.campaign.id.value)
            criterion_id = row.campaign_criterion.criterion_id.value
            criterion_negative = row.campaign_criterion.negative

            if len(str(criterion_id)) > 7:  # Skip custom locations
                continue
            if campaign_id not in self.existing_campaign_ids:
                continue
            # (str, int)
            uid = (campaign_id, criterion_id)
            # PRESENCE_OR_INTEREST or PRESENCE negative placement
            stats = dict(
                is_negative=criterion_negative == 2 or criterion_negative == 3,
                **self.get_base_stats(row)
            )
            if uid in self.existing_targeting:
                GeoTargeting.objects.filter(campaign_id=campaign_id, geo_target_id=criterion_id).update(**stats)
                continue
            else:
                yield GeoTargeting(campaign_id=campaign_id, geo_target_id=criterion_id, **stats)
