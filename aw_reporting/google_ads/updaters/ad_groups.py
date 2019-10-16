from datetime import timedelta
import logging

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.constants import DEVICE_ENUM_TO_ID
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models.ad_words.constants import Device
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class AdGroupUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group"

    def __init__(self, account, start_date=None, end_date=None):
        self.client = None
        self.ga_service = None
        self.account = account
        self.start_date = start_date
        self.end_date = end_date
        self.existing_statistics = AdGroupStatistic.objects.filter(ad_group__campaign__account=account)
        self.existing_ad_group_ids = set(AdGroup.objects.filter(campaign__account=account).values_list("id", flat=True))
        self.existing_campaign_ids = set(Campaign.objects.filter(account=account).values_list("id", flat=True))

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")

        now = now_in_default_tz()
        max_available_date = self.max_ready_date(now, tz_str=self.account.timezone)
        start_date = self.start_date
        end_date = self.end_date
        today = now.date()

        # If valid, start date and end date provided, drop stats in date range and update with new data
        if start_date and end_date and start_date < end_date:
            self.drop_custom_stats(self.existing_statistics, start_date, end_date)
            min_date = start_date
            max_date = end_date
        else:
            self.drop_latest_stats(self.existing_statistics, today)
            min_date, max_date = self.get_account_border_dates(self.account)
            # Update ad groups and daily stats only if there have been changes
            min_date, max_date = (max_date + timedelta(days=1), max_available_date) if max_date else (constants.MIN_FETCH_DATE, max_available_date)

        click_type_data = self.get_clicks_report(
            self.client, self.ga_service, self.account,
            min_date, max_date,
            resource_name=self.RESOURCE_NAME
        )
        ad_group_performance = self._get_ad_group_performance(min_date, max_date)
        generator = self._generate_instances(ad_group_performance, click_type_data)
        AdGroupStatistic.objects.safe_bulk_create(generator)

    def _get_ad_group_performance(self, min_date, max_date):
        """
        Retrieve ad_group performance
        :param min_date: str -> 2012-01-01
        :param max_date: str -> 2012-12-31
        :return: Google Ads search response
        """
        query_fields = self.format_query(constants.AD_GROUP_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        ad_group_performance = self.ga_service.search(self.account.id, query=query)
        return ad_group_performance

    def _generate_instances(self, ad_group_performance, click_type_data):
        """
        Method to create and update AdGroup and AdGroupStatistic objects
        :param ad_group_performance: Google ads ad_group resource search response
        :return:
        """
        ad_group_status_enum = self.client.get_type("AdGroupStatusEnum", version="v2").AdGroupStatus
        ad_group_type_enum = self.client.get_type("AdGroupTypeEnum", version="v2").AdGroupType
        updated_ad_groups = set()
        ad_groups_to_create = []
        for row in ad_group_performance:
            ad_group_id = str(row.ad_group.id.value)
            campaign_id = str(row.campaign.id.value)

            if campaign_id not in self.existing_campaign_ids:
                logger.warning(f"CID: {self.account.id} Campaign {campaign_id} is missed. Skipping AdGroup {ad_group_id}")
                continue
            if ad_group_id not in updated_ad_groups:
                updated_ad_groups.add(ad_group_id)
                ad_group_data = {
                    "de_norm_fields_are_recalculated": False,
                    "name": row.ad_group.name.value,
                    "status": ad_group_status_enum.Name(row.ad_group.status).lower(),
                    "type": ad_group_type_enum.Name(row.ad_group.type).lower(),
                    "campaign_id": campaign_id,
                    "cpv_bid": row.ad_group.cpv_bid_micros.value if row.ad_group.cpv_bid_micros else None,
                    "cpm_bid": row.ad_group.cpm_bid_micros.value if row.ad_group.cpm_bid_micros else None,
                    "cpc_bid": row.ad_group.cpc_bid_micros.value if row.ad_group.cpc_bid_micros else None,
                }
                # Check for AdGroup existence with set membership instead of making database queries for efficiency
                if ad_group_id in self.existing_ad_group_ids:
                    AdGroup.objects.filter(pk=ad_group_id).update(**ad_group_data)
                else:
                    self.existing_ad_group_ids.add(ad_group_id)
                    ad_group_data["id"] = ad_group_id
                    ad_groups_to_create.append(AdGroup(**ad_group_data))

            statistics = {
                "date": row.segments.date.value,
                "ad_network": row.segments.ad_network_type,
                "device_id": DEVICE_ENUM_TO_ID.get(row.segments.device, Device.COMPUTER),
                "ad_group_id": ad_group_id,
                "average_position": 0.0,
                "engagements": row.metrics.engagements.value if row.metrics.engagements else 0,
                "active_view_impressions": row.metrics.active_view_impressions.value if row.metrics.active_view_impressions else 0,
                **self.get_quartile_views(row)
            }
            statistics.update(self.get_base_stats(row))
            # Update statistics with click performance obtained in get_clicks_report
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME, ignore_a_few_records=True)
            statistics.update(click_data)
            yield AdGroupStatistic(**statistics)
        AdGroup.objects.bulk_create(ad_groups_to_create)
