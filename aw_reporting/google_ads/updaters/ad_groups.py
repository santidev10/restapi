from datetime import timedelta
import logging

from aw_reporting.adwords_reports import ad_group_performance_report
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CriterionType
from aw_reporting.models.ad_words.constants import get_device_id_by_name
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data
from aw_reporting.update.adwords_utils import get_base_stats
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class AdGroupUpdater(UpdateMixin):
    RESOURCE_NAME = "ad_group"

    def __init__(self, account):
        self.account = account
        self.criterion_mapping = CriterionType.get_mapping_to_id()

    def update(self, client, start_date=None, end_date=None):
        click_type_report_fields = (
            "AdGroupId",
            "Date",
            "Device",
            "Clicks",
            "ClickType",
        )
        report_unique_field_name = "Device"
        now = now_in_default_tz()
        max_available_date = self.max_ready_date(now, tz_str=self.account.timezone)
        today = now.date()
        stats_queryset = AdGroupStatistic.objects.filter(
            ad_group__campaign__account=self.account
        )
        if start_date and end_date and start_date < end_date:
            self.drop_custom_stats(stats_queryset, start_date, end_date)
            dates = (start_date, end_date)

        else:
            self.drop_latest_stats(stats_queryset, today)
            min_date, max_date = self.get_account_border_dates(self.account)

            # we update ad groups and daily stats only if there have been changes
            dates = (max_date + timedelta(days=1), max_available_date) \
                if max_date \
                else (self.MIN_FETCH_DATE, max_available_date)

        report = ad_group_performance_report(
            client, dates=dates)
        if report:
            click_type_report = ad_group_performance_report(client, dates=dates, fields=click_type_report_fields)
            click_type_data = format_click_types_report(click_type_report, report_unique_field_name)

            ad_group_ids = list(AdGroup.objects.filter(
                campaign__account=self.account).values_list("id", flat=True))
            campaign_ids = list(Campaign.objects.filter(
                account=self.account).values_list("id", flat=True))

            create_ad_groups = []
            create_stats = []
            updated_ad_groups = []

            for row_obj in report:
                ad_group_id = int(row_obj.AdGroupId)
                campaign_id = int(row_obj.CampaignId)

                if campaign_id not in campaign_ids:
                    logger.warning("Campaign {campaign_id} is missed."
                                   " Skipping AdGroup {ad_group_id}"
                                   "".format(ad_group_id=ad_group_id,
                                             campaign_id=campaign_id)
                                   )
                    continue

                # update ad groups
                if ad_group_id not in updated_ad_groups:
                    updated_ad_groups.append(ad_group_id)

                    stats = {
                        "de_norm_fields_are_recalculated": False,
                        "name": row_obj.AdGroupName,
                        "status": row_obj.AdGroupStatus,
                        "type": row_obj.AdGroupType,
                        "campaign_id": campaign_id,
                        "criterion_type_id": self.criterion_mapping.get(row_obj.ContentBidCriterionTypeGroup),
                    }

                    if ad_group_id in ad_group_ids:
                        AdGroup.objects.filter(
                            pk=ad_group_id).update(**stats)
                    else:
                        ad_group_ids.append(ad_group_id)
                        stats["id"] = ad_group_id
                        create_ad_groups.append(AdGroup(**stats))
                # --update ad groups
                # insert stats
                stats = {
                    "date": row_obj.Date,
                    "ad_network": row_obj.AdNetworkType1,
                    "device_id": get_device_id_by_name(row_obj.Device),
                    "ad_group_id": ad_group_id,
                    "average_position": 0,
                    "engagements": row_obj.Engagements,
                    "active_view_impressions": row_obj.ActiveViewImpressions,
                }
                stats.update(get_base_stats(row_obj, quartiles=True))
                update_stats_with_click_type_data(
                    stats, click_type_data, row_obj, report_unique_field_name, ignore_a_few_records=True)
                create_stats.append(AdGroupStatistic(**stats))

            if create_ad_groups:
                AdGroup.objects.bulk_create(create_ad_groups)

            if create_stats:
                AdGroupStatistic.objects.safe_bulk_create(create_stats)
