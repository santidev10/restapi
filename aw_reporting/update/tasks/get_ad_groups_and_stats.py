import logging
from datetime import timedelta

from aw_reporting.models import Campaign
from aw_reporting.models.ad_words.constants import get_device_id_by_name
from aw_reporting.update.tasks.utils.constants import MIN_FETCH_DATE
from aw_reporting.update.tasks.utils.cta import format_click_types_report
from aw_reporting.update.tasks.utils.cta import update_stats_with_click_type_data
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_account_border_dates import get_account_border_dates
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from aw_reporting.update.tasks.utils.max_ready_date import max_ready_date
from aw_reporting.update.tasks.utils.quart_views import quart_views
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


def get_ad_groups_and_stats(client, account, *_):
    from aw_reporting.models import AdGroup
    from aw_reporting.models import AdGroupStatistic
    from aw_reporting.adwords_reports import ad_group_performance_report
    click_type_report_fields = (
        "AdGroupId",
        "Date",
        "Device",
        "Clicks",
        "ClickType",
    )
    report_unique_field_name = "Device"

    now = now_in_default_tz()
    max_available_date = max_ready_date(now, tz_str=account.timezone)
    today = now.date()
    stats_queryset = AdGroupStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)
    min_date, max_date = get_account_border_dates(account)

    # we update ad groups and daily stats only if there have been changes
    dates = (max_date + timedelta(days=1), max_available_date) \
        if max_date \
        else (MIN_FETCH_DATE, max_available_date)

    report = ad_group_performance_report(
        client, dates=dates)
    if report:
        click_type_report = ad_group_performance_report(client, dates=dates, fields=click_type_report_fields)
        click_type_data = format_click_types_report(click_type_report, report_unique_field_name)

        ad_group_ids = list(AdGroup.objects.filter(
            campaign__account=account).values_list("id", flat=True))
        campaign_ids = list(Campaign.objects.filter(
            account=account).values_list("id", flat=True))

        create_ad_groups = []
        create_stats = []
        updated_ad_groups = []

        for row_obj in report:
            ad_group_id = row_obj.AdGroupId
            campaign_id = row_obj.CampaignId

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
                "average_position": row_obj.AveragePosition,
                "engagements": row_obj.Engagements,
                "active_view_impressions": row_obj.ActiveViewImpressions,
                "video_views_25_quartile": quart_views(row_obj, 25),
                "video_views_50_quartile": quart_views(row_obj, 50),
                "video_views_75_quartile": quart_views(row_obj, 75),
                "video_views_100_quartile": quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, report_unique_field_name, ignore_a_few_records=True)
            create_stats.append(AdGroupStatistic(**stats))

        if create_ad_groups:
            AdGroup.objects.bulk_create(create_ad_groups)

        if create_stats:
            AdGroupStatistic.objects.safe_bulk_create(create_stats)
