import re
from datetime import datetime
from datetime import timedelta

from django.db.models import Max

from aw_reporting.models.ad_words.constants import BudgetType
from aw_reporting.models.ad_words.constants import get_device_id_by_name
from aw_reporting.update.tasks.utils.constants import GET_DF
from aw_reporting.update.tasks.utils.constants import MIN_FETCH_DATE
from aw_reporting.update.tasks.utils.cta import format_click_types_report
from aw_reporting.update.tasks.utils.cta import update_stats_with_click_type_data
from aw_reporting.update.tasks.utils.drop_latest_stats import drop_latest_stats
from aw_reporting.update.tasks.utils.get_base_stats import get_base_stats
from aw_reporting.update.tasks.utils.max_ready_date import max_ready_date
from aw_reporting.update.tasks.utils.quart_views import quart_views
from utils.datetime import now_in_default_tz


def get_campaigns(client, account, *_):
    from aw_reporting.adwords_reports import campaign_performance_report
    from aw_reporting.models import ACTION_STATUSES
    from aw_reporting.models import Campaign
    from aw_reporting.models import CampaignStatistic

    now = now_in_default_tz()
    today = now.date()
    max_date = max_ready_date(now, tz_str=account.timezone)

    stats_queryset = CampaignStatistic.objects.filter(
        campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    # lets find min and max dates for the report request
    dates = stats_queryset.aggregate(max_date=Max("date"))
    min_date = dates["max_date"] + timedelta(days=1) \
        if dates["max_date"] \
        else MIN_FETCH_DATE
    report = campaign_performance_report(
        client,
        dates=(min_date, max_date),
        include_zero_impressions=False,
        additional_fields=("Device", "Date"),
    )
    click_type_fields = (
        "CampaignId",
        "Date",
        "Clicks",
        "ClickType",
    )
    click_type_report = campaign_performance_report(
        client, dates=(min_date, max_date), fields=click_type_fields, include_zero_impressions=False)
    click_type_data = format_click_types_report(click_type_report, "CampaignId", "CampaignId")
    insert_stat = []
    for row_obj in report:
        campaign_id = row_obj.CampaignId
        try:
            end_date = datetime.strptime(row_obj.EndDate, GET_DF)
        except ValueError:
            end_date = None

        status = row_obj.CampaignStatus \
            if row_obj.CampaignStatus in ACTION_STATUSES \
            else row_obj.ServingStatus

        name = row_obj.CampaignName
        placement_code = extract_placement_code(name)
        budget_type = BudgetType.DAILY if row_obj.TotalAmount.strip() == "--" else BudgetType.TOTAL
        budget_str = row_obj.Amount if budget_type == BudgetType.DAILY else row_obj.TotalAmount
        budget = float(budget_str) / 1000000
        stats = {
            "de_norm_fields_are_recalculated": False,
            "name": name,
            "account": account,
            "type": row_obj.AdvertisingChannelType,
            "start_date": datetime.strptime(row_obj.StartDate, GET_DF),
            "end_date": end_date,
            "budget": budget,
            "budget_type": budget_type.value,
            "status": status,
            "placement_code": placement_code
        }

        statistic_data = {
            "date": row_obj.Date,
            "campaign_id": row_obj.CampaignId,
            "device_id": get_device_id_by_name(row_obj.Device),

            "video_views_25_quartile": quart_views(row_obj, 25),
            "video_views_50_quartile": quart_views(row_obj, 50),
            "video_views_75_quartile": quart_views(row_obj, 75),
            "video_views_100_quartile": quart_views(row_obj, 100),
        }
        statistic_data.update(get_base_stats(row_obj))
        update_stats_with_click_type_data(
            statistic_data, click_type_data, row_obj, unique_field_name="CampaignId", ref_id_name="CampaignId")

        insert_stat.append(CampaignStatistic(**statistic_data))

        try:
            campaign = Campaign.objects.get(pk=campaign_id)

            # Continue if the campaign's sync time is less than its update time, as it is pending to be synced with viewiq
            if campaign.sync_time < campaign.update_time:
                continue

            for field, value in stats.items():
                setattr(campaign, field, value)
            campaign.save()
        except Campaign.DoesNotExist:
            stats["id"] = campaign_id
            Campaign.objects.create(**stats)

    if insert_stat:
        CampaignStatistic.objects.safe_bulk_create(insert_stat)


def extract_placement_code(name):
    try:
        return re.search(r"(PL\d+)", name).group(1)
    except AttributeError:
        return None
