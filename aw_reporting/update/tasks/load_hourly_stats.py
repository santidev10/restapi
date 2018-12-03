from datetime import datetime
from datetime import timedelta

import pytz
from django.db import transaction

from aw_reporting.update.tasks.utils.constants import GET_DF


def load_hourly_stats(client, account, *_):
    from aw_reporting.models import CampaignHourlyStatistic
    from aw_reporting.models import Campaign
    from aw_reporting.models import ACTION_STATUSES
    from aw_reporting.adwords_reports import campaign_performance_report
    from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS

    statistic_queryset = CampaignHourlyStatistic.objects.filter(
        campaign__account=account)

    today = datetime.now(tz=pytz.timezone(account.timezone)).date()
    min_date = today - timedelta(days=10)

    last_entry = statistic_queryset.filter(date__lt=min_date) \
        .order_by("-date").first()

    start_date = min_date
    if last_entry:
        start_date = last_entry.date

    statistic_to_drop = statistic_queryset.filter(date__gte=start_date)

    report = campaign_performance_report(
        client,
        dates=(start_date, today),
        fields=["CampaignId", "CampaignName", "StartDate", "EndDate",
                "AdvertisingChannelType", "Amount", "CampaignStatus",
                "ServingStatus", "Date", "HourOfDay"
                ] + list(MAIN_STATISTICS_FILEDS[:4]),
        include_zero_impressions=False)

    if not report:
        return

    campaign_ids = list(
        account.campaigns.values_list("id", flat=True)
    )
    create_campaign = []
    create_stat = []
    for row in report:
        campaign_id = row.CampaignId
        if campaign_id not in campaign_ids:
            campaign_ids.append(campaign_id)
            try:
                end_date = datetime.strptime(row.EndDate, GET_DF)
            except ValueError:
                end_date = None
            create_campaign.append(
                Campaign(
                    id=campaign_id,
                    name=row.CampaignName,
                    account=account,
                    type=row.AdvertisingChannelType,
                    start_date=datetime.strptime(row.StartDate, GET_DF),
                    end_date=end_date,
                    budget=float(row.Amount) / 1000000,
                    status=row.CampaignStatus if row.CampaignStatus in ACTION_STATUSES else row.ServingStatus,
                    impressions=1,
                    # to show this item on the accounts lists Track/Filters
                )
            )

        create_stat.append(
            CampaignHourlyStatistic(
                date=row.Date,
                hour=row.HourOfDay,
                campaign_id=row.CampaignId,
                video_views=row.VideoViews,
                impressions=row.Impressions,
                clicks=row.Clicks,
                cost=float(row.Cost) / 1000000,
            )
        )

    with transaction.atomic():
        if create_campaign:
            Campaign.objects.bulk_create(create_campaign)

        statistic_to_drop.delete()

        if create_stat:
            CampaignHourlyStatistic.objects.bulk_create(create_stat)
