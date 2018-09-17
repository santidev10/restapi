import csv
import heapq
import logging
import re
from collections import defaultdict
from collections import namedtuple
from datetime import date
from datetime import datetime
from datetime import timedelta
from functools import reduce

import pytz
from celery import task
from django.db import transaction
from django.db.models import Case
from django.db.models import Count
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.db.models import When
from pytz import timezone

from aw_reporting.adwords_api import get_all_customers
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.adwords_reports import AccountInactiveError
from aw_reporting.adwords_reports import parent_performance_report
from aw_reporting.models import ALL_AGE_RANGES
from aw_reporting.models import ALL_DEVICES
from aw_reporting.models import ALL_GENDERS
from aw_reporting.models import ALL_PARENTS
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models.ad_words.statistic import ModelDenormalizedFields
from utils.datetime import now_in_default_tz
from utils.lang import flatten

logger = logging.getLogger(__name__)

MIN_FETCH_DATE = date(2012, 1, 1)

TRACKING_CLICK_TYPES = (
    ("Website", "clicks_website"),
    ("Call-to-Action overlay", "clicks_call_to_action_overlay"),
    ("App store", "clicks_app_store"),
    ("Cards", "clicks_cards"),
    ("End cap", "clicks_end_cap")
)

DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME = "Criteria"

DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS = (
    "AdGroupId",
    "Date",
    "Criteria",
    "Clicks",
    "ClickType",
)


#  helpers --
def update_stats_with_click_type_data(
        stats, click_type_data, row_obj, report_unique_field_name, ignore_a_few_records=False):
    if click_type_data:
        key = prepare_click_type_report_key(
            row_obj.AdGroupId, getattr(row_obj, report_unique_field_name), row_obj.Date)
        if ignore_a_few_records:
            try:
                key_data = click_type_data.pop(key)
            except KeyError:
                return stats
        else:
            key_data = click_type_data.get(key)
        if key_data:
            for obj in key_data:
                stats[obj.get("click_type")] = obj.get("clicks")
    return stats


def prepare_click_type_report_key(ad_group_id, unique_field_name, row_date):
    return "{}{}{}".format(ad_group_id, unique_field_name, row_date)


def format_click_types_report(report, unique_field_name):
    """
    :param report: click types report
    :param unique_field_name: Device, Age, Gender, Location, etc.
    :return {"ad_group_id+unique_field+date": [Row(), Row() ...], ... }
    """
    if not report:
        return {}
    tracking_click_types = dict(TRACKING_CLICK_TYPES)
    report = [row for row in report if row.ClickType in tracking_click_types.keys()]
    result = dict()
    for row in report:
        key = prepare_click_type_report_key(row.AdGroupId, getattr(row, unique_field_name), row.Date)
        value = {"click_type": tracking_click_types.get(row.ClickType), "clicks": int(row.Clicks)}
        try:
            prev_value = result[key]
        except KeyError:
            result[key] = [value]
        else:
            prev_value.append(value)
            result[key] = prev_value
    return result


def quart_views(row, n):
    per = getattr(row, 'VideoQuartile%dRate' % n)
    impressions = int(row.Impressions)
    return float(per.rstrip('%')) / 100 * impressions


def get_base_stats(row, quartiles=False):
    stats = dict(
        impressions=int(row.Impressions),
        video_views=int(row.VideoViews),
        clicks=int(row.Clicks),
        cost=float(row.Cost) / 1000000,
        conversions=float(row.Conversions.replace(',', '')),
        all_conversions=float(row.AllConversions.replace(',', ''))
        if hasattr(row, "AllConversions") else 0,
        view_through=int(row.ViewThroughConversions),
    )
    if quartiles:
        stats.update(
            video_views_25_quartile=quart_views(row, 25),
            video_views_50_quartile=quart_views(row, 50),
            video_views_75_quartile=quart_views(row, 75),
            video_views_100_quartile=quart_views(row, 100),
        )
    return stats


MIN_UPDATE_HOUR = 6


def max_ready_datetime(dt: datetime):
    return dt - timedelta(hours=MIN_UPDATE_HOUR)


def max_ready_date(dt: datetime, tz=None, tz_str="UTC"):
    tz = tz or timezone(tz_str)
    return max_ready_datetime(dt).astimezone(tz).date() - timedelta(days=1)


def extract_placement_code(name):
    try:
        return re.search(r'(PL\d+)', name).group(1)
    except AttributeError:
        return None


AD_WORDS_STABILITY_STATS_DAYS_COUNT = 11


def date_to_refresh_statistic(today):
    return today - timedelta(AD_WORDS_STABILITY_STATS_DAYS_COUNT)


def drop_latest_stats(queryset, today):
    # delete stats for ten days
    date_delete = date_to_refresh_statistic(today)
    queryset.filter(date__gte=date_delete).delete()


def get_account_border_dates(account):
    from aw_reporting.models import AdGroupStatistic
    dates = AdGroupStatistic.objects.filter(
        ad_group__campaign__account=account
    ).aggregate(
        min_date=Min('date'),
        max_date=Max('date'),
    )
    return dates['min_date'], dates['max_date']


GET_DF = '%Y-%m-%d'


# -- helpers


def load_hourly_stats(client, account, *_):
    from aw_reporting.models import CampaignHourlyStatistic, Campaign, \
        ACTION_STATUSES
    from aw_reporting.adwords_reports import campaign_performance_report, \
        MAIN_STATISTICS_FILEDS

    statistic_queryset = CampaignHourlyStatistic.objects.filter(
        campaign__account=account)

    today = datetime.now(tz=pytz.timezone(account.timezone)).date()
    min_date = today - timedelta(days=10)

    last_entry = statistic_queryset.filter(date__lt=min_date) \
        .order_by('-date').first()

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
        account.campaigns.values_list('id', flat=True)
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


def is_ad_disapproved(campaign_row):
    return campaign_row.CombinedApprovalStatus == 'disapproved' \
        if hasattr(campaign_row, 'CombinedApprovalStatus') \
        else False


@task
def upload_initial_aw_data(connection_pk):
    from aw_reporting.models import AWConnection, Account
    from aw_reporting.aw_data_loader import AWDataLoader
    connection = AWConnection.objects.get(pk=connection_pk)

    updater = AWDataLoader(datetime.now(tz=pytz.utc).date())
    client = get_web_app_client(
        refresh_token=connection.refresh_token,
    )

    mcc_to_update = Account.objects.filter(
        mcc_permissions__aw_connection=connection,
        update_time__isnull=True,  # they were not updated before
    ).distinct()
    for mcc in mcc_to_update:
        client.SetClientCustomerId(mcc.id)
        updater.save_all_customers(client, mcc)

    accounts_to_update = Account.objects.filter(
        managers__mcc_permissions__aw_connection=connection,
        can_manage_clients=False,
        update_time__isnull=True,  # they were not updated before
    )
    for account in accounts_to_update:
        client.SetClientCustomerId(account.id)
        updater.advertising_account_update(client, account)
        # hourly stats
        load_hourly_stats(client, account)


def detect_success_aw_read_permissions():
    from aw_reporting.models import AWAccountPermission
    for permission in AWAccountPermission.objects.filter(
            can_read=False,
            account__is_active=True,
            aw_connection__revoked_access=False,
    ):
        try:
            client = get_web_app_client(
                refresh_token=permission.aw_connection.refresh_token,
                client_customer_id=permission.account_id,
            )
        except Exception as e:
            logger.info(e)
        else:
            try:
                get_all_customers(client, page_size=1, limit=1)
            except AccountInactiveError:
                account = permission.account
                account.is_active = False
                account.save()
            except Exception as e:
                logger.info(e)
            else:
                permission.can_read = True
                permission.save()


def get_campaigns(client, account, *_):
    from aw_reporting.adwords_reports import campaign_performance_report
    from aw_reporting.models import ACTION_STATUSES
    from aw_reporting.models import Campaign
    from aw_reporting.models import CampaignStatistic
    from aw_reporting.models import Devices

    min_fetch_date = datetime(2012, 1, 1).date()
    now = now_in_default_tz()
    today = now.date()

    stats_queryset = CampaignStatistic.objects.filter(
        campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    # lets find min and max dates for the report request
    dates = stats_queryset.aggregate(max_date=Max('date'))
    min_date = dates['max_date'] + timedelta(days=1) \
        if dates['max_date'] \
        else min_fetch_date
    max_date = max_ready_date(now, tz_str=account.timezone)

    report = campaign_performance_report(client,
                                         dates=(min_date, max_date),
                                         include_zero_impressions=False,
                                         additional_fields=('Device', 'Date')
                                         )
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
        stats = {
            'de_norm_fields_are_recalculated': False,
            'name': name,
            'account': account,
            'type': row_obj.AdvertisingChannelType,
            'start_date': datetime.strptime(row_obj.StartDate, GET_DF),
            'end_date': end_date,
            'budget': float(row_obj.Amount) / 1000000,
            'status': status,
            'placement_code': placement_code
        }

        statistic_data = {
            'date': row_obj.Date,
            'campaign_id': row_obj.CampaignId,
            'device_id': Devices.index(row_obj.Device),

            'video_views_25_quartile': quart_views(row_obj, 25),
            'video_views_50_quartile': quart_views(row_obj, 50),
            'video_views_75_quartile': quart_views(row_obj, 75),
            'video_views_100_quartile': quart_views(row_obj, 100),
        }
        statistic_data.update(get_base_stats(row_obj))

        insert_stat.append(CampaignStatistic(**statistic_data))

        try:
            campaign = Campaign.objects.get(pk=campaign_id)
            for field, value in stats.items():
                setattr(campaign, field, value)
            campaign.save()
        except Campaign.DoesNotExist:
            stats['id'] = campaign_id
            Campaign.objects.create(**stats)

    if insert_stat:
        CampaignStatistic.objects.safe_bulk_create(insert_stat)


def get_ad_groups_and_stats(client, account, *_):
    from aw_reporting.models import AdGroup, AdGroupStatistic, Devices, \
        SUM_STATS
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
    today = now.date()
    max_available_date = max_ready_date(now, tz_str=account.timezone)

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
            campaign__account=account).values_list('id', flat=True))
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
                    'de_norm_fields_are_recalculated': False,
                    'name': row_obj.AdGroupName,
                    'status': row_obj.AdGroupStatus,
                    'type': row_obj.AdGroupType,
                    'campaign_id': campaign_id,
                }

                if ad_group_id in ad_group_ids:
                    AdGroup.objects.filter(
                        pk=ad_group_id).update(**stats)
                else:
                    ad_group_ids.append(ad_group_id)
                    stats['id'] = ad_group_id
                    create_ad_groups.append(AdGroup(**stats))
            # --update ad groups
            # insert stats
            stats = {
                'date': row_obj.Date,
                'ad_network': row_obj.AdNetworkType1,
                'device_id': Devices.index(row_obj.Device),
                'ad_group_id': ad_group_id,
                'average_position': row_obj.AveragePosition,
                'engagements': row_obj.Engagements,
                'active_view_impressions': row_obj.ActiveViewImpressions,
                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, report_unique_field_name, ignore_a_few_records=True)
            create_stats.append(AdGroupStatistic(**stats))

        if create_ad_groups:
            AdGroup.objects.bulk_create(create_ad_groups)

        if create_stats:
            AdGroupStatistic.objects.safe_bulk_create(create_stats)

        SUM_STATS += (
            'engagements',
            'active_view_impressions',
            "clicks_website",
            "clicks_call_to_action_overlay",
            "clicks_app_store",
            "clicks_cards",
            "clicks_end_cap")
        stats = stats_queryset.values("ad_group_id").order_by(
            "ad_group_id").annotate(
            **{s: Sum(s) for s in SUM_STATS}
        )
        for ag_stats in stats:
            AdGroup.objects.filter(
                id=ag_stats['ad_group_id']
            ).update(**{s: ag_stats[s] for s in SUM_STATS})


def get_videos(client, account, today):
    from aw_reporting.models import VideoCreative, \
        VideoCreativeStatistic, AdGroup
    from aw_reporting.adwords_reports import video_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = VideoCreativeStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'))['max_date']

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        v_ids = list(
            VideoCreative.objects.all().values_list('id', flat=True)
        )
        ad_group_ids = set(
            AdGroup.objects.filter(
                campaign__account=account
            ).values_list('id', flat=True)
        )
        dates = (min_date, max_date)
        reports = video_performance_report(client, dates=dates)
        create = []
        create_creative = []
        for row_obj in reports:
            video_id = row_obj.VideoId.strip()
            if video_id not in v_ids:
                v_ids.append(video_id)
                create_creative.append(
                    VideoCreative(
                        id=video_id,
                        duration=row_obj.VideoDuration,
                    )
                )

            ad_group_id = row_obj.AdGroupId
            if ad_group_id not in ad_group_ids:
                continue

            stats = dict(
                creative_id=video_id,
                ad_group_id=ad_group_id,
                date=row_obj.Date,
                **get_base_stats(row_obj, quartiles=True)
            )
            create.append(
                VideoCreativeStatistic(**stats)
            )

        if create_creative:
            VideoCreative.objects.safe_bulk_create(create_creative)

        if create:
            VideoCreativeStatistic.objects.safe_bulk_create(create)


def get_ads(client, account, today):
    from aw_reporting.models import Ad, AdStatistic
    from aw_reporting.adwords_reports import ad_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return
    click_type_report_fields = (
        "AdGroupId",
        "Date",
        "Id",
        "Clicks",
        "ClickType",
    )
    report_unique_field_name = "Id"
    stats_queryset = AdStatistic.objects.filter(
        ad__ad_group__campaign__account=account)
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date')).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        ad_ids = list(
            Ad.objects.filter(
                ad_group__campaign__account=account
            ).values_list('id', flat=True)
        )

        report = ad_performance_report(
            client,
            dates=(min_date, max_date),
        )
        click_type_report = ad_performance_report(client, dates=(min_date, max_date), fields=click_type_report_fields)
        click_type_data = format_click_types_report(click_type_report, report_unique_field_name)
        create_ad = []
        create_stat = []
        updated_ads = []
        for row_obj in report:
            ad_id = row_obj.Id
            # update ads
            if ad_id not in updated_ads:
                updated_ads.append(ad_id)

                stats = {
                    'headline': row_obj.Headline,
                    'creative_name': row_obj.ImageCreativeName,
                    'display_url': row_obj.DisplayUrl,
                    'status': row_obj.Status,
                    'is_disapproved': is_ad_disapproved(row_obj)
                }
                kwargs = {
                    'id': ad_id, 'ad_group_id': row_obj.AdGroupId
                }

                if ad_id in ad_ids:
                    Ad.objects.filter(**kwargs).update(**stats)
                else:
                    ad_ids.append(ad_id)
                    stats.update(kwargs)
                    create_ad.append(Ad(**stats))
            # -- update ads
            # insert stats
            stats = {
                'date': row_obj.Date,
                'ad_id': ad_id,
                'average_position': row_obj.AveragePosition,
                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(
                get_base_stats(row_obj)
            )
            update_stats_with_click_type_data(stats, click_type_data, row_obj, report_unique_field_name)
            create_stat.append(AdStatistic(**stats))

        if create_ad:
            Ad.objects.safe_bulk_create(create_ad)

        if create_stat:
            AdStatistic.objects.safe_bulk_create(create_stat)


def get_genders(client, account, today):
    from aw_reporting.models import GenderStatistic, Genders
    from aw_reporting.adwords_reports import gender_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = GenderStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)
    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date')).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date
        report = gender_performance_report(
            client, dates=(min_date, max_date),
        )
        click_type_report = gender_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
        click_type_data = format_click_types_report(click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        bulk_data = []
        for row_obj in report:
            stats = {
                'gender_id': Genders.index(row_obj.Criteria),
                'date': row_obj.Date,
                'ad_group_id': row_obj.AdGroupId,

                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            bulk_data.append(GenderStatistic(**stats))

        if bulk_data:
            GenderStatistic.objects.safe_bulk_create(bulk_data)


def get_parents(client, account, today):
    from aw_reporting.models import ParentStatistic, ParentStatuses

    stats_queryset = ParentStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    ad_group_ids = set()

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(
            days=1) if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = parent_performance_report(
            client, dates=(min_date, max_date),
        )
        bulk_data = []
        for row_obj in report:
            ad_group_id = row_obj.AdGroupId
            stats = {
                'parent_status_id': ParentStatuses.index(row_obj.Criteria),
                'date': row_obj.Date,
                'ad_group_id': ad_group_id,

                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            bulk_data.append(ParentStatistic(**stats))

            ad_group_ids.add(row_obj.AdGroupId)

        if bulk_data:
            ParentStatistic.objects.safe_bulk_create(bulk_data)

    _reset_denorm_flag(ad_group_ids=ad_group_ids)


def get_age_ranges(client, account, today):
    from aw_reporting.models import AgeRangeStatistic, AgeRanges
    from aw_reporting.adwords_reports import age_range_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = AgeRangeStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date')).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = age_range_performance_report(
            client, dates=(min_date, max_date),
        )
        bulk_data = []
        click_type_report = age_range_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
        click_type_data = format_click_types_report(click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        for row_obj in report:
            stats = {
                'age_range_id': AgeRanges.index(row_obj.Criteria),
                'date': row_obj.Date,
                'ad_group_id': row_obj.AdGroupId,

                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            bulk_data.append(AgeRangeStatistic(**stats))

        if bulk_data:
            AgeRangeStatistic.objects.safe_bulk_create(bulk_data)


def get_placements(client, account, today):
    from aw_reporting.models import YTVideoStatistic, YTChannelStatistic, \
        Devices
    from aw_reporting.adwords_reports import placement_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    channel_stats_queryset = YTChannelStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    video_stats_queryset = YTVideoStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(channel_stats_queryset, today)
    drop_latest_stats(video_stats_queryset, today)

    channel_saved_max_date = channel_stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')
    video_saved_max_date = video_stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if channel_saved_max_date and video_saved_max_date:
        saved_max_date = max(channel_saved_max_date, video_saved_max_date)
    else:
        saved_max_date = channel_saved_max_date or video_saved_max_date

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = placement_performance_report(
            client, dates=(min_date, max_date),
        )
        bulk_channel_data = []
        bulk_video_data = []

        for row_obj in report:
            # only channels
            display_name = row_obj.DisplayName
            criteria = row_obj.Criteria.strip()

            if '/channel/' in display_name:
                stats = {
                    'yt_id': criteria,
                    'date': row_obj.Date,
                    'ad_group_id': row_obj.AdGroupId,
                    'device_id': Devices.index(row_obj.Device),
                    'video_views_25_quartile': quart_views(
                        row_obj, 25),
                    'video_views_50_quartile': quart_views(
                        row_obj, 50),
                    'video_views_75_quartile': quart_views(
                        row_obj, 75),
                    'video_views_100_quartile': quart_views(
                        row_obj, 100),
                }
                stats.update(get_base_stats(row_obj))
                bulk_channel_data.append(YTChannelStatistic(**stats))

            elif '/video/' in display_name:
                # only youtube ids we need in criteria
                if 'youtube.com/video/' in criteria:
                    criteria = criteria.split('/')[-1]

                stats = {
                    'yt_id': criteria,
                    'date': row_obj.Date,
                    'ad_group_id': row_obj.AdGroupId,
                    'device_id': Devices.index(row_obj.Device),
                    'video_views_25_quartile': quart_views(
                        row_obj, 25),
                    'video_views_50_quartile': quart_views(
                        row_obj, 50),
                    'video_views_75_quartile': quart_views(
                        row_obj, 75),
                    'video_views_100_quartile': quart_views(
                        row_obj, 100),
                }
                stats.update(get_base_stats(row_obj))
                bulk_video_data.append(YTVideoStatistic(**stats))

        if bulk_channel_data:
            YTChannelStatistic.objects.safe_bulk_create(
                bulk_channel_data)

        if bulk_video_data:
            YTVideoStatistic.objects.safe_bulk_create(
                bulk_video_data)


def get_keywords(client, account, today):
    from aw_reporting.models import KeywordStatistic
    from aw_reporting.adwords_reports import keywords_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = KeywordStatistic.objects.filter(
        ad_group__campaign__account=account)
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date')).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = keywords_performance_report(
            client,
            dates=(min_date, max_date),
        )
        click_type_report = keywords_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
        click_type_data = format_click_types_report(click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        bulk_data = []
        for row_obj in report:
            keyword = row_obj.Criteria
            stats = {
                'keyword': keyword,
                'date': row_obj.Date,
                'ad_group_id': row_obj.AdGroupId,

                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            bulk_data.append(KeywordStatistic(**stats))

        if bulk_data:
            KeywordStatistic.objects.safe_bulk_create(bulk_data)


def get_topics(client, account, today):
    from aw_reporting.models import Topic, TopicStatistic
    from aw_reporting.adwords_reports import topics_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = TopicStatistic.objects.filter(ad_group__campaign__account=account)

    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(max_date=Max('date')).get('max_date')

    topics = dict(Topic.objects.values_list('name', 'id'))

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = topics_performance_report(client, dates=(min_date, max_date),)

        click_type_report = topics_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)

        click_type_data = format_click_types_report(
            click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        bulk_data = []
        for row_obj in report:
            topic_name = row_obj.Criteria
            if topic_name not in topics:
                logger.warning("topic not found: {}".format(topic_name))
                continue
            stats = {
                'topic_id': topics[topic_name],
                'date': row_obj.Date,
                'ad_group_id': row_obj.AdGroupId,
                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            bulk_data.append(TopicStatistic(**stats))

        if bulk_data:
            TopicStatistic.objects.bulk_create(bulk_data)


class AudienceAWType:
    REMARK = "boomuserlist"
    USER_VERTICAL = "uservertical"
    CUSTOM_AFFINITY = "customaffinity"


def get_interests(client, account, today):
    from aw_reporting.models import AudienceStatistic, RemarkStatistic, \
        RemarkList, Audience
    from aw_reporting.adwords_reports import audience_performance_report

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    audience_stats_queryset = AudienceStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    remark_stats_queryset = RemarkStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(audience_stats_queryset, today)
    drop_latest_stats(remark_stats_queryset, today)

    aud_max_date = audience_stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')
    remark_max_date = remark_stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')
    if aud_max_date and remark_max_date:
        saved_max_date = max(aud_max_date, remark_max_date)
    else:
        saved_max_date = aud_max_date or remark_max_date

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        report = audience_performance_report(
            client, dates=(min_date, max_date))
        click_type_report = audience_performance_report(
            client, dates=(min_date, max_date), fields=DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS)
        click_type_data = format_click_types_report(click_type_report, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
        remark_ids = set(RemarkList.objects.values_list('id', flat=True))
        interest_ids = set(Audience.objects.values_list('id', flat=True))
        bulk_aud_stats = []
        bulk_remarks = []
        bulk_rem_stats = []
        bulk_custom_audiences = []
        for row_obj in report:
            stats = dict(
                date=row_obj.Date,
                ad_group_id=row_obj.AdGroupId,
                video_views_25_quartile=quart_views(row_obj, 25),
                video_views_50_quartile=quart_views(row_obj, 50),
                video_views_75_quartile=quart_views(row_obj, 75),
                video_views_100_quartile=quart_views(row_obj, 100),
                **get_base_stats(row_obj)
            )
            update_stats_with_click_type_data(
                stats, click_type_data, row_obj, DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME)
            au_type, au_id, *_ = row_obj.Criteria.split('::')
            if au_type == AudienceAWType.REMARK:
                stats.update(remark_id=au_id)
                bulk_rem_stats.append(RemarkStatistic(**stats))
                if au_id not in remark_ids:
                    remark_ids.update({au_id})
                    bulk_remarks.append(
                        RemarkList(id=au_id, name=row_obj.UserListName)
                    )

            elif au_type == AudienceAWType.USER_VERTICAL:
                if int(au_id) not in interest_ids:
                    logger.warning("Audience %s not found" % au_id)
                    continue

                stats.update(audience_id=au_id)
                bulk_aud_stats.append(AudienceStatistic(**stats))

            elif au_type == AudienceAWType.CUSTOM_AFFINITY:
                if int(au_id) not in interest_ids:
                    interest_ids |= {int(au_id)}
                    bulk_custom_audiences.append(Audience(
                        id=au_id, name=row_obj.Criteria,
                        type=Audience.CUSTOM_AFFINITY_TYPE
                    ))

                stats.update(audience_id=au_id)
                bulk_aud_stats.append(AudienceStatistic(**stats))
            else:
                logger.warning(
                    'Undefined criteria = %s' % row_obj.Criteria)

        if bulk_remarks:
            RemarkList.objects.safe_bulk_create(bulk_remarks)

        if bulk_rem_stats:
            RemarkStatistic.objects.safe_bulk_create(
                bulk_rem_stats)

        if bulk_custom_audiences:
            Audience.objects.safe_bulk_create(bulk_custom_audiences)

        if bulk_aud_stats:
            AudienceStatistic.objects.safe_bulk_create(
                bulk_aud_stats)


def get_top_cities(report):
    top_cities = []
    top_number = 10

    summary_cities_costs = defaultdict(int)
    report_by_campaign = defaultdict(list)
    for r in report:
        report_by_campaign[r.CampaignId].append(r)
        summary_cities_costs[r.CityCriteriaId] += int(r.Cost)

    # top for every campaign
    for camp_rep in report_by_campaign.values():
        top = heapq.nlargest(
            top_number, camp_rep,
            lambda i: int(i.Cost) if i.CityCriteriaId.isnumeric() else 0
        )
        for s in top:
            top_cities.append(s.CityCriteriaId)

    # global top
    global_top = heapq.nlargest(
        top_number,
        summary_cities_costs.items(),
        lambda i: i[1] if i[0].isnumeric() else 0
    )
    for item in global_top:
        top_cities.append(item[0])
    return set(int(i) for i in top_cities if i.isnumeric())


def get_cities(client, account, today):
    from aw_reporting.models import CityStatistic, GeoTarget
    from aw_reporting.adwords_reports import geo_performance_report, \
        MAIN_STATISTICS_FILEDS

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    stats_queryset = CityStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset, today)

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date')).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        # getting top cities
        report = geo_performance_report(
            client, additional_fields=('Cost',))

        top_cities = get_top_cities(report)
        existed_top_cities = set(GeoTarget.objects.filter(
            id__in=top_cities
        ).values_list('id', flat=True))

        if len(top_cities) != len(existed_top_cities):
            logger.error(
                "Missed geo targets with ids "
                "%r" % (top_cities - existed_top_cities)
            )
            top_cities = existed_top_cities

        # map of latest dates for cities
        latest_dates = stats_queryset.filter(
            city_id__in=top_cities,
        ).values('city_id', 'ad_group__campaign_id').order_by(
            'city_id', 'ad_group__campaign_id'
        ).annotate(max_date=Max('date'))
        latest_dates = {
            (d['city_id'], d['ad_group__campaign_id']): d['max_date']
            for d in latest_dates
        }

        # recalculate min date
        #  check if we already have stats for every city
        if latest_dates and len(latest_dates) == len(top_cities):
            min_saved_date = min(latest_dates.values())

            # we don't have to load stats earlier min_saved_date
            if min_saved_date > min_date:
                min_date = min_saved_date

            # all campaigns are finished,
            # and we have already saved all possible stats
            if min_saved_date >= max_date:
                return

        report = geo_performance_report(
            client, dates=(min_date, max_date),
            additional_fields=tuple(MAIN_STATISTICS_FILEDS) +
                              ('Date', 'AdGroupId')
        )

        bulk_data = []
        for row_obj in filter(
                lambda i: i.CityCriteriaId.isnumeric()
                          and int(i.CityCriteriaId) in top_cities, report):

            city_id = int(row_obj.CityCriteriaId)
            date = latest_dates.get((city_id, row_obj.CampaignId))
            row_date = datetime.strptime(row_obj.Date, GET_DF).date()
            if date and row_date <= date:
                continue
            stats = {
                'city_id': city_id,
                'date': row_date,
                'ad_group_id': row_obj.AdGroupId,
            }
            stats.update(get_base_stats(row_obj))
            bulk_data.append(CityStatistic(**stats))
        if bulk_data:
            CityStatistic.objects.bulk_create(bulk_data)


def get_geo_targeting(ad_client, account, *_):
    from aw_reporting.models import GeoTargeting, Campaign
    from aw_reporting.adwords_reports import geo_location_report

    saved_targeting = set(
        GeoTargeting.objects.filter(campaign__account=account) \
            .values_list("campaign_id", "geo_target_id")
    )

    _, max_acc_date = get_account_border_dates(account)
    yesterday = now_in_default_tz().date() - timedelta(days=1)
    week_ago = yesterday - timedelta(days=7)
    if saved_targeting and (max_acc_date is None or max_acc_date < week_ago):
        # don't update if there is no data or the data is old, just optimization
        return

    campaign_ids = set(Campaign.objects.filter(account=account) \
                       .values_list("id", flat=True))

    report = geo_location_report(ad_client)

    bulk_create = []
    for row_obj in report:
        if row_obj.CampaignId not in campaign_ids \
                or not row_obj.Id.isnumeric():
            continue
        uid = (row_obj.CampaignId, int(row_obj.Id))
        stats = dict(
            is_negative=row_obj.IsNegative == "true",
            **get_base_stats(row_obj)
        )
        if len(row_obj.Id) > 7:  # this is a custom location
            continue

        if uid in saved_targeting:
            GeoTargeting.objects.filter(campaign_id=row_obj.CampaignId,
                                        geo_target_id=row_obj.Id) \
                .update(**stats)
        else:
            bulk_create.append(
                GeoTargeting(campaign_id=row_obj.CampaignId,
                             geo_target_id=row_obj.Id, **stats))

    if bulk_create:
        GeoTargeting.objects.safe_bulk_create(bulk_create)


##
# statistics
##


def categories_define_parents():
    from aw_reporting.models import Audience
    offset = 0
    limit = 100
    while True:
        audiences = Audience.objects.filter(
            parent__isnull=True).order_by('id')[offset:offset + limit]
        if not audiences:
            break
        for audience in audiences:
            if audience.name.count('/') > 1:
                parent_name = "/".join(
                    audience.name.split('/')[:audience.name.count('/')]
                )
                parent = Audience.objects.get(name=parent_name)
                audience.parent = parent
                audience.save()
                offset -= 1

        offset += limit


# other


def load_google_categories(skip_audiences=False, skip_topics=False):
    from aw_reporting.models import Audience, Topic

    if not Audience.objects.count() and not skip_audiences:

        logger.info('Loading audiences...')
        files = (
            ('affinity_categories.csv', ("ID", "Category")),
            ('in-market_categories.csv', ("ID", "Category")),
            ('verticals.csv', ("Category", "ID", "ParentID")),
        )
        bulk_data = []

        for f_name, fields in files:

            list_type = f_name.split('_')[0]
            with open('aw_campaign/fixtures/google/%s' % f_name) as f:
                content = f.read()

            reader = csv.reader(content.split('\n')[1:], delimiter=',')
            row = namedtuple('Row', fields)
            for row_data in reader:
                if not row_data:
                    continue
                r = row(*row_data)
                bulk_data.append(
                    Audience(
                        id=r.ID,
                        name=r.Category,
                        parent_id=r.ParentID if 'ParentID' in fields and
                                                r.ParentID != '0' else None,
                        type=list_type,
                    )
                )
        Audience.objects.bulk_create(bulk_data)
        categories_define_parents()

    if not Topic.objects.count() and not skip_topics:
        logger.info('Loading topics...')
        bulk_data = []
        # topics
        fields = ("Category", "ID", "ParentID")
        with open('aw_campaign/fixtures/google/verticals.csv') as f:
            content = f.read()
            reader = csv.reader(content.split('\n')[1:], delimiter=',')
            row = namedtuple('Row', fields)
            for row_data in reader:
                if not row_data:
                    continue
                r = row(*row_data)
                bulk_data.append(
                    Topic(
                        id=r.ID,
                        name=r.Category,
                        parent_id=r.ParentID if r.ParentID != '0' else None
                    )
                )
        Topic.objects.bulk_create(bulk_data)


def load_google_geo_targets():
    from aw_reporting.models import GeoTarget

    ids = set(GeoTarget.objects.values_list('id', flat=True))
    logger.info('Loading google geo targets...')
    bulk_data = []
    with open('aw_campaign/fixtures/google/geo_locations.csv') as f:
        reader = csv.reader(f, delimiter=',')
        fields = next(reader)
        row = namedtuple('Row', [f.replace(" ", "") for f in fields])
        for row_data in reader:
            if not row_data:
                continue

            r = row(*row_data)
            if int(r.CriteriaID) not in ids:
                bulk_data.append(
                    GeoTarget(
                        id=r.CriteriaID,
                        name=r.Name,
                        canonical_name=r.CanonicalName,
                        parent_id=r.ParentID
                        if r.ParentID != '0' else None,
                        country_code=r.CountryCode,
                        target_type=r.TargetType,
                        status=r.Status,
                    )
                )
    if bulk_data:
        logger.info('Saving %d new geo targets...' % len(bulk_data))
        GeoTarget.objects.bulk_create(bulk_data)


def recalculate_de_norm_fields(*args, **kwargs):
    from aw_reporting.models import Campaign, AdGroup
    from math import ceil
    from django.conf import settings

    batch_size = 100

    for model in (Campaign, AdGroup):
        queryset = model.objects.filter(
            de_norm_fields_are_recalculated=False).order_by("id")
        iterations = ceil(queryset.count() / batch_size)
        if not settings.IS_TEST:
            model = queryset.model
            logger.info(
                "Calculating de-norm fields: {}.{} {}".format(model.__module__,
                                                              model.__name__,
                                                              iterations))

        ag_link = "ad_groups__" if model is Campaign else ""
        for i in range(iterations):
            if not settings.IS_TEST:
                logger.info("./Iteration: {}".format(i + 1))
            queryset = queryset[:batch_size]
            items = queryset.values("id")

            data = items.annotate(
                min_date=Min("statistics__date"),
                max_date=Max("statistics__date"),
            )
            sum_statistic = items.annotate(
                sum_cost=Sum("statistics__cost"),
                sum_impressions=Sum("statistics__impressions"),
                sum_video_views=Sum("statistics__video_views"),
                sum_clicks=Sum("statistics__clicks"),
            )
            sum_statistic_map = {s["id"]: s for s in sum_statistic}

            device_data = items.annotate(**_device_annotation())
            gender_data = items.annotate(**_gender_annotation(ag_link))
            age_data = items.annotate(**_age_annotation(ag_link))
            parent_data = items.annotate(**_parent_annotation(ag_link))

            def update_key(aggregator, item, key):
                current_value = aggregator[key]
                current_value.update(item)
                return aggregator

            stats_by_id = reduce(
                lambda res, i: update_key(res, i, i["id"]),
                flatten([device_data, gender_data, age_data, parent_data]),
                defaultdict(dict)
            )

            # Targeting data
            audience_data = items.annotate(
                count=Count("{}audiences__audience_id".format(ag_link)),
            )
            audience_data = {e["id"]: e["count"] for e in audience_data}

            keyword_data = items.annotate(
                count=Count("{}keywords__keyword".format(ag_link)),
            )
            keyword_data = {e["id"]: e["count"] for e in keyword_data}

            channel_data = items.annotate(
                count=Count("{}channel_statistics__id".format(ag_link)),
            )
            channel_data = {e["id"]: e["count"] for e in channel_data}

            video_data = items.annotate(
                count=Count("{}managed_video_statistics__id".format(ag_link)),
            )
            video_data = {e["id"]: e["count"] for e in video_data}

            rem_data = items.annotate(
                count=Count("{}remark_statistic__remark_id".format(ag_link)),
            )
            rem_data = {e["id"]: e["count"] for e in rem_data}

            topic_data = items.annotate(
                count=Count("{}topics__topic_id".format(ag_link)),
            )
            topic_data = {e["id"]: e["count"] for e in topic_data}

            update = {}
            for i in data:
                uid = i["id"]
                sum_stats = sum_statistic_map.get(uid, {})
                stats = stats_by_id[uid]
                update[uid] = dict(
                    de_norm_fields_are_recalculated=True,

                    min_stat_date=i["min_date"],
                    max_stat_date=i["max_date"],

                    cost=sum_stats.get("sum_cost") or 0,
                    impressions=sum_stats.get("sum_impressions") or 0,
                    video_views=sum_stats.get("sum_video_views") or 0,
                    clicks=sum_stats.get("sum_clicks") or 0,

                    **stats,

                    has_interests=bool(audience_data.get(uid)),
                    has_keywords=bool(keyword_data.get(uid)),
                    has_channels=bool(channel_data.get(uid)),
                    has_videos=bool(video_data.get(uid)),
                    has_remarketing=bool(rem_data.get(uid)),
                    has_topics=bool(topic_data.get(uid)),
                )

            for uid, updates in update.items():
                model.objects.filter(id=uid).update(**updates)


def _reset_denorm_flag(ad_group_ids=None, campaign_ids=None):
    if ad_group_ids:
        AdGroup.objects.filter(id__in=ad_group_ids) \
            .update(de_norm_fields_are_recalculated=False)
    if campaign_ids is None:
        campaign_ids = AdGroup.objects.filter(id__in=ad_group_ids) \
            .values_list("campaign_id", flat=True).distinct()
    Campaign.objects.filter(id__in=campaign_ids) \
        .update(de_norm_fields_are_recalculated=False)


def _build_boolean_case(ref, value):
    when = When(**{ref: value},
                then=1)
    return Max(Case(when,
                    default=0,
                    output_field=IntegerField()))


def _build_group_aggregation_map(ref, all_values, fields_map):
    return {
        fields_map[value]: _build_boolean_case(ref, value)
        for value in all_values
    }


def _age_annotation(ad_group_link):
    age_ref = "{}age_statistics__age_range_id".format(ad_group_link)

    return _build_group_aggregation_map(age_ref, ALL_AGE_RANGES, ModelDenormalizedFields.AGES)


def _gender_annotation(ad_group_link):
    gender_ref = "{}gender_statistics__gender_id".format(ad_group_link)

    return _build_group_aggregation_map(gender_ref, ALL_GENDERS, ModelDenormalizedFields.GENDERS)


def _parent_annotation(ad_group_link):
    parent_ref = "{}parent_statistics__parent_status_id".format(ad_group_link)

    return _build_group_aggregation_map(parent_ref, ALL_PARENTS, ModelDenormalizedFields.PARENTS)


def _device_annotation():
    device_ref = "statistics__device_id"

    return _build_group_aggregation_map(device_ref, ALL_DEVICES, ModelDenormalizedFields.DEVICES)
