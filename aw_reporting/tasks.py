from datetime import datetime, timedelta
from aw_reporting.adwords_api import get_web_app_client, get_all_customers
from suds import WebFault
from celery import task
from django.db import transaction
from django.db.models import Max, Min, Sum
import pytz
import heapq
import logging

logger = logging.getLogger(__name__)
#logging.getLogger('oauth2client.client').setLevel(logging.WARNING)

#  helpers --


def update_aw_read_permissions():
    from aw_reporting.models import AWAccountPermission
    for permission in AWAccountPermission.objects.all():
        client = get_web_app_client(
            refresh_token=permission.aw_connection.refresh_token,
            client_customer_id=permission.account_id,
        )
        try:
            get_all_customers(client, page_size=1, limit=1)
        except WebFault as e:
            if "AuthorizationError.USER_PERMISSION_DENIED" in\
                    e.fault.faultstring:
                # access was terminated
                permission.can_read = False
            else:
                raise
        else:
            permission.can_read = True
        finally:
            permission.save()


def quart_views(row, n):
    per = getattr(row, 'VideoQuartile%dRate' % n)
    impressions = int(row.Impressions)
    return float(per.rstrip('%')) / 100 * impressions


def get_base_stats(row, quartiles=False):
    stats = dict(
        impressions=row.Impressions,
        video_views=row.VideoViews,
        clicks=row.Clicks,
        cost=float(row.Cost)/1000000,
        conversions=row.Conversions.replace(',', ''),
        all_conversions=row.AllConversions.replace(',', '')
        if hasattr(row, "AllConversions") else 0,
        view_through=row.ViewThroughConversions,
    )
    if quartiles:
        stats.update(
            video_views_25_quartile=quart_views(row, 25),
            video_views_50_quartile=quart_views(row, 50),
            video_views_75_quartile=quart_views(row, 75),
            video_views_100_quartile=quart_views(row, 100),
        )
    return stats


def execute_for_every_account(method, connection, queue, account_ids=None):
    accounts = connection.account.all_customers
    if account_ids:
        accounts = [i for i in accounts if i.id in account_ids]
    if queue:
        for account in accounts:
            queue.put((method, connection, account))
    else:
        for account in accounts:
            method(connection, account)


def _get_yesterday(timezone=None):
    from aw_reporting.models import DEFAULT_TIMEZONE
    return datetime.now(
        tz=pytz.timezone(timezone or DEFAULT_TIMEZONE)
    ).date() - timedelta(1)


def drop_latest_stats(queryset):
    # delete stats for ten days
    yesterday = _get_yesterday()
    date_delete = yesterday - timedelta(10)
    queryset.filter(date__gte=date_delete).delete()


def get_account_border_dates(account):
    from aw_campaign.models import CampaignStatistic
    dates = CampaignStatistic.objects.filter(
        campaign__account=account
    ).aggregate(
        min_date=Min('date'),
        max_date=Max('date'),
    )
    return dates['min_date'], dates['max_date']


def format_bid_value(value):
    value = value.strip(' -')
    if value and value.isnumeric():
        return value

MIN_FETCH_DATE = datetime(2012, 1, 1).date()

GET_DF = '%Y-%m-%d'
# -- helpers


def save_accounts(account_connection, *_):
    from aw_campaign.models import Account
    customers, links = get_customers_with_links(
        account_connection.account.id,
        account_connection.refresh_token
    )
    customer_ids = [c['customerId'] for c in customers]
    saved_customers = Account.objects.filter(id__in=customer_ids)
    saved_customers = {c.id: c for c in saved_customers}

    ids = set([acc_id for l in links
               for acc_id in (l['clientCustomerId'],
                              l['managerCustomerId'])])

    with transaction.atomic():
        create = []
        for customer in customers:
            customer_id = str(customer['customerId'])
            if customer_id in saved_customers:
                sc = saved_customers[customer_id]
                if (sc.name, sc.currency_code, sc.timezone) != (
                    customer['name'], customer['currencyCode'],
                    customer['dateTimeZone']
                ):
                    sc.name = customer['name']
                    sc.currency_code = customer['currencyCode']
                    sc.timezone = customer['dateTimeZone']
                    sc.save()
            else:
                create.append(
                    Account(
                        pk=customer['customerId'],
                        name=customer['name'],
                        currency_code=customer['currencyCode'],
                        timezone=customer['dateTimeZone'],
                    )
                )

        if create:
            Account.objects.bulk_create(create)

        accounts = {a.id: a for a in Account.objects.filter(id__in=ids)}
        for link in links:
            manager = accounts[str(link['managerCustomerId'])]
            customer = accounts[str(link['clientCustomerId'])]
            if customer.manager_id != manager.id:
                customer.manager = manager
                customer.save()


def save_campaigns(connection, queue, accounts):
    execute_for_every_account(get_campaigns, connection, queue, accounts)


def save_videos(connection, queue, accounts):
    execute_for_every_account(get_videos, connection, queue, accounts)


def save_ads(connection, queue, accounts):
    execute_for_every_account(get_ads, connection, queue, accounts)


def save_ad_groups(connection, queue, accounts):
    execute_for_every_account(get_ad_groups, connection, queue, accounts)


def save_placements(connection, queue, accounts):
    execute_for_every_account(get_placements, connection, queue, accounts)


def save_genders(connection, queue, accounts):
    execute_for_every_account(get_genders, connection, queue, accounts)


def save_age_ranges(connection, queue, accounts):
    execute_for_every_account(get_age_ranges, connection, queue, accounts)


def save_keywords(connection, queue, accounts):
    execute_for_every_account(get_keywords, connection, queue, accounts)


def save_topics(connection, queue, accounts):
    execute_for_every_account(get_topics, connection, queue, accounts)


def save_interests(connection, queue, accounts):
    execute_for_every_account(get_interests, connection, queue, accounts)


def save_cities(connection, queue, accounts):
    execute_for_every_account(get_cities, connection, queue, accounts)


def get_campaigns(connection, account):
    from aw_campaign.models import Devices, CampaignStatistic, Campaign,\
        SUM_STATS, QUARTILE_STATS

    stats_queryset = CampaignStatistic.objects.filter(
        campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    # lets find min and max dates for the report request
    dates = stats_queryset.aggregate(max_date=Max('date'))
    min_date = dates['max_date'] + timedelta(days=1) \
        if dates['max_date'] else MIN_FETCH_DATE
    max_date = _get_yesterday(account.timezone)

    existed_ids = list(
        Campaign.objects.filter(
            account=account
        ).values_list('id', flat=True)
    )

    ad_client = get_client(
        refresh_token=connection.refresh_token,
        client_customer_id=account.id
    )
    report = campaign_performance_report(
        ad_client,
        dates={'min': min_date,
               'max': max_date},
        include_zero_impressions=False,
        additional_fields=('Device', 'Date')
    )
    insert_campaign = []
    insert_stat_item = []
    updated_campaigns = []

    with transaction.atomic():
        for row_obj in report:
            campaign_id = row_obj.CampaignId

            # update or create campaigns
            if campaign_id not in updated_campaigns:
                updated_campaigns.append(campaign_id)

                try:
                    end_date = datetime.strptime(row_obj.EndDate, GET_DF)
                except ValueError:
                    end_date = None

                stats = {
                    'name': row_obj.CampaignName,
                    'account': account,
                    'type': row_obj.AdvertisingChannelType,
                    'start_date': datetime.strptime(row_obj.StartDate,
                                                    GET_DF),
                    'end_date': end_date,
                    'budget': float(row_obj.Amount)/1000000,
                    '_status': row_obj.CampaignStatus,
                    'labels': row_obj.Labels.strip('- ') or "[]",
                }
                if campaign_id not in existed_ids:
                    existed_ids.append(campaign_id)
                    stats['id'] = campaign_id
                    insert_campaign.append(Campaign(**stats))
                else:
                    Campaign.objects.filter(pk=campaign_id).update(**stats)
            # --update or create campaigns
            # insert stats
            stats = {
                'date': row_obj.Date,
                'campaign_id': row_obj.CampaignId,
                'device_id': Devices.index(row_obj.Device),

                'video_views_25_quartile': quart_views(row_obj, 25),
                'video_views_50_quartile': quart_views(row_obj, 50),
                'video_views_75_quartile': quart_views(row_obj, 75),
                'video_views_100_quartile': quart_views(row_obj, 100),
            }
            stats.update(get_base_stats(row_obj))
            insert_stat_item.append(CampaignStatistic(**stats))

        if insert_campaign:
            Campaign.objects.bulk_create(insert_campaign)

        if insert_stat_item:
            CampaignStatistic.objects.safe_bulk_create(insert_stat_item)

        # update aggregated campaign stats
        if updated_campaigns:
            stat_names = QUARTILE_STATS + SUM_STATS
            stats = CampaignStatistic.objects.filter(
                campaign_id__in=updated_campaigns
            ).values('campaign_id').order_by('campaign_id').annotate(
                **{s: Sum(s) for s in stat_names}
            )
            for s in stats:
                campaign_id = s.pop('campaign_id')
                Campaign.objects.filter(id=campaign_id).update(**s)


def get_videos(account_connection, account):
    from aw_campaign.models import VideoCreative, VideoCreativeStatistic, \
        AdGroup

    stats_queryset = VideoCreativeStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    # lets find min and max dates for the report request
    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_stats = stats_queryset.aggregate(
        max_date=Max('date'),
    )
    saved_max_date = saved_stats.get('max_date')

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

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        reports = video_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
            more_fields=("Date",)
        )
        with transaction.atomic():
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


def get_ads(account_connection, account):
    from aw_campaign.models import Ad, AdStatistic

    stats_queryset = AdStatistic.objects.filter(
        ad__ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        ad_ids = list(
            Ad.objects.filter(
                ad_group__campaign__account=account
            ).values_list('id', flat=True)
        )

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = ad_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
            include_zero_impressions=False,
            additional_fields=('Date', 'AveragePosition',)
        )

        create_ad = []
        create_stat = []
        updated_ads = []
        with transaction.atomic():
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
                create_stat.append(AdStatistic(**stats))

            if create_ad:
                Ad.objects.bulk_create(create_ad)

            if create_stat:
                AdStatistic.objects.safe_bulk_create(create_stat)


def get_ad_groups(account_connection, account):
    from aw_campaign.models import AdGroup, AdGroupStatistic, Devices

    stats_queryset = AdGroupStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')
    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        ad_group_ids = list(AdGroup.objects.filter(
            campaign__account=account
        ).values_list('id', flat=True))

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = ad_group_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
            additional_fields=(
                "Date", "Device", "AdNetworkType1", "AdGroupStatus",
                "AveragePosition",
            )
        )

        with transaction.atomic():
            create_items = []
            create_stats = []
            updated_items = []
            for row_obj in report:
                ad_group_id = row_obj.AdGroupId

                # update ad groups
                if ad_group_id not in updated_items:
                    updated_items.append(ad_group_id)

                    stats = {
                        'name': row_obj.AdGroupName,
                        'status': row_obj.AdGroupStatus,
                        'campaign_id': row_obj.CampaignId,
                        'cpv_bid': format_bid_value(row_obj.CpvBid),
                        'cpm_bid': format_bid_value(row_obj.CpmBid),
                        'cpc_bid': format_bid_value(row_obj.CpcBid),
                    }
                    if ad_group_id in ad_group_ids:
                        AdGroup.objects.filter(
                            pk=ad_group_id).update(**stats)
                    else:
                        ad_group_ids.append(ad_group_id)
                        stats['id'] = ad_group_id
                        create_items.append(AdGroup(**stats))
                # --update ad groups
                # insert stats
                stats = {
                    'date': row_obj.Date,
                    'ad_network': row_obj.AdNetworkType1,
                    'device_id': Devices.index(row_obj.Device),
                    'ad_group_id': ad_group_id,
                    'average_position': row_obj.AveragePosition,
                    'video_views_25_quartile': quart_views(row_obj, 25),
                    'video_views_50_quartile': quart_views(row_obj, 50),
                    'video_views_75_quartile': quart_views(row_obj, 75),
                    'video_views_100_quartile': quart_views(row_obj, 100),
                }
                stats.update(get_base_stats(row_obj))
                create_stats.append(AdGroupStatistic(**stats))

            if create_items:
                AdGroup.objects.bulk_create(create_items)

            if create_stats:
                AdGroupStatistic.objects.safe_bulk_create(create_stats)


def get_placements(account_connection, account):

    from aw_campaign.models import ChannelStatistic, PlacementChannel, \
        Devices, ManagedPlacementStatistic, Placement

    channel_stats_queryset = ChannelStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    video_stats_queryset = ManagedPlacementStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(channel_stats_queryset)
    drop_latest_stats(video_stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

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

        channel_ids = set(
            PlacementChannel.objects.values_list('id', flat=True)
        )
        placements = set(
            Placement.objects.all().values_list('id', flat=True))

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = placement_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        with transaction.atomic():
            bulk_channels = []
            bulk_channel_data = []
            bulk_videos = []
            bulk_video_data = []

            for row_obj in report:
                # only channels
                display_name = row_obj.DisplayName
                criteria = row_obj.Criteria.strip()

                if '/channel/' in display_name:

                    if criteria not in channel_ids:
                        channel_ids.update({criteria})
                        bulk_channels.append(
                            PlacementChannel(id=criteria))

                    stats = {
                        'channel_id': criteria,
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
                    bulk_channel_data.append(ChannelStatistic(**stats))

                elif '/video/' in display_name:

                    # only youtube ids we need in criteria
                    if 'youtube.com/video/' in criteria:
                        criteria = criteria.split('/')[-1]

                    if criteria not in placements:
                        placements.update({criteria})
                        bulk_videos.append(Placement(id=criteria))

                    stats = {
                        'placement_id': criteria,
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
                    bulk_video_data.append(
                        ManagedPlacementStatistic(**stats)
                    )

            if bulk_channels:
                PlacementChannel.objects.safe_bulk_create(
                    bulk_channels)

            if bulk_channel_data:
                ChannelStatistic.objects.safe_bulk_create(
                    bulk_channel_data)

            if bulk_videos:
                Placement.objects.safe_bulk_create(bulk_videos)

            if bulk_video_data:
                ManagedPlacementStatistic.objects.safe_bulk_create(
                    bulk_video_data)


def get_genders(account_connection, account):
    from aw_campaign.models import GenderStatistic, Genders

    stats_queryset = GenderStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = gender_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        with transaction.atomic():
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
                bulk_data.append(GenderStatistic(**stats))

            if bulk_data:
                GenderStatistic.objects.safe_bulk_create(bulk_data)


def get_age_ranges(account_connection, account):
    from aw_campaign.models import AgeRangeStatistic, AgeRanges

    stats_queryset = AgeRangeStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = age_range_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        with transaction.atomic():
            bulk_data = []
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
                bulk_data.append(AgeRangeStatistic(**stats))

            if bulk_data:
                AgeRangeStatistic.objects.safe_bulk_create(bulk_data)


def get_keywords(account_connection, account):
    from aw_campaign.models import Keyword, KeywordStatistic

    stats_queryset = KeywordStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        keywords = set(Keyword.objects.values_list('name', flat=True))

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = keywords_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        with transaction.atomic():
            bulk_keywords = []
            bulk_data = []
            for row_obj in report:
                keyword = row_obj.Criteria
                stats = {
                    'keyword_id': keyword,
                    'date': row_obj.Date,
                    'ad_group_id': row_obj.AdGroupId,

                    'video_views_25_quartile': quart_views(row_obj, 25),
                    'video_views_50_quartile': quart_views(row_obj, 50),
                    'video_views_75_quartile': quart_views(row_obj, 75),
                    'video_views_100_quartile': quart_views(row_obj, 100),
                }
                stats.update(get_base_stats(row_obj))
                if keyword not in keywords:
                    keywords.update({keyword})
                    bulk_keywords.append(Keyword(name=keyword))

                bulk_data.append(KeywordStatistic(**stats))

            if bulk_keywords:
                Keyword.objects.safe_bulk_create(bulk_keywords)

            if bulk_data:
                KeywordStatistic.objects.safe_bulk_create(bulk_data)


def get_topics(account_connection, account):
    from aw_campaign.models import Topic, TopicStatistic

    stats_queryset = TopicStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        topics = dict(Topic.objects.values_list('name', 'id'))

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = topics_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        with transaction.atomic():
            bulk_data = []
            for row_obj in report:
                topic_name = row_obj.Criteria
                if topic_name not in topics:
                    logger.warning("topic not found: {}")
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
                stats.update(
                    get_base_stats(row_obj)
                )
                bulk_data.append(TopicStatistic(**stats))

            if bulk_data:
                TopicStatistic.objects.safe_bulk_create(bulk_data)


def get_interests(account_connection, account):

    from aw_campaign.models import AudienceStatistic, RemarkStatistic, \
        RemarkList, Audience

    audience_stats_queryset = AudienceStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    remark_stats_queryset = RemarkStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(audience_stats_queryset)
    drop_latest_stats(remark_stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

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

        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = audience_performance_report(
            ad_client,
            dates={'min': min_date, 'max': max_date},
        )
        remark_ids = set(RemarkList.objects.values_list('id', flat=True))
        interest_ids = set(Audience.objects.values_list('id', flat=True))
        bulk_aud_stats = []
        bulk_remarks = []
        bulk_rem_stats = []
        with transaction.atomic():
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
                au_type, au_id, *_ = row_obj.Criteria.split('::')
                if au_type == 'boomuserlist':
                    stats.update(remark_id=au_id)
                    bulk_rem_stats.append(RemarkStatistic(**stats))
                    if au_id not in remark_ids:
                        remark_ids.update({au_id})
                        bulk_remarks.append(
                            RemarkList(id=au_id, name=row_obj.UserListName)
                        )

                elif au_type == 'uservertical':
                    if int(au_id) not in interest_ids:
                        logger.warning("Audience %s not found" % au_id)
                        continue

                    stats.update(audience_id=au_id)
                    bulk_aud_stats.append(AudienceStatistic(**stats))

                elif au_type == 'customaffinity':
                    # custom audiences ara not yet supported
                    continue
                else:
                    logger.warning(
                        'Undefined criteria = %s' % row_obj.Criteria)

            if bulk_remarks:
                RemarkList.objects.safe_bulk_create(bulk_remarks)

            if bulk_rem_stats:
                RemarkStatistic.objects.safe_bulk_create(
                    bulk_rem_stats)

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


def get_cities(account_connection, account):
    from aw_campaign.models import CityStatistic, GeoTarget

    stats_queryset = CityStatistic.objects.filter(
        ad_group__campaign__account=account
    )
    drop_latest_stats(stats_queryset)

    min_acc_date, max_acc_date = get_account_border_dates(account)
    if max_acc_date is None:
        return

    saved_max_date = stats_queryset.aggregate(
        max_date=Max('date'),
    ).get('max_date')

    if saved_max_date is None or saved_max_date < max_acc_date:
        min_date = saved_max_date + timedelta(days=1) \
            if saved_max_date else min_acc_date
        max_date = max_acc_date

        # getting top cities
        ad_client = get_client(
            refresh_token=account_connection.refresh_token,
            client_customer_id=account.id
        )
        report = geo_performance_report(
            ad_client, additional_fields=('Cost',)
        )

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
            ad_client,
            dates={'min': min_date, 'max': max_date},
            additional_fields=tuple(main_statistics) +
            ('Date', 'AdGroupId')
        )

        bulk_data = []
        for row_obj in filter(
            lambda i: i.CityCriteriaId.isnumeric() and
                int(i.CityCriteriaId) in top_cities, report):

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
##
# statistics
##


def categories_define_parents():
    from aw_campaign.models import Audience
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
    from aw_campaign.models import Audience, Topic

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
    from aw_campaign.models import GeoTarget

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
