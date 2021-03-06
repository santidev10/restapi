import logging
import re
from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.db.models import Max
from django.db.models import Min
import pytz

from administration.notifications import send_html_email
from audit_tool.models import get_hash_name
from aw_reporting.models import Account
from aw_reporting.adwords_api import get_all_customers
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.adwords_reports import AccountInactiveError
from aw_reporting.google_ads.constants import GEO_TARGET_CONSTANT_FIELDS
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from utils.celery.tasks import REDIS_CLIENT


AD_WORDS_STABILITY_STATS_DAYS_COUNT = 10
"""int: Number of days, when Ads data can be changed by Google Ads.

Syncronization of Campaign stats, AdGroup stats, and other stats
is processed during this number of days, until data is stabilized.
VIQ-3255 shows difference between Campaign stats and AdGroups stats,
while this parameter was set to 7.
Similar problem: https://support.google.com/searchads/answer/1344072?hl=en
Most commonly, search engines may remove spam clicks or spam impressions.
As the engines make these changes, it will take time for us to reflect
the new data in Search Ads 360 reporting, as described in How fresh is
the reporting data on Search Ads 360? The data on the engines and
Search Ads 360 should be more stable after 10 days.
"""

logger = logging.getLogger(__name__)


def format_query(fields):
    query_fields = ""
    for field, values in fields.items():
        if query_fields:
            query_fields += ","
        query_str = ",".join(["{}.{}".format(field, value) for value in values])
        query_fields += query_str
    return query_fields


def drop_custom_stats(queryset, min_date, max_date):
    """ Delete stats for custom range """
    queryset.filter(date__gte=min_date, date__lte=max_date).delete()


def date_to_refresh_statistic(today):
    return today - timedelta(AD_WORDS_STABILITY_STATS_DAYS_COUNT)


def extract_placement_code(name):
    try:
        return re.search(r"(PL\d+)", name).group(1)
    except AttributeError:
        return None


def get_quartile_views(row):
    impressions = row.metrics.impressions.value
    quartiles = {
        "video_views_25_quartile": row.metrics.video_quartile_25_rate.value * impressions,
        "video_views_50_quartile": row.metrics.video_quartile_50_rate.value * impressions,
        "video_views_75_quartile": row.metrics.video_quartile_75_rate.value * impressions,
        "video_views_100_quartile": row.metrics.video_quartile_100_rate.value * impressions,
    }
    return quartiles


def get_base_stats(row, quartiles=False):
    impressions = row.metrics.impressions.value
    stats = dict(
        impressions=impressions,
        video_views=row.metrics.video_views.value,
        clicks=row.metrics.clicks.value,
        cost=row.metrics.cost_micros.value / 1000000,
        conversions=row.metrics.conversions.value,
        all_conversions=row.metrics.all_conversions.value if row.metrics.all_conversions else 0,
        view_through=row.metrics.view_through_conversions.value if row.metrics.view_through_conversions.value else 0,
    )
    if quartiles:
        stats.update(get_quartile_views(row))
    return stats


def get_date_range(days):
    days = int(days)
    if days >= 30:
        period = 30
    elif days >= 14:
        period = 14
    else:
        period = 7
    return "LAST_{}_DAYS".format(period)


def get_geo_target_constants(account, ga_service, resource_names: iter = None):
    """

    :param ga_service: GoogleAdsService
    :param resource_names: list -> Google Ads Geo Target Constant ids to filter for
    :return:
    """
    query_fields = format_query(GEO_TARGET_CONSTANT_FIELDS)
    query = "SELECT {} FROM geo_target_constant".format(query_fields)
    if resource_names:
        in_query = " IN ({})".format(",".join(resource_names))
        query += in_query
    geo_constants = ga_service.search(account.id, query=query)
    return geo_constants


def get_account_border_dates(account):
    dates = AdGroupStatistic.objects.filter(
        ad_group__campaign__account=account
    ).aggregate(
        min_date=Min("date"),
        max_date=Max("date"),
    )
    return dates["min_date"], dates["max_date"]


def max_ready_date(dt: datetime, tz=None, tz_str="UTC"):
    tz = tz or pytz.timezone(tz_str)
    return dt.astimezone(tz).date()


def drop_latest_stats(queryset, today):
    """ Delete stats for ten days """
    date_delete = date_to_refresh_statistic(today)
    queryset.filter(date__gte=date_delete).delete()


def reset_denorm_flag(ad_group_ids=None, campaign_ids=None):
    if ad_group_ids:
        AdGroup.objects.filter(id__in=ad_group_ids) \
            .update(de_norm_fields_are_recalculated=False)
    if campaign_ids is None:
        campaign_ids = AdGroup.objects.filter(id__in=ad_group_ids) \
            .values_list("campaign_id", flat=True).distinct()
    Campaign.objects.filter(id__in=campaign_ids) \
        .update(de_norm_fields_are_recalculated=False)


def detect_success_aw_read_permissions():
    # pylint: disable=import-outside-toplevel
    from aw_reporting.models import AWAccountPermission
    # pylint: enable=import-outside-toplevel
    aw_permissions = AWAccountPermission.objects.filter(
        can_read=False,
        account__is_active=True,
        aw_connection__revoked_access=False,
    )
    for permission in aw_permissions:
        try:
            client = get_web_app_client(
                refresh_token=permission.aw_connection.refresh_token,
                client_customer_id=permission.account_id,
            )
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            logger.error(e)
        else:
            try:
                get_all_customers(client, page_size=1, limit=1)
            except AccountInactiveError:
                account = permission.account
                account.is_active = False
                account.save()
                send_read_lost_email(account.name, account.id,
                                     "AccountInactiveError in detect_success_aw_read_permissions")
            # pylint: disable=broad-except
            except Exception as e:
                # pylint: enable=broad-except
                logger.error(e)
            else:
                permission.can_read = True
                permission.save()


def get_criteria_exists_key(ad_group_id: int, criteria_type_value: int, statistic_criteria: str):
    """
    Method to ensure tuple key is consistent with same Adwords row object and CriteriaTypeEnum.name
    """
    if not isinstance(ad_group_id, int):
        raise ValueError("ad_group_id must be int")
    if not isinstance(criteria_type_value, int):
        raise ValueError("criteria_type_value must be int")
    if not isinstance(statistic_criteria, str):
        raise ValueError("statistic_criteria must be str")
    exists_key = (ad_group_id, criteria_type_value, statistic_criteria)
    return exists_key


def send_read_lost_email(account: Account, message, recipients=settings.GOOGLE_ADS_UPDATE_ERROR_EMAIL_ADDRESSES):
    lock_name = f"adwords_update_alert_email_{get_hash_name(f'{account.id}{message}')}"
    is_acquired = REDIS_CLIENT.lock(lock_name, timeout=60 * 60 * 12).acquire(blocking=False)
    if is_acquired:
        subject = f"Adwords API Update: Read lost for account: {account.name} - {account.id}"
        send_html_email(
            subject=subject,
            to=recipients,
            text_header=subject,
            text_content=message,
            from_email=settings.EXPORTS_EMAIL_ADDRESS
        )
