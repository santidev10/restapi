import logging
from datetime import date
from datetime import timedelta
from typing import Tuple
from typing import Union

from django.conf import settings
from django.db.models import Max
from django.db.models import Min
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.v6.proto.services.google_ads_service_pb2 import GoogleAdsRow
from google.api_core.page_iterator import GRPCIterator

from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models.ad_words.account import Account
from aw_reporting.models.ad_words.ad_group import AdGroup
from aw_reporting.models.statistic import AdGroupGeoViewStatistic
from performiq.analyzers.utils import Coercers
from saas import celery_app
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.datetime import now_in_default_tz
from utils.db.functions import safe_bulk_create


LOCK_NAME = "pricing_tool_ad_group_stats"
DATE_FORMAT = "%Y-%m-%d"  # month and day are zero-padded
PERMISSION_DENIED_ERROR_CODE = "authorization_error: USER_PERMISSION_DENIED"
EXPECTED_ERROR_CODES = [
    PERMISSION_DENIED_ERROR_CODE,
]
CREATE_THRESHOLD = 10000


logger = logging.getLogger(__name__)


@celery_app.task
def update_geo_view_ad_group_stats_task(hourly_update=True, size=None):
    """
    task for updating geo view ad group stats on a schedule
    :return:
    """
    logger.info(f"google_ads_geo_view_ad_group_stats: Started...")
    lock(lock_name=LOCK_NAME, expires=timedelta(hours=1).total_seconds())
    if not isinstance(size, int):
        size = getattr(settings, "PRICING_TOOL_AD_GROUP_STATS_SIZE", 1)
    logger.info(f"google_ads_geo_view_ad_group_stats: Updating {size:,} accounts...")
    accounts = GoogleAdsUpdater.get_accounts_to_update(hourly_update=hourly_update, size=size, as_obj=True)
    created_count = 0
    try:
        for account in accounts:
            updater = PricingToolAccountAdGroupStatsUpdater(account=account)
            updater.run()
            created_count += updater.get_created_count()
    # pylint: disable=broad-except
    except Exception as e:
        # pylint: enable=broad-except
        logger.error(e)
    finally:
        logger.info(f"google_ads_geo_view_ad_group_stats: Completed. {created_count:,} stats records created.")
        unlock(lock_name=LOCK_NAME, fail_silently=True)


def update_all():
    accounts = Account.objects.filter(managers__isnull=False).distinct()
    for account in accounts:
        updater = PricingToolAccountAdGroupStatsUpdater(account=account)
        updater.run()


class PricingToolAccountAdGroupStatsUpdater(UpdateMixin):
    """
    Gets segmented adgroup stats using Google Ads' Geographic View Report. Saves those stats to the
    AdGroupGeoViewStatistic model. These stats will be used by PricingTool V2 to provide an segmented pricing history
    @see https://developers.google.com/google-ads/api/fields/v6/geographic_view
    """

    def __init__(self, account: Account):
        self.account = account
        # we'll use this list as a reference for which ad group ids we can create stats records for
        self.existing_ad_group_ids = list(AdGroup.objects.filter(
            campaign__account=self.account).values_list("id", flat=True))
        # create a list of clients to make requests with, based on managers who have access
        # the login_customer_id is partially used to determine if access for the request is granted or denied
        # @see https://developers.google.com/google-ads/api/docs/concepts/call-structure#cid
        self.clients = [get_client(login_customer_id=manager.id) for manager in account.managers.all()]
        # standard channelfactory mcc login_customer_id
        self.clients.append(get_client())
        self.create_queue = []
        self.query_start_date = None
        self.query_end_date = None
        self.missing_ad_group_ids = []
        self.stats_dropped = False
        self.created_count = 0

    @staticmethod
    def _get_query(*_, **substitutions) -> str:
        """
        build a GAQL query with supplied substitutions to transform the base query
        :param args:
        :param substitutions:
        :return:
        """
        query = """
            SELECT geographic_view.country_criterion_id, metrics.cost_micros, metrics.impressions,
            geographic_view.location_type, geographic_view.resource_name, ad_group.id, ad_group.name, ad_group.status,
            segments.geo_target_region, segments.geo_target_metro, segments.device, metrics.conversions,
            metrics.all_conversions, metrics.clicks, metrics.video_views, customer.id, customer.manager, segments.date
            FROM geographic_view WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """.format(**substitutions)
        return query

    def run(self, start_date: date = None, end_date: date = None) -> None:
        """
        run the updater for an account, optionally specify a star/end date
        :param start_date:
        :param end_date:
        :return:
        """
        # set query dates as supplied if valid
        if start_date and end_date and start_date < end_date:
            self.query_start_date = start_date
            self.query_end_date = end_date
        # set default start/end dates if not supplied
        else:
            now = now_in_default_tz()
            min_account_date, max_account_date = self.get_account_border_dates(self.account)
            # get the latest date depending on timezone of the account, use max ready date since that can be greater
            # than our latest stats because of timezone shift
            self.query_end_date = self.max_ready_date(now, tz_str=self.account.timezone)
            self.query_start_date = self.query_end_date - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT) \
                if max_account_date else self.MIN_FETCH_DATE

        # get the GAQL query
        query = self._get_query(start_date=self.query_start_date.strftime(DATE_FORMAT),
                                end_date=self.query_end_date.strftime(DATE_FORMAT))
        # use available clients until we get stats to save, or exhaust all clients
        for client in self.clients:
            service = client.get_service("GoogleAdsService", version="v6")
            response = service.search(str(self.account.id), query=query)
            try:
                self._handle_response(response=response)
            except GoogleAdsException as e:
                failure = e.failure
                error_codes = [str(error.error_code).strip() for error in failure.errors]
                intersection = list(set(error_codes) & set(EXPECTED_ERROR_CODES))
                if not intersection:
                    raise e

                # retry with a different client if we get an expected permission denied exception
                continue

            # stop trying report requests on the first successful response, even if no items in the report
            break

        # only drop/create stats if there are stats to create
        self._drop_stats_before_first_create()
        self._create_stats()
        self._clear_create_queue()

        if self.created_count:
            created_count_message = (f"{self.__class__.__qualname__} created {self.created_count:,} pricing tool ad "
                                     f"group stats records for account {self.account}")
            logger.info(created_count_message)
        else:
            logger.info(f"{self.__class__.__qualname__} unable to create records for account: {self.account}")
        if len(self.missing_ad_group_ids):
            logger.warning(f"skipped stats for the following non-existant adgroup ids: {self.missing_ad_group_ids}")

    def _handle_response(self, response: GRPCIterator) -> None:
        """
        queue up AdGroupGeoViewStatistic instances to create. create when threshold met. Continue until report rows
        are exhausted
        :param response:
        :return:
        """
        for row in response:
            # check that the ad group exists before adding a stat row
            ad_group_id = row.ad_group.id
            if ad_group_id not in self.existing_ad_group_ids:
                self.missing_ad_group_ids.append(ad_group_id)
                continue

            stats = self._get_stats_from_row(row)
            if self._skip_stats_creation(stats):
                continue
            self.create_queue.append(AdGroupGeoViewStatistic(**stats))
            # create stats if threshold met/exceeded. we don't want a huge queue of stuff to create.
            # not doing this could cause memory issues
            self._create_stats_if_threshold_met()

    @staticmethod
    def _skip_stats_creation(stats: dict) -> bool:
        """
        given a stats dictionary, representing a serialized report row, determine whether or not to discard the data
        we want to save db space by discarding data that has no impact on pricing, like when cost is zero
        :return:
        """
        if not stats.get("cost"):
            return True
        if not stats.get("impressions"):
            return True
        return False

    def _create_stats_if_threshold_met(self) -> None:
        """
        create all stats in create queue if create queue exceeds threshold
        :return:
        """
        if len(self.create_queue) < CREATE_THRESHOLD:
            return

        self._drop_stats_before_first_create()
        self._create_stats()
        self._clear_create_queue()

    def _drop_stats_before_first_create(self) -> None:
        """
        drop stats here, after we know the response is good and we have stats records to create. Only drop once
        per run, since we're creating records that would be deleted on a subsequent run of the drop query
        :return:
        """
        if self.stats_dropped:
            return

        if not len(self.create_queue):
            return

        queryset = AdGroupGeoViewStatistic.objects.filter(ad_group__campaign__account=self.account)
        self.drop_custom_stats(queryset=queryset, min_date=self.query_start_date, max_date=self.query_end_date)
        # don't drop stats again, since we're filling in stats that would be deleted by this query if run again
        self.stats_dropped = True

    def _create_stats(self) -> None:
        """
        persist the stats records
        :return:
        """
        create_count = len(self.create_queue)
        if not create_count:
            return
        safe_bulk_create(AdGroupGeoViewStatistic, self.create_queue)
        # record the number of items created
        self.created_count += len(self.create_queue)

    def _clear_create_queue(self) -> None:
        """
        clear the create queue (after having created all within)
        :return:
        """
        self.create_queue = []

    def _get_stats_from_row(self, row: GoogleAdsRow) -> dict:
        """
        given a row, get a dict of the data we want to keep
        NOTE: metrics are never null
        :param row:
        :return: dict
        """
        stats = {
            "ad_group_id": row.ad_group.id,
            "date": row.segments.date,
            "device_id": row.segments.device,
            "country_id": row.geographic_view.country_criterion_id,
            "region_id": self._get_geo_target_id(row.segments.geo_target_region),
            "metro_id": self._get_geo_target_id(row.segments.geo_target_metro),
            "impressions": row.metrics.impressions,
            "video_views": row.metrics.video_views,
            "clicks": row.metrics.clicks,
            "cost": Coercers.cost_micros(row.metrics.cost_micros),
            "conversions": row.metrics.conversions,
            "all_conversions": row.metrics.all_conversions,
        }
        return stats

    @staticmethod
    def _get_geo_target_id(value: str) -> Union[int, None]:
        """
        geo target id needs to be extracted from a "geoTargetConstant/{id}" string
        :param value:
        :return:
        """
        if "geoTargetConstants" not in value:
            return

        id_string = value.split("/")[-1]
        return int(id_string)

    @staticmethod
    def get_account_border_dates(account: Account) -> Tuple[date, ...]:
        """
        overrides the mixin method.
        Gets border dates (min/max stats dates) based on the AdGroupGeoViewStatistic model
        :param account:
        :return:
        """
        dates = AdGroupGeoViewStatistic.objects.filter(
            ad_group__campaign__account=account
        ).aggregate(
            min_date=Min("date"),
            max_date=Max("date"),
        )
        return dates["min_date"], dates["max_date"]

    def get_created_count(self):
        """
        get the number of records that were queued for creation
        :return:
        """
        return self.created_count
