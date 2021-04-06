from datetime import date
from datetime import timedelta
from typing import Tuple
from typing import Union

from django.db.models import Max
from django.db.models import Min
from google.api_core.page_iterator import GRPCIterator

from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models.ad_words.account import Account
from aw_reporting.models.ad_words.ad_group import AdGroup
from aw_reporting.models.statistic import AdGroupGeoViewStatistic
from google.ads.google_ads.errors import GoogleAdsException
from performiq.analyzers.utils import Coercers
from utils.datetime import now_in_default_tz
from utils.db.functions import safe_bulk_create


DATE_FORMAT = "%Y-%m-%d"  # month and day are zero-padded
PERMISSION_DENIED_ERROR_CODE = "authorization_error: USER_PERMISSION_DENIED"
EXPECTED_ERROR_CODES = [
    PERMISSION_DENIED_ERROR_CODE,
]


def update(hourly_update=True, size=50):
    """
    this will update pricing tool ad group stats on a schedule
    will also support updating all as an option
    :return:
    """
    accounts = GoogleAdsUpdater.get_accounts_to_update(hourly_update=hourly_update, size=size, as_obj=True)
    for account in accounts:
        print(f"processing account: {account.name} (id: {account.id})")
        updater = PricingToolAdGroupStatsUpdater(account=account)
        updater.run()


class PricingToolAdGroupStatsUpdater(UpdateMixin):
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
        self.stats_to_create = []
        self.query_start_date = None
        self.query_end_date = None
        self.missing_ad_group_ids = []

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

    def run(self, start_date: date = None, end_date: date = None):
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
            self.query_start_date = max_account_date - timedelta(days=AD_WORDS_STABILITY_STATS_DAYS_COUNT) \
                if max_account_date else self.MIN_FETCH_DATE
            # get the latest date depending on timezone of the account
            self.query_end_date = self.max_ready_date(now, tz_str=self.account.timezone)

        # make the request
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

                print(f"caught {e.__class__} exception. Errors codes: {','.join(error_codes)}")
                # retry with a different client if we get an expected permission denied exception
                continue

            # stop trying report requests on the first successful response, even if no items in the report
            break

        # only drop/create stats if there are stats to create
        self._drop_stats()
        self._create_stats()

        if len(self.missing_ad_group_ids):
            print(f"skipped stats for the following non-existant adgroup ids: {self.missing_ad_group_ids}")

    def _handle_response(self, response: GRPCIterator):
        """
        Update or create stat records
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
            self.stats_to_create.append(AdGroupGeoViewStatistic(**stats))

    def _drop_stats(self):
        """
        drop stats here, after we know the response is good and we have stats records to create
        :return:
        """
        if not len(self.stats_to_create):
            return
        print(f"dropping stats from {self.query_start_date} to {self.query_end_date}")
        queryset = AdGroupGeoViewStatistic.objects.filter(ad_group__campaign__account=self.account)
        self.drop_custom_stats(queryset=queryset, min_date=self.query_start_date, max_date=self.query_end_date)

    def _create_stats(self) -> None:
        """
        persist the stats records
        :return:
        """
        if not len(self.stats_to_create):
            return
        print(f"creating {len(self.stats_to_create)} stats record(s)")
        safe_bulk_create(AdGroupGeoViewStatistic, self.stats_to_create)

    def _get_stats_from_row(self, row) -> dict:
        """
        given a row, get a dict of the data we want to keep
        :param row:
        :return:
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
