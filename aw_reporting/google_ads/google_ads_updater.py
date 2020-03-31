from datetime import date
from datetime import timedelta
import logging
import time

from django.db.models import F
from django.utils import timezone
from oauth2client.client import HttpAccessTokenRefreshError
from google.auth.exceptions import RefreshError
from googleads.errors import AdManagerReportError
from suds import WebFault

from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.adwords_reports import AccountInactiveError
from aw_reporting.google_ads.google_ads_api import get_client
from aw_reporting.google_ads.updaters.accounts import AccountUpdater
from aw_reporting.google_ads.updaters.ad_groups import AdGroupUpdater
from aw_reporting.google_ads.updaters.ads import AdUpdater
from aw_reporting.google_ads.updaters.audiences import update_audiences
from aw_reporting.google_ads.updaters.age_range import AgeRangeUpdater
from aw_reporting.google_ads.updaters.campaigns import CampaignUpdater
from aw_reporting.google_ads.updaters.campaign_location_target import CampaignLocationTargetUpdater
from aw_reporting.google_ads.updaters.cities import CityUpdater
from aw_reporting.google_ads.updaters.genders import GenderUpdater
from aw_reporting.google_ads.updaters.geo_targets import GeoTargetUpdater
from aw_reporting.google_ads.updaters.interests import InterestUpdater
from aw_reporting.google_ads.updaters.keywords import KeywordUpdater
from aw_reporting.google_ads.updaters.parents import ParentUpdater
from aw_reporting.google_ads.updaters.placements import PlacementUpdater
from aw_reporting.google_ads.updaters.topics import TopicUpdater
from aw_reporting.google_ads.updaters.videos import VideoUpdater
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models import Account
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account

logger = logging.getLogger(__name__)


class GoogleAdsErrors:
    USER_PERMISSION_DENIED = "USER_PERMISSION_DENIED"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    CUSTOMER_NOT_ENABLED = "CUSTOMER_NOT_ENABLED"
    UNSPECIFIED = "UNSPECIFIED"
    OAUTH_TOKEN_EXPIRED = "OAUTH_TOKEN_EXPIRED"
    OAUTH_TOKEN_REVOKED = "OAUTH_TOKEN_REVOKED"
    CUSTOMER_NOT_FOUND = "CUSTOMER_NOT_FOUND"
    NOT_ADS_USER = "NOT_ADS_USER"
    INVALID_PAGE_TOKEN = "INVALID_PAGE_TOKEN"
    EXPIRED_PAGE_TOKEN = "EXPIRED_PAGE_TOKEN"


class GoogleAdsUpdater(object):
    CACHE_KEY_PREFIX = "restapi.GoogleAdsUpdater"
    MAX_RETRIES = 5
    SLEEP_COEFF = 2

    main_updaters = (
        AdGroupUpdater,
        VideoUpdater,
        AdUpdater,
        GenderUpdater,
        ParentUpdater,
        AgeRangeUpdater,
        PlacementUpdater,
        KeywordUpdater,
        TopicUpdater,
        InterestUpdater,
        CityUpdater,
        CampaignLocationTargetUpdater,
    )

    def __init__(self, account):
        self.account = account

    def update_all_except_campaigns(self):
        """
        Update / Save all reporting data except Campaign data
        :return:
        """
        for update_class in self.main_updaters:
            updater = update_class(self.account)
            self.execute_with_any_permission(updater)
        recalculate_de_norm_fields_for_account(self.account.id)
        self.account.update_time = timezone.now()
        self.account.save()

    def update_campaigns(self):
        """
        Update / Save campaigns for account
            Run in separate process on a more frequent interval than other reporting data
        :return:
        """
        campaign_updater = CampaignUpdater(self.account)
        self.execute_with_any_permission(campaign_updater)
        self.account.hourly_updated_at = timezone.now()
        self.account.save()
        recalculate_de_norm_fields_for_account(self.account.id)

    def update_accounts_as_mcc(self, mcc_account=None):
        if mcc_account:
            self.account = mcc_account
        """ Update /Save accounts managed by MCC """
        account_updater = AccountUpdater(self.account)
        self.execute_with_any_permission(account_updater, mcc_account=self.account)

    def full_update(self, mcc_account=None):
        """
        Full Google ads update with all Updaters
        :param mcc_account: Account
        :return:
        """
        self.main_updaters = (CampaignUpdater,) + self.main_updaters
        for update_class in self.main_updaters:
            updater = update_class(self.account)
            if mcc_account:
                self.execute_with_any_permission(updater, mcc_account=mcc_account)
            else:
                self.execute_with_any_permission(updater)
        recalculate_de_norm_fields_for_account(self.account.id)
        self.account.update_time = timezone.now()
        self.account.save()

    @staticmethod
    def get_accounts_to_update(hourly_update=True, end_date_from_days=AD_WORDS_STABILITY_STATS_DAYS_COUNT, as_obj=False, size=None):
        """
        Get current CID accounts to update
            Retrieves all active Placements and linked CID accounts
            Retrieves all active Opportunities and linked CID accounts
            Need to query from both models since in some cases, a CID will be
                linked through a placement but not through an Opportunity
            Ordered by last updated
        :param hourly_update: bool
            If hourly_update is True, then order accounts by hourly updated at
            Else, order accounts by full_update at

            hourly_updated_at ordering used by Google Ads hourly account / campaign update
            full_updated_at ordering used by Google Ads update all without campaigns
        :param end_date_from_days: int -> Retrieve Opportunities with end dates from today
        :param as_obj: bool
        :param size: int -> How many items to return from start of list
        :return: list
        """
        to_update = []
        end_date_threshold = date.today() - timedelta(days=end_date_from_days)
        if hourly_update:
            order_by_field = "hourly_updated_at"
        else:
            order_by_field = "update_time"

        active_ids_from_placements = set(OpPlacement.objects.filter(end__gte=end_date_threshold).values_list("adwords_campaigns__account", flat=True).distinct())
        active_accounts_from_placements = Account.objects.filter(id__in=active_ids_from_placements, can_manage_clients=False, is_active=True).order_by(F(order_by_field).asc(nulls_first=True))

        active_opportunities = Opportunity.objects.filter(end__gte=end_date_threshold)
        active_ids_from_opportunities = []
        for opp in active_opportunities:
            try:
                aw_cid = opp.aw_cid.split(",")
                active_ids_from_opportunities.extend([_id.strip() for _id in aw_cid if _id and _id not in active_ids_from_placements])
            except AttributeError:
                continue
        active_accounts_from_opportunities = Account.objects.filter(id__in=active_ids_from_opportunities, can_manage_clients=False, is_active=True).order_by(F(order_by_field).asc(nulls_first=True))

        for account in active_accounts_from_placements | active_accounts_from_opportunities:
            try:
                int(account.id)
            except ValueError:
                continue
            except AttributeError:
                # Account name is None
                pass
            if as_obj is False:
                account = account.id
            to_update.append(account)
        if size:
            to_update = to_update[:size]
        return to_update

    @staticmethod
    def update_audiences():
        """ Update Google ads Audiences """
        update_audiences()

    @staticmethod
    def update_geo_targets():
        """ Update Google ads GeoTargets """
        client = get_client()
        geo_target_updater = GeoTargetUpdater()
        geo_target_updater.update(client)

    def execute_with_any_permission(self, updater, mcc_account=None):
        """
        Run update procedure with any available permission with error handling
        :param updater: Updater class object
        :param mcc_account:
        :return:
        """
        if mcc_account:
            permissions = AWAccountPermission.objects.filter(
                account=mcc_account
            )
        else:
            permissions = AWAccountPermission.objects.filter(
                account__in=self.account.managers.all()
            )
        permissions = permissions.filter(can_read=True, aw_connection__revoked_access=False,)
        for permission in permissions:
            aw_connection = permission.aw_connection
            try:
                client = get_web_app_client(
                    refresh_token=aw_connection.refresh_token,
                    client_customer_id=self.account.id,
                )
                self.execute(updater, client)

            except AccountInactiveError:
                self.account.is_active = False
                self.account.save()

            except (RefreshError, HttpAccessTokenRefreshError) as e:
                logger.warning((permission, e))
                aw_connection.revoked_access = True
                aw_connection.save()
                continue

            except WebFault as e:
                if "AuthorizationError.USER_PERMISSION_DENIED" in \
                        e.fault.faultstring:
                    logger.warning((permission, e))
                    permission.can_read = False
                    permission.save()
                else:
                    raise

            except Exception as e:
                logger.error(f"Unhandled error in execute_with_any_permission: {e}")
            else:
                return

        # If exhausted entire list of AWConnections, then was unable to find credentials to update
        if updater.__class__ == AccountUpdater and mcc_account:
            Account.objects.filter(id=mcc_account.id).update(is_active=False)
            logger.info(f"Account access revoked for MCC: {mcc_account.id}")

        logger.warning(f"Unable to find AWConnection for CID: {self.account.id} with updater: {updater.__class__.__name__}")

    def execute(self, updater, client):
        """
        Handle invocation of update methods with retry for Google Ads server errors
            All updaters must implement an "update" method
        :param updater: Updater class object
        :param client:
        :return:
        """
        try:
            updater.update(client)
        except RefreshError:
            raise
        except Exception:
            try:
                self._retry(updater, client)
            except Exception:
                raise

    def _retry(self, updater, client):
        """
        Retry on Google Ads API 500 errors
        :param updater: Updater class object
        :param client: Valid Google Ads oauthed Client
        :return:
        """
        tries_count = 0
        while tries_count <= self.MAX_RETRIES:
            try:
                updater.update(client)
            except Exception as err:
                tries_count += 1
                if tries_count <= self.MAX_RETRIES:
                    sleep = tries_count ** self.SLEEP_COEFF
                    time.sleep(sleep)
                else:
                    raise err
            else:
                return


# Unable to oauth for mcc
class GoogleAdsNoAWConnectionException(Exception):
    pass


class GoogleAdsUpdaterPermissionDenied(Exception):
    pass
