from datetime import date
from datetime import timedelta
import logging
import time

from django.utils import timezone

from google.ads.google_ads.v2.services.enums import AuthorizationErrorEnum
from google.ads.google_ads.errors import GoogleAdsException
from google.api_core.exceptions import InternalServerError
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import RetryError
from google.api_core.exceptions import ResourceExhausted
from google.auth.exceptions import RefreshError

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
from aw_reporting.models import Account
from aw_reporting.models import Opportunity
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account

logger = logging.getLogger(__name__)


class GoogleAdsAuthErrors:
    USER_PERMISSION_DENIED = "USER_PERMISSION_DENIED"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    CUSTOMER_NOT_ENABLED = "CUSTOMER_NOT_ENABLED"
    UNSPECIFIED = "UNSPECIFIED"
    OAUTH_TOKEN_EXPIRED = "OAUTH_TOKEN_EXPIRED"
    OAUTH_TOKEN_REVOKED = "OAUTH_TOKEN_REVOKED"
    CUSTOMER_NOT_FOUND = "CUSTOMER_NOT_FOUND"
    NOT_ADS_USER = "NOT_ADS_USER"


class GoogleAdsUpdater(object):
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

    def __init__(self, cid_account=None, mcc_account=None):
        self.mcc_account = mcc_account
        self.cid_account = cid_account
        self.auth_error_enum = AuthorizationErrorEnum().AuthorizationError

    def update_all_except_campaigns(self, mcc_account, cid_account):
        """
        Update / Save all reporting data except Campaign data
        :return:
        """
        self.mcc_account = mcc_account
        self.cid_account = cid_account

        for update_class in self.main_updaters:
            updater = update_class(self.cid_account)
            self.execute_with_any_permission(updater)
        self.cid_account.update_time = timezone.now()
        self.cid_account.save()
        recalculate_de_norm_fields_for_account(self.cid_account.id)

    def update_campaigns(self, mcc_account, cid_account):
        """
        Update / Save campaigns for all accounts managed by
            Run in separate process on a more frequent interval than other reporting data
        :return:
        """
        self.mcc_account = mcc_account
        self.cid_account = cid_account

        campaign_updater = CampaignUpdater(self.cid_account)
        self.execute_with_any_permission(campaign_updater)
        self.cid_account.hourly_updated_at = timezone.now()
        self.cid_account.save()
        recalculate_de_norm_fields_for_account(self.cid_account.id)

    def update_accounts_for_mcc(self, mcc_account=None):
        """ Update /Save accounts managed by MCC """
        if mcc_account:
            self.mcc_account = mcc_account
        account_updater = AccountUpdater(self.mcc_account)
        self.execute_with_any_permission(account_updater)

    def full_update(self, cid_account, any_permission=False, client=None):
        """
        Full Google ads update with all Updaters
        :param mcc_account:
        :param cid_accounts:
        :return:
        """
        self.cid_account = cid_account
        self.main_updaters = (CampaignUpdater,) + self.main_updaters
        if any_permission is False:
            client = get_client()
        for update_class in self.main_updaters:
            updater = update_class(cid_account)
            if any_permission:
                self.execute_with_any_permission(updater)
            else:
                self.execute(updater, client)
        recalculate_de_norm_fields_for_account(self.cid_account.id)

    @staticmethod
    def get_accounts_to_update_for_mcc(mcc_id, end_date_threshold=None, as_obj=False):
        """
        Get current CID accounts to update
        :param mcc_id: str
        :param end_date_threshold: date obj
        :param as_obj: bool
        :return: list
        """
        to_update = []
        end_date_threshold = end_date_threshold or date.today() - timedelta(days=1)
        cid_accounts = Account.objects.filter(managers=mcc_id, can_manage_clients=False, is_active=True)
        for account in cid_accounts:
            try:
                # Check if account has invalid Google Ads id
                int(account.id)
            except ValueError:
                continue
            # If no linked opportunities or any linked opportunity has not ended, update
            opportunities = Opportunity.objects.filter(aw_cid=account.id)
            opportunities_running = not opportunities or any(opp.end is None or opp.end > end_date_threshold for opp in opportunities)
            if opportunities_running or account.end_date is None or account.end_date > end_date_threshold:
                if as_obj is False:
                    account = account.id
                to_update.append(account)
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

    def execute_with_any_permission(self, updater):
        """
        Run update procedure with any available permission with error handling
        :param updater: Updater class object
        :return:
        """
        permissions = self.mcc_account.mcc_permissions.filter(
            can_read=True, aw_connection__revoked_access=False,
        )
        for permission in permissions:
            aw_connection = permission.aw_connection
            try:
                client = get_client(
                    login_customer_id=str(self.mcc_account.id),
                    refresh_token=aw_connection.refresh_token
                )
                self.execute(updater, client)
            # Permission denied, continue to try next
            except GoogleAdsUpdaterPermissionDenied:
                permission.can_read = False
                permission.save()
                continue

            except RefreshError:
                continue

            except Exception as e:
                logger.error(f"UNHANDLED Exception in GoogleAdsUpdater.execute_with_any_permission: {e}")

            else:
                return
        message = f"Unable to find AWConnection for MCC: {self.mcc_account.id} with updater: {updater.__class__.__name__}"
        if self.cid_account:
            message += f" for CID: {self.cid_account.id}"
        # If exhausted entire list of AWConnections, then was unable to find credentials to update
        logger.error(message)

    def execute(self, updater, client):
        """
        Handle invocation of update methods with error handling
            All updaters must implement an "update" method
        :param updater: Updater class object
        :param client:
        :return:
        """
        try:
            updater.update(client)

        # Client / request exceptions
        except GoogleAdsException as e:
            error = self.auth_error_enum(e.failure.errors[0].error_code.authorization_error).name

            # Invalid client
            if error == GoogleAdsAuthErrors.USER_PERMISSION_DENIED:
                logger.error(
                    f"Invalid client: login_customer_id: {client.login_customer_id}, {e}"
                )
                raise GoogleAdsUpdaterPermissionDenied
            # Customer is not valid
            elif error == GoogleAdsAuthErrors.CUSTOMER_NOT_ENABLED:
                self.cid_account.is_active = False
                self.cid_account.save()
            elif error == GoogleAdsAuthErrors.UNSPECIFIED:
                # Issue with customer resource
                logger.error(e)
                raise GoogleAdsUpdaterContinueException
            else:
                # Uncaught GoogleAdsException
                logger.error(f"UNCAUGHT GoogleAdsException EXCEPTION: {e}")

        # Google Ads API Interval exceptions
        except (ResourceExhausted, RetryError, InternalServerError, GoogleAPIError) as e:
            # Retry on Google Ads API server errors
            logger.error(f"retrying: {e}")
            try:
                self._retry(updater, client)
            except GoogleAdsUpdaterContinueException as e:
                logger.error(str(e))
                raise
            except Exception as e:
                logger.error(f"MAX RETRIES EXCEEDED: CID: {self.cid_account}, {e}")

        except Exception as e:
            try:
                cid = updater.account.id
            except AttributeError:
                cid = "None"
            logger.error(
                f"Unable to update with {updater.__class__.__name__} for cid: {cid} for mcc: {self.mcc_account.id}. ERR: {e}"
            )

    def _retry(self, updater, client):
        """
        Retry on Google Ads API 500 errors
        :param updater: Updater class object
        :param client: Valid Google Ads oauthed Client
        :return:
        """
        tries_count = 0
        try:
            while tries_count <= self.MAX_RETRIES:
                try:
                    updater.update(client)
                except Exception as err:
                    logger.error(f"On try {tries_count} of {self.MAX_RETRIES}")
                    tries_count += 1
                    if tries_count <= self.MAX_RETRIES:
                        sleep = tries_count ** self.SLEEP_COEFF
                        time.sleep(sleep)
                    else:
                        logger.error(f"MAX RETRIES EXCEEDED: {err}")
        except Exception as err:
            logger.error(f"UNHANDLED RETRY EXCEPTION: {err}")


# Exception has been handled and should continue processing next account
class GoogleAdsUpdaterContinueException(Exception):
    pass


# Unable to oauth for mcc
class GoogleAdsNoAWConnectionException(Exception):
    pass


class GoogleAdsUpdaterPermissionDenied(Exception):
    pass
