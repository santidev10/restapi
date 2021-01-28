import abc
import csv
import logging
import os
from datetime import timedelta
from io import StringIO
from typing import Union

from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage

from aw_reporting.models import Campaign
from aw_reporting.models import SalesForceGoalType
from email_reports.reports.base import BaseEmailReport
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class NoStatsDataException(Exception):
    """ Raised when there is no stats data from which to create a report """
    pass


class AbstractDailyApexEmailReport(BaseEmailReport):

    def __init__(self, *args, **kwargs):
        """
        is_historical: Bool: If True, fetches ALL VideoCreativeStatistic records
        for the given Accounts ids, rather than just the previous day's
        """
        super().__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)
        self.user = self.get_user()
        self.from_email = self.get_from_email()
        self.to = self.get_to_list()
        self.cc = self.get_cc_list()
        self.attachment_filename = self.get_attachment_filename()
        self.historical_filename = self.get_historical_filename()

    @abc.abstractmethod
    def get_attachment_filename(self) -> str:
        """ set the attachment filename when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_historical_filename(self) -> str:
        """ set the historical filename (to write locally) when using the historical() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user(self):
        """ set the user from which to get visible_accounts """
        raise NotImplementedError

    def get_from_email(self):
        """ set the from email when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_to_list(self):
        """ set the list of recipients when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_cc_list(self):
        """ set the list of cc recipients when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def _get_subject(self):
        """ set the email subject when using the send method """
        raise NotImplementedError

    @abc.abstractmethod
    def _get_body(self):
        """ set the email body when using the send method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_stats(self, campaign_ids: list, is_historical: bool = False):
        """
        given a list of campaign ids, get relevant stats. should modify the return to get all values of is_historical
        is True
        :param campaign_ids:
        :param is_historical:
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_rows_from_stats(self, creative_statistics: list):
        """ given a list of creative statistics, get the rows for a given report """
        raise NotImplementedError

    def get_account_ids(self):
        """
        get the visible accounts for the supplied user
        :return:
        """
        return self.user.aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)

    def _get_campaign_ids(self):
        """
        get all campaigns that belong to the set user's visible_accounts account ids
        :return:
        """
        account_ids = self.get_account_ids()
        campaigns = Campaign.objects.filter(account_id__in=account_ids) \
            .values_list("id", flat=True)
        campaigns_ids = list(campaigns)
        return campaigns_ids

    def get_rows(self, is_historical=False):
        """
        main method used by self.historical and self.send to get rows for the report
        :param is_historical:
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        stats = self.get_stats(campaign_ids, is_historical=is_historical)
        if not stats.exists():
            raise NoStatsDataException()

        rows = self.get_rows_from_stats(list(stats))
        return rows

    def historical(self):
        """
        write a historical report to the local filesystem. DOES NOT SEND email report
        write historical csv data locally to self.historical_filename
        :return:
        """
        rows = self.get_rows(is_historical=True)

        try:
            os.remove(self.historical_filename)
        except FileNotFoundError:
            pass
        with open(self.historical_filename, mode="w") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            writer.writerows(rows)
        print(f"wrote historical data to filename: {self.historical_filename}")

    def send(self):
        """
        used by automated task to send the daily report to defined recipients
        :return:
        """
        if not self.to:
            logger.error(f"No recipients set for {self.__class__.__name__} Apex campaign report")
            return

        if not isinstance(self.user, get_user_model()):
            logger.error(f"The user must be a UserProfile instance for report: {self.__class__.__name__}")
            return

        rows = self.get_rows()
        csv_file = StringIO()
        writer = csv.writer(csv_file)
        writer.writerow(self.CSV_HEADER)
        writer.writerows(rows)
        csv_context = csv_file.getvalue()
        if not csv_context:
            logger.error(f"No data to send {self.__class__.__name__} Apex campaign report.")
            return

        msg = EmailMessage(
            subject=self._get_subject(),
            body=self._get_body(),
            from_email=self.from_email,
            to=self.get_to(self.to),
            cc=self.get_cc(self.cc),
            bcc=self.get_bcc()
        )

        msg.attach(self.attachment_filename, csv_context, "text/csv")
        msg.send(fail_silently=False)

    @staticmethod
    def _get_revenue(obj, campaign_prefix, rate_field=None) -> Union[float, None]:
        """
        calcuate revenue from the given stats object. If not calculable, return None
        :param obj:
        :param campaign_prefix:
        :param rate_field:
        :return: float/none
        """
        goal_type_id = getattr(obj, f"{campaign_prefix}salesforce_placement__goal_type_id")
        ordered_rate = getattr(obj, rate_field) if rate_field \
            else getattr(obj, f"{campaign_prefix}salesforce_placement__ordered_rate")

        # validate
        if ordered_rate is None or goal_type_id is None:
            return None
        try:
            ordered_rate = float(ordered_rate)
        except ValueError:
            return None

        if goal_type_id == SalesForceGoalType.CPV:
            return round(ordered_rate * obj.video_views, 2)
        if goal_type_id == SalesForceGoalType.CPM:
            return round(ordered_rate * obj.impressions / 1000, 2)
        return None
