from rest_framework.generics import UpdateAPIView
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import Flight, Campaign
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.models import Account
from django.db.models import F
import datetime

class PacingReportFlightsCampaignAllocationsChangedView(APIView):

    def get(self, request, *_, **kwargs) -> list:
        """
        Retrieves all updated account campaings under request mcc_account for syncing on adwords
        :param request: pk -> mcc_account_id
        :param _: None
        :param kwargs: None
        :return: list
        """
        mcc_account_id = kwargs.pop('pk')
        cid_accounts = self.get_managed_accounts(mcc_account_id)
        all_updated_campaign_budgets = self.get_campaign_budets(accounts=cid_accounts)

        return Response(all_updated_campaign_budgets)


    def get_managed_accounts(self, mcc_account_id=None) -> 'queryset':
        """
        Retrieves all accounts managed by mcc account. Excludes accounts that have already been synced with adwords
        :param mcc_account_id: mcc account id to retrieve managed accounts for
        :return: query_set of all managed accounts
        """
        if mcc_account_id is None:
            raise ValueError('Must provide mcc account id.')

        try:
            managed_accounts = Account.objects\
                .get(id=mcc_account_id)\
                .managers\
                .all()\
                .exclude(is_active=False)\
                .exclude(update_time=None)\
                .exclude(hourly_updated_at__gte=F("update_time"))\

        except Exception as e:
            pass

        return managed_accounts or []


    def get_campaign_budets(self, accounts=None) -> list:
        """
        Retrieves all campaigns and maps them as a dictionary with their budget
        :param accounts: account ids to get campaign budgets for
        :return: list of dictionaries
        """
        all_campaigns = []

        for account in accounts:
            campaigns = account.campaigns\
                .filter(status='eligible')\
                .exclude(end_date__lte=datetime.now())\
                .values_list('id', 'goal_allocation', 'account')

            if not campaigns:
                continue

            campaigns = [{
                'id': campaign[0],
                'budget': campaign[1],
                'account': campaign[2],
            } for campaign in campaigns]

            all_campaigns.extend(campaigns)

        return all_campaigns


