from rest_framework.generics import UpdateAPIView
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from aw_reporting.models import Account
from django.db.models import F
import datetime

class PacingReportFlightsCampaignAllocationsChangedView(APIView):

    def get(self, request, *_, **kwargs) -> list:
        """
        Retrieves all updated account campaigns under request mcc_account for syncing on adwords
        :param request: pk -> mcc_account_id
        :param _: None
        :param kwargs: None
        :return: list
        """
        mcc_account_id = kwargs.pop('pk')

        cid_accounts = self.get_managed_accounts(mcc_account_id=mcc_account_id)
        campaign_budgets = self.get_campaign_budgets(accounts=cid_accounts)

        all_updated_campaign_budgets = {
            'accountIds': cid_accounts.values_list('id', flat=True),
            'campaignBudgets': campaign_budgets,
        }

        return Response(all_updated_campaign_budgets)

    def get_managed_accounts(self, mcc_account_id=None) -> 'queryset':
        """
        Retrieves all accounts managed by mcc account. Excludes accounts that have already been synced with adwords
        :param mcc_account_id: mcc account id to retrieve managed accounts for
        :return: query_set of all managed accounts
        """
        if mcc_account_id is None:
            raise ValueError('Must provide mcc account id.')

        managed_accounts = Account.objects \
            .filter(managers__id=mcc_account_id) \
            .distinct("pk") \
            .exclude(is_active=False) \
            .exclude(update_time=None) \
            .exclude(hourly_updated_at__gte=F("update_time")) \

        return managed_accounts

    def get_campaign_budgets(self, accounts=None) -> dict:
        """
        Uses _campaigns_generator to produce campaigns for the current account being processed
        :param accounts: List of accounts to get campaigns for
        :return: dict -> campaign_id: campaign_budget
        """
        if accounts is None:
            raise ValueError('Must provide accounts to query')

        campaign_generator = self._campaigns_generator(accounts=accounts)
        campaigns = next(campaign_generator)
        campaign_budgets = {}

        while True:
            if not campaigns:
                break

            for campaign in campaigns:
                campaign_id = campaign[0]
                campaign_budget = campaign[1]

                campaign_budgets[campaign_id] = campaign_budget

            try:
                campaigns = next(campaign_generator)

            except StopIteration:
                break

        return campaign_budgets

    def _campaigns_generator(self, accounts=None) -> iter:
        """
        Generator to yield campaigns for each account
        :param accounts:
        :return:
        """
        for account in accounts:
            campaigns = account.campaigns \
                .filter(status='eligible') \
                .values_list('id', 'goal_allocation', 'account') \

            if not campaigns:
                continue

            yield campaigns






