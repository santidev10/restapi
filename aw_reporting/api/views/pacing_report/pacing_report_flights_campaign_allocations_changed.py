from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.models import Account
from django.utils import timezone
from django.db.models import F

class PacingReportFlightsCampaignAllocationsChangedView(APIView):
    def get(self, request, *_, **kwargs):
        """
        Retrieves all updated account campaigns under request mcc_account for syncing on Adwords
        :param request: request['pk'] -> (str) mcc_account_id
        :param _: None
        :param kwargs: None
        :return: (dict) Updated campaign budgets
        """
        mcc_account_id = kwargs.pop('pk')

        if mcc_account_id is None:
            return Response(status=HTTP_400_BAD_REQUEST, data='Please provide an MCC Account ID.')

        # Only allow queries for MCC Accounts
        try:
            Account.objects.get(id=mcc_account_id, can_manage_clients=True)

        except Account.DoesNotExist:
            return Response(status=HTTP_400_BAD_REQUEST, data='The provided MCC Account ID was not found.')

        cid_accounts = self.get_managed_accounts(mcc_account_id=mcc_account_id)
        campaign_budgets = self.get_campaign_budgets(accounts=cid_accounts)

        all_updated_campaign_budgets = {
            'accountIds': cid_accounts.values_list('id', flat=True),
            'campaignBudgets': campaign_budgets,
            'hourlyUpdatedAt': timezone.now()
        }

        return Response(all_updated_campaign_budgets)

    def get_managed_accounts(self, mcc_account_id=None) -> Account:
        """
        Retrieves all accounts managed by mcc account. Excludes accounts that have
            already been synced with Adwords by retrieving only active accounts
            with hourly updated times less than its update_time
        :param mcc_account_id: mcc account id to retrieve managed accounts for
        :return: query_set of all managed accounts
        """
        if mcc_account_id is None:
            raise ValueError('Must provide MCC Account ID.')

        managed_accounts = Account\
            .objects \
            .filter(managers__id=mcc_account_id) \
            .distinct("pk") \
            .exclude(is_active=False) \
            .exclude(update_time=None) \
            .exclude(hourly_updated_at__gte=F("update_time")) \

        return managed_accounts

    def get_campaign_budgets(self, accounts=None) -> dict:
        """
        Uses self._campaigns_generator to produce campaigns for the current account being processed

        :param accounts: List of accounts to get campaigns for
        :return: dict -> campaign_id: campaign_budget
        """
        if accounts is None:
            raise ValueError('Must provide account to retrieve campaigns for.')

        campaign_generator = self.campaigns_generator(accounts=accounts)
        campaign_budgets = {}

        while True:
            try:
                campaigns = next(campaign_generator)

            except StopIteration:
                break

            # campaigns is yielded from self._campaigns_generator as a django values list tuple (id, goal_allocation, account)
            for campaign in campaigns:
                campaign_id = campaign.get('id')
                campaign_budget = campaign.get('budget')
                campaign_goal_allocation = campaign.get('goal_allocation')

                # Goal allocations are stored as integer percent values
                # campaign_budgets[campaign_id] = round(campaign_budget * campaign_goal_allocation / 100, 2)
                campaign_budgets[campaign_id] = campaign_goal_allocation

        return campaign_budgets

    def campaigns_generator(self, accounts=None) -> iter:
        """
        Generator to yield campaigns for each account
        
        :param accounts: (list) Adwords cid's
        :return: (list) Account campaigns
        """
        for account in accounts:
            campaigns = account\
                .campaigns \
                .filter(status='eligible') \
                .exclude(goal_allocation=0.0) \
                .values('id', 'budget', 'goal_allocation', 'account') \

            if not campaigns:
                continue

            yield campaigns






