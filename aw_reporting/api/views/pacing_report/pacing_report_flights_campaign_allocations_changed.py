from rest_framework.views import APIView
from rest_framework.response import Response
from aw_reporting.models import Campaign
from aw_reporting.models import Account
from django.utils import timezone

class PacingReportFlightsCampaignAllocationsChangedView(APIView):
    permission_classes = tuple()

    def get(self, request, *_, **kwargs):
        """
        Retrieves all updated account campaigns under request mcc_account for syncing on Adwords
        :param request: request['pk'] -> (str) mcc_account_id
        :param _: None
        :param kwargs: None
        :return: (dict) Updated campaign budgets
        """

        mcc_account_id = kwargs.pop('pk')
        managed_accounts = Account \
            .objects \
            .filter(managers__id=mcc_account_id) \
            .distinct("pk") \
            .values_list('id', flat=True)

        now = timezone.now()
        running_campaigns = Campaign.objects.filter(salesforce_placement__start__lte=now, salesforce_placement__end__gte=now)
        all_updated_campaign_budgets = {}

        for campaign in running_campaigns:
            account_id = campaign.account.id

            # Skip over campaigns whose account is not managed by mcc_account_id or budget that has not been set in viewiq
            if account_id not in managed_accounts or campaign.goal_allocation <= 0:
                continue

            if not all_updated_campaign_budgets.get(account_id):
                all_updated_campaign_budgets[account_id] = {}
                all_updated_campaign_budgets[account_id][campaign.id] = campaign.budget

            else:
                all_updated_campaign_budgets[account_id][campaign.id] = campaign.budget

        return Response(all_updated_campaign_budgets)
