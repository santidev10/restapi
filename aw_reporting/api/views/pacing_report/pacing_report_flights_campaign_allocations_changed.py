from rest_framework.views import APIView
from rest_framework.response import Response
from aw_reporting.models import OpPlacement
from aw_reporting.models import Account


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

        managed_accounts_ref = {
            account_id: True
            for account_id in managed_accounts
        }

        running_placements = OpPlacement.objects.filter(start__lte=now, end__gte=now)
        running_campaigns = []

        all_updated_campaign_budgets = {
            'accounts': {}
        }

        for placement in running_placements:
            running_campaigns.extend(placement.adwords_campaigns.all())

        for campaign in running_campaigns:
            account_id = campaign.account.id

            # Skip over campaigns whose account is not managed by mcc_account_id or budget that has not been set in viewiq
            if not managed_accounts_ref.get(account_id) or not campaign.budget:
                continue

            if not all_updated_campaign_budgets['accounts'].get(account_id):
                all_updated_campaign_budgets['accounts'][account_id] = {}
                all_updated_campaign_budgets['accounts'][account_id][campaign.id] = campaign.budget

            else:
                all_updated_campaign_budgets['accounts'][account_id][campaign.id] = campaign.budget

        return Response(all_updated_campaign_budgets)
