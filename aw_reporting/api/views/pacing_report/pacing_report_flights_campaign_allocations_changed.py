from collections import defaultdict

from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Account
from aw_reporting.models import CampaignBudgetHistory


class PacingReportFlightsCampaignAllocationsChangedView(APIView):
    permission_classes = tuple()

    def get(self, request, *_, **kwargs):
        """
        Retrieves all updated campaign budgets under mcc account
        :param request: request["pk"] -> (str) mcc_account_id
        :param _: None
        :param kwargs: None
        :return: (dict) Updated campaign budgets
        """
        mcc_account_id = kwargs.pop("pk")
        managed_accounts = Account.objects.filter(managers__id=mcc_account_id)\
            .distinct("pk").values_list("id", flat=True)

        campaign_budget_history = CampaignBudgetHistory.objects.select_related("campaign")\
            .annotate(account_id=F("campaign__account_id"))\
            .filter(account_id__in=managed_accounts)\
            .order_by("created_at")
        by_campaign = {}
        # Set the latest change as the value for campaign id key
        for history in campaign_budget_history:
            try:
                last = by_campaign[history.campaign.id]
                if history.created_at > last.created_at:
                    by_campaign[history.campaign.id] = history
            except KeyError:
                by_campaign[history.campaign.id] = history

        updated_campaign_budgets = defaultdict(dict)
        budget_history_ids = []
        for history in by_campaign.values():
            # Do not return already synced changes
            if history.sync_at:
                continue
            updated_campaign_budgets[history.account_id][history.campaign.id] = history.budget
            budget_history_ids.append(history.id)
        payload = {
            "budget_history_ids": budget_history_ids,
            "updated_campaign_budgets": updated_campaign_budgets,
        }
        return Response(payload)
