from collections import defaultdict

from django.db.models import F
from django.db.models import Q
from django.utils import timezone
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Account
from aw_reporting.models import Campaign


class PacingReportFlightsCampaignAllocationsChangedView(APIView):
    permission_classes = tuple()

    def get(self, request, *_, **kwargs):
        """
        Retrieves all updated account campaigns under request mcc_account for syncing on Adwords
        :param request: request["pk"] -> (str) mcc_account_id
        :param _: None
        :param kwargs: None
        :return: (dict) Updated campaign budgets
        """
        mcc_account_id = kwargs.pop("pk")
        managed_accounts = Account \
            .objects \
            .filter(managers__id=mcc_account_id) \
            .distinct("pk") \
            .values_list("id", flat=True)
        now = timezone.now()
        running_campaigns = Campaign.objects \
            .filter(
            Q(sync_time__lte=F("update_time")) | Q(sync_time=None),
            salesforce_placement__start__lte=now,
            salesforce_placement__end__gte=now,
        )

        all_updated_campaign_budgets = defaultdict(dict)
        for campaign in running_campaigns:
            account_id = campaign.account.id
            # Ignore campaigns whose account is not managed by mcc_account_id
            if account_id not in managed_accounts:
                continue
            all_updated_campaign_budgets[account_id][campaign.id] = campaign.budget
        return Response(all_updated_campaign_budgets)
