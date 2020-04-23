from django.db.models import F
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.models import AccountCreation
from ads_analyzer.reports.account_targeting_report.constants import ReportType
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.api.views.media_buying.constants import TARGETING_MAPPING
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting
from aw_creation.api.serializers.media_buying.campaign_setting_serializer import CampaignSettingSerializer
from aw_creation.api.serializers.media_buying.campaign_breakout_serializer import CampaignBreakoutSerializer
from aw_creation.models import CampaignCreation
from aw_creation.models import AdGroupCreation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from utils.views import validate_fields



class AccountCampaignBreakoutAPIView(APIView):
    """
    GET: Retrieve campaign breakout details
    POST: Create breakout campaigns
    """
    def get(request, *args, **kwargs):
        from django.db.models import Subquery
        from django.db.models import OuterRef
        account_id = kwargs["account_id"]
        campaign_creations = Account.objects.get(id=account_id).account_creation.campaign_creations.all()
        sync_data = [creation.get_sync_data() for creation in campaign_creations]
        for campaign_creation in sync_data:
            campaign_creation["ad_group_ids"] = AdGroupCreation.objects.filter(campaign_creatin_id=campaign_creation["id"]).values_list("ad_group_id", flat=True)
        return Response(sync_data)
        # Get camapigns creations of type 1 under accounts
        # for each of those campaigns, get their ad groups and ads