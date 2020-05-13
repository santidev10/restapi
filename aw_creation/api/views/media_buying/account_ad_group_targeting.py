from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import AdGroupTargeting
from aw_creation.api.views.media_buying.utils import get_account_creation
from utils.views import get_object


class AccountAdGroupTargetingAPIView(APIView):

    def patch(self, request, *args, **kwargs):
        account_creation_id = kwargs["account_id"]
        data = request.data
        targeting_id = kwargs["ad_group_targeting_id"]
        get_account_creation(request.user, account_creation_id)
        targeting = get_object(AdGroupTargeting, id=targeting_id)
        targeting.status = data["targeting_status"]
        targeting.save()
        return Response(targeting.id)
