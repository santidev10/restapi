from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import CriteriaTypeEnum
from aw_reporting.models import TargetingStatusEnum
from aw_creation.api.views.media_buying.utils import get_account_creation
from utils.views import get_object


class AccountAdGroupTargetingAPIView(APIView):
    EXCLUSION_TARGETING = (CriteriaTypeEnum.AGE_RANGE.value, CriteriaTypeEnum.GENDER.value, CriteriaTypeEnum.PARENT.value)

    def patch(self, request, *args, **kwargs):
        account_creation_id = kwargs["account_id"]
        data = request.data
        targeting_id = kwargs["ad_group_targeting_id"]
        get_account_creation(request.user, account_creation_id)
        targeting = get_object(AdGroupTargeting, id=targeting_id)
        self._validate_update_status(targeting, data["targeting_status"])
        return Response(targeting.id)

    def _validate_update_status(self, targeting, updated_status):
        # exclude
        if updated_status == TargetingStatusEnum.EXCLUDED.name:
            if targeting.type_id not in self.EXCLUSION_TARGETING:
                raise ValidationError(f"You can only enable or exclude this type of targeting: {str(targeting.type)}.")
            if targeting.is_negative is True:
                raise ValidationError("Targeting is already excluded.")

            targeting.is_negative = True
        # enable
        elif updated_status == TargetingStatusEnum.ENABLED.name:
            # Include targeting that is excluded
            if targeting.type_id in self.EXCLUSION_TARGETING:
                targeting.is_negative = False
            elif targeting.status == TargetingStatusEnum.ENABLED.value:
                raise ValidationError("Targeting is already enabled.")
            # Enable paused targeting
            else:
                targeting.status = TargetingStatusEnum.ENABLED.value
        # pause
        else:
            if targeting.type_id in self.EXCLUSION_TARGETING:
                raise ValidationError(f"You can only enable or exclude this type of targeting: {str(targeting.type)}")
            if targeting.status == TargetingStatusEnum.PAUSED.value:
                raise ValidationError("Targeting is already paused.")
            targeting.status = TargetingStatusEnum.PAUSED.value
        targeting.sync_pending = True
        targeting.save()
