from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.utils.audit_utils import AuditUtils
from userprofile.constants import StaticPermissions


class AuditVettingOptionsAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_VET, StaticPermissions.BUILD__CTL_VET_ADMIN),
    )

    def get(self, request, *args, **kwargs):
        options = {
            "brand_safety_categories": AuditUtils.get_brand_safety_categories(),
            "content_categories": AuditUtils.get_iab_categories(),
            "languages": AuditUtils.get_languages(),
            "age_groups": AuditUtils.get_age_groups(),
            "channel_types": AuditUtils.get_channel_types(),
            "quality_types": AuditUtils.get_quality_types(),
            "genders": AuditUtils.get_genders(),
        }
        response = Response(data=options)
        return response
