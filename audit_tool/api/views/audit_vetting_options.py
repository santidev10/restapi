from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.utils.audit_utils import AuditUtils
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class AuditVettingOptionsAPIView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.vet_audit"),
            user_has_permission("userprofile.vet_audit_admin")
        ),
    )

    def get(self, request, *args, **kwargs):
        options = {
            "brand_safety_categories": AuditUtils.get_brand_safety_categories(),
            "content_categories": AuditUtils.get_iab_categories(),
            "languages": AuditUtils.get_languages(),
            "age_groups": AuditUtils.get_age_groups(),
            "channel_types": AuditUtils.get_channel_types(),
            "genders": AuditUtils.get_genders(),
        }
        response = Response(data=options)
        return response
