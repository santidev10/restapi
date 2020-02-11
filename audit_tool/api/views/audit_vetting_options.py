from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.utils.audit_options import AuditOptions


class AuditVettingOptionsAPIView(APIView):
    def get(self, request, *args, **kwargs):
        options = {
            "brand_safety_categories": AuditOptions.get_brand_safety_categories(),
            "content_categories": AuditOptions.get_iab_categories(),
            "languages": AuditOptions.get_languages(),
            "age_groups": AuditOptions.get_age_groups(),
            "channel_types": AuditOptions.get_channel_types(),
            "genders": AuditOptions.get_genders(),
        }
        response = Response(data=options)
        return response
