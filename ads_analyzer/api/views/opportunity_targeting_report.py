from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from ads_analyzer.api.serializers.opportunity_target_report_payload_serializer import \
    OpportunityTargetReportPayloadSerializer
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class OpportunityTargetingReportAPIView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.create_opportunity_report"),
            IsAdminUser,
        ),
    )
    def put(self, request):
        serializer = OpportunityTargetReportPayloadSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(data=serializer.errors, status=HTTP_400_BAD_REQUEST)
        return Response(data=dict(

        ))
