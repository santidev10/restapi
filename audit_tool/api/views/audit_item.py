"""
Audit individual Channel and Video items separate from audits and segments
"""

from django.http.response import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.api.serializers.audit_channel_vet_serializer import AuditChannelVetSerializer
from audit_tool.api.serializers.audit_video_vet_serializer import AuditVideoVetSerializer
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.views import validate_fields


class AuditItemRetrieveUpdateAPIView(APIView):
    ES_SECTIONS = (Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.MONETIZATION)
    REQUIRED_FIELDS = ("age_group", "brand_safety", "content_type", "gender", "iab_categories",
                       "is_monetizable", "language")

    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.vet_audit"),
            user_has_permission("userprofile.vet_audit_admin")
        ),
    )

    def get(self, request, *args, **kwargs):
        """
        Retrieve item to audit
        """
        doc_id = kwargs["pk"]
        es_manager, serializer = self._get_config(doc_id, sections=self.ES_SECTIONS)
        try:
            doc = es_manager.get([doc_id], skip_none=True)[0]
        except IndexError:
            raise Http404
        data = serializer(doc).data
        return Response(data=data)

    def patch(self, request, *args, **kwargs):
        doc_id = kwargs["pk"]
        data = request.data
        validate_fields(self.REQUIRED_FIELDS, list(data.keys()))
        data["lang_code"] = data["language"]
        es_manager, serializer = self._get_config(doc_id, sections=[Sections.TASK_US_DATA])
        serializer = serializer(data=data, context={"user": request.user})
        serializer.is_valid(raise_exception=True)
        brand_safety = serializer.save_brand_safety(doc_id)
        serializer.save_elasticsearch(doc_id, brand_safety)
        return Response()

    def _get_config(self, doc_id, sections=None):
        sections = sections if sections else self.ES_SECTIONS
        if len(doc_id) < 20:
            es_manager = VideoManager(sections=sections)
            serializer = AuditVideoVetSerializer
        else:
            es_manager = ChannelManager(sections=sections)
            serializer = AuditChannelVetSerializer
        return es_manager, serializer
