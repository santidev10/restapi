"""
Audit individual Channel and Video items separate from audits and segments
"""

from django.http.response import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.api.serializers.audit_channel_vet_serializer import AuditChannelVetSerializer
from audit_tool.api.serializers.audit_video_vet_serializer import AuditVideoVetSerializer
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from userprofile.constants import StaticPermissions
from utils.views import validate_fields


class AuditItemRetrieveUpdateAPIView(APIView):
    ES_SECTIONS = (Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.MONETIZATION)
    REQUIRED_FIELDS = ("age_group", "brand_safety", "content_type", "gender", "iab_categories", "language")

    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.RESEARCH__VETTING),
    )

    def get(self, request, *args, **kwargs):
        """
        Retrieve item to audit
        """
        doc_id = kwargs["pk"]
        es_manager, serializer, _, _ = self._get_config(doc_id, sections=self.ES_SECTIONS)
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
        es_manager, serializer, vetting_model, audit_model = self._get_config(doc_id, sections=[Sections.TASK_US_DATA])
        vet_obj = self._create_vet(audit_model, vetting_model, doc_id)
        serializer = serializer(vet_obj, data=data, context={"user": request.user})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response()

    def _get_config(self, doc_id, sections=None):
        sections = sections if sections else self.ES_SECTIONS
        if len(doc_id) < 20:
            es_manager = VideoManager(sections=sections)
            serializer = AuditVideoVetSerializer
            vetting_model = AuditVideoVet
            audit_model = AuditVideo
        else:
            es_manager = ChannelManager(sections=sections)
            serializer = AuditChannelVetSerializer
            vetting_model = AuditChannelVet
            audit_model = AuditChannel
        return es_manager, serializer, vetting_model, audit_model

    def _create_vet(self, audit_model, vetting_model, item_id):
        if "video" in audit_model.__name__.lower():
            item_type = "video"
        else:
            item_type = "channel"
        audit_obj, _ = audit_model.objects.get_or_create(**{f"{item_type}_id": item_id})
        vet_obj = vetting_model.objects.create(**{
            item_type: audit_obj,
            "audit_id": None,
        })
        return vet_obj
