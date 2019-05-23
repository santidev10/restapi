from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor

from django.conf import settings
from audit_tool.api.views.audit_save import AuditFileS3Exporter

class AuditExportApiView(APIView):
    pass
