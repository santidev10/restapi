from distutils.util import strtobool

from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor


class AuditSaveApiView(APIView):
    def post(self, request):
        query_params = request.query_params
        name = query_params["name"] if "name" in query_params else None
        audit_type = int(query_params["audit_type"]) if "audit_type" in query_params else None
        source_urls = query_params["source_urls"] if "source_urls" in query_params else None
        source_file = query_params["source_file"] if "source_file" in query_params else None
        blacklist_words = query_params["blacklist_words"] if "blacklist_words" in query_params else None
        blacklist_file = query_params["blacklist_file"] if "blacklist_file" in query_params else None
        whitelist_words = query_params["whitelist_words"] if "whitelist_words" in query_params else None
        whitelist_file = query_params["whitelist_file"] if "whitelist_file" in query_params else None

        return Response()
