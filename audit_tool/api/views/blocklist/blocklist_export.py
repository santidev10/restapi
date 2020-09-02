from rest_framework.views import APIView
from rest_framework.response import Response


class BlocklistExportAPIView(APIView):
    def get(self, request, *args, **kwargs):
        doc_type = request.get("type", "channel")
