from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView


class StatusApiView(APIView):
    permission_classes = tuple()

    def get(self, request):
        response_status = request.query_params.get("echo_status", HTTP_200_OK)
        return Response(status=response_status)
