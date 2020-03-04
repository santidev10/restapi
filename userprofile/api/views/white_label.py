from rest_framework.response import Response
from rest_framework.views import APIView

from userprofile.models import WhiteLabel


class WhiteLabelApiView(APIView):
    permission_classes = ()

    def get(self, request):
        domain = (request.get_host() or "").lower()
        data = WhiteLabel.get(domain=domain).config
        return Response(data)
