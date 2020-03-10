from rest_framework.response import Response
from rest_framework.views import APIView

from userprofile.constants import DEFAULT_DOMAIN
from userprofile.models import WhiteLabel


class WhiteLabelApiView(APIView):
    permission_classes = ()

    def get(self, request):
        domain = (request.get_host() or DEFAULT_DOMAIN).lower()
        # Remove top level domain
        sub_domain = domain.rsplit(".", 1)[0]
        data = WhiteLabel.get(domain=sub_domain).config
        return Response(data)
