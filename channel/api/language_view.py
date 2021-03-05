from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from es_components.languages import LANGUAGES


class LanguageListApiView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, *args, **kwargs):
        return Response(LANGUAGES)
