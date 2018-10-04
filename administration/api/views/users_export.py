from rest_framework.response import Response
from rest_framework.views import APIView


class UsersExport(APIView):
    def get(self):
        return Response()
