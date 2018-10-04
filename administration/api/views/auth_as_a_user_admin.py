from django.conf import settings
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer as RegularUserSerializer
from userprofile.models import UserProfile


class AuthAsAUserAdminApiView(APIView):
    """
    Login as a user endpoint
    """
    permission_classes = (IsAdminUser,)

    def get(self, request, pk):
        """
        Get the selected user and return its data
        """
        try:
            user = UserProfile.objects.get(pk=pk)
        except UserProfile.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        Token.objects.get_or_create(user=user)
        response_data = RegularUserSerializer(user).data
        custom_auth_flags = settings.CUSTOM_AUTH_FLAGS.get(user.email.lower())
        if custom_auth_flags:
            for name, value in custom_auth_flags.items():
                response_data[name] = value
        return Response(response_data)