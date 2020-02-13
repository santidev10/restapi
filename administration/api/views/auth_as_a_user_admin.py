from django.conf import settings
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer as RegularUserSerializer
from userprofile.models import UserProfile
from userprofile.models import UserDeviceToken


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
        if user.is_superuser:
            return Response(status=HTTP_403_FORBIDDEN, data="You do not have permission to perform this action.")
        response_data = RegularUserSerializer(user).data
        custom_auth_flags = settings.CUSTOM_AUTH_FLAGS.get(user.email.lower())
        if custom_auth_flags:
            for name, value in custom_auth_flags.items():
                response_data[name] = value
        return Response(response_data)

    def finalize_response(self, request, response, *args, **kwargs):
        """
        Set new UserDeviceToken to set appropriate permissions for subsequent requests
        """
        if response.status_code == 200:
            device_token = UserDeviceToken.objects.create(user_id=response.data["id"])
            response.data.update({
                "token": device_token.key,
                "device_id": device_token.device_id,
            })
        return super().finalize_response(request, response, *args, **kwargs)
