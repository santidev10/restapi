from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_406_NOT_ACCEPTABLE
from rest_framework.views import APIView

from userprofile.api.serializers import UserChangePasswordSerializer


class UserPasswordChangeApiView(APIView):
    """
    Endpoint for changing user's password
    """
    serializer_class = UserChangePasswordSerializer

    def post(self, request):
        """
        Update user password
        """
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)
        user = request.user
        if user.is_superuser:
            return Response(status=HTTP_406_NOT_ACCEPTABLE)
        old_password = serializer.data.get("old_password")
        new_password = serializer.data.get("new_password")

        if not user.check_password(old_password):
            return Response(status=HTTP_403_FORBIDDEN)

        user.set_password(new_password)
        user.save()
        return Response(status=HTTP_202_ACCEPTED)
