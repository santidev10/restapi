from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_406_NOT_ACCEPTABLE
from rest_framework.views import APIView

from userprofile.api.serializers import UserSetPasswordSerializer


class UserPasswordSetApiView(APIView):
    """
    Endpoint for setting new password for user
    """
    permission_classes = tuple()
    serializer_class = UserSetPasswordSerializer

    def post(self, request):
        """
        Update user password
        """
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)
        email = serializer.data.get("email")
        token = serializer.data.get("token")
        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        if not PasswordResetTokenGenerator().check_token(user, token):
            return Response({"error": "Invalid link"}, HTTP_400_BAD_REQUEST)
        if user.is_superuser:
            return Response(status=HTTP_406_NOT_ACCEPTABLE)
        user.set_password(serializer.data.get("new_password"))
        user.save()
        try:
            token = Token.objects.get(user=user)
        except Token.DoesNotExist:
            return Response(status=HTTP_202_ACCEPTED)
        else:
            token.delete()
        return Response(status=HTTP_202_ACCEPTED)
