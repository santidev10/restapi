from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from oauth.models import OAuthAccount


class BaseOAuthAPIView(APIView):
    def handler(self, *args, **kwargs):
        raise NotImplementedError

    def get_flow(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def delete(request, email, **_):
        try:
            oauth_account = OAuthAccount.objects.get(
                user=request.user,
                email=email
            )
            oauth_account.delete()
        except OAuthAccount.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        return Response(data=f"Deleted OAuth for email: {email}.")
