from rest_framework.exceptions import NotFound
from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import UpdateAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import OAuthAccountSerializer
from oauth.models import OAuthAccount


class OAuthAccountUpdateAPIView(UpdateAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = OAuthAccountSerializer

    def get_object(self):
        oauth_account_id = self.kwargs.get("pk")
        try:
            oauth_account = OAuthAccount.objects.get(id=oauth_account_id)
        except OAuthAccount.DoesNotExist:
            raise NotFound
        if oauth_account.user != self.request.user:
            raise PermissionDenied("You do not have permission to modify this resource!")
        return oauth_account
