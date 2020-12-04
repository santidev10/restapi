from rest_framework.exceptions import NotFound
from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import UpdateAPIView

from performiq.api.serializers import OAuthAccountSerializer
from performiq.models import OAuthAccount


class PerformIQOAuthAccountUpdateAPIView(UpdateAPIView):

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