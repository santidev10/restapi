from rest_framework.generics import UpdateAPIView

from performiq.api.serializers import OAuthAccountSerializer
from performiq.models import OAuthAccount


class PerformIQOAuthAccountUpdateAPIView(UpdateAPIView):

    serializer_class = OAuthAccountSerializer

    def get_object(self):
        oauth_account_id = self.kwargs.get("pk")
        return OAuthAccount.objects.get(id=oauth_account_id, user=self.request.user)