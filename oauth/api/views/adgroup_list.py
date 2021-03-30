from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import AdGroupSerializer
from oauth.models import AdGroup
from oauth.utils.view import validate_oath_type


class OAuthAdGroupListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = AdGroupSerializer

    def get_queryset(self):
        oauth_type = validate_oath_type(self.request)
        parent_id = self.request.query_params.get("parent_id")
        filters = {
            "campaign__account__oauth_accounts__user": self.request.user,
            "oauth_type": oauth_type,
        }
        if parent_id is not None:
            filters["campaign_id"] = parent_id
        qs = AdGroup.objects.filter(**filters)
        return qs
