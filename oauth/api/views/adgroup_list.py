from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.constants import OAuthType
from oauth.api.serializers import AdGroupSerializer
from oauth.models import AdGroup
from oauth.utils.view import validate_oath_type


class OAuthAdGroupListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = AdGroupSerializer
    queryset = AdGroup.objects.all()

    def filter_queryset(self, queryset):
        oauth_type = validate_oath_type(self.request)
        parent_id = self.request.query_params.get("parent_id")
        filter_config = self._get_filter_config(oauth_type)
        filters = {
            filter_config["user"]: self.request.user,
            "oauth_type": oauth_type,
        }
        if parent_id:
            filters[filter_config["parent"]] = parent_id
        qs = queryset.filter(**filters)
        return qs

    def _get_filter_config(self, oauth_type):
        config = {
            OAuthType.GOOGLE_ADS.value: {
                "parent": "campaign_id",
                "user": "campaign__account__oauth_accounts__user",
            },
            OAuthType.DV360.value: {
                "parent": "line_item_id",
                "user": "line_item__insertion_order__campaign__advertiser__oauth_accounts__user",
            }
        }
        return config[oauth_type]
