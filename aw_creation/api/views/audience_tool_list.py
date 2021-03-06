from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import AudienceHierarchySerializer
from aw_reporting.models import Audience
from userprofile.constants import StaticPermissions


class AudienceToolListApiView(ListAPIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE),)
    serializer_class = AudienceHierarchySerializer
    queryset = Audience.objects.filter(
        parent__isnull=True,
        type__in=[Audience.AFFINITY_TYPE, Audience.IN_MARKET_TYPE],
    ).order_by("type", "name")
