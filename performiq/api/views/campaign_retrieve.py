from django.contrib.auth import get_user_model
from rest_framework import permissions
from rest_framework.generics import RetrieveAPIView

from performiq.api.serializers import IQCampaignSerializer
from performiq.models import IQCampaign
from userprofile.constants import StaticPermissions
from utils.views import get_object


class PerformIQAnalysisOwnerPermission(permissions.BasePermission):
    """ Check permissions and if user created requested analysis """
    def has_permission(self, request, view):
        analysis = get_object(IQCampaign, id=view.kwargs["pk"], message="Analysis not found")
        has_permission = isinstance(request.user, get_user_model()) \
            and request.user.has_permission(StaticPermissions.PERFORMIQ) and analysis.user == request.user
        return has_permission


class PerformIQCampaignRetrieveAPIView(RetrieveAPIView):
    queryset = IQCampaign.objects.all()
    serializer_class = IQCampaignSerializer
    permission_classes = (
        PerformIQAnalysisOwnerPermission,
    )
