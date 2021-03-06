from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.views import APIView

from userprofile.constants import StaticPermissions
from utils.celery.dmp_celery import send_task_delete_channels


class ChannelSetApiView(APIView, PermissionRequiredMixin):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.ADMIN),
    )

    def delete(self, request, *args, **kwargs):
        channel_ids = request.data.get("delete", [])
        send_task_delete_channels((channel_ids,))
        return Response()
