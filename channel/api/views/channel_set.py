from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.views import APIView

from es_components.connections import init_es_connection
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.celery.dmp_celery import send_task_delete_channels

init_es_connection()


class ChannelSetApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)

    def delete(self, request, *args, **kwargs):
        channel_ids = request.data.get("delete", [])
        send_task_delete_channels((channel_ids,))
        return Response()
