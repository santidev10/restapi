"""
Video api views module
"""
from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.views import APIView
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete
from utils.celery.dmp_celery import send_task_delete_videos


from es_components.connections import init_es_connection


init_es_connection()


class VideoSetApiView(APIView, PermissionRequiredMixin):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)

    def delete(self, request, *args, **kwargs):
        video_ids = request.data.get("delete", [])
        send_task_delete_videos((video_ids,))
        return Response()