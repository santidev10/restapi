"""
Video api urls module
"""
from django.conf.urls import url

from video.api.views import VideoListApiView
from video.api.views import VideoListExportApiView
from video.api.views import VideoRetrieveUpdateApiView
from video.api.views import VideoSetApiView
from .names import Name

urlpatterns = [
    url(r"^videos/$", VideoListApiView.as_view(), name=Name.VIDEO_LIST),
    url(r"^videos/export/$", VideoListExportApiView.as_view(), name=Name.VIDEO_PREPARE_EXPORT),
    url(r"^videos/export/(?P<export_name>.+)/$", VideoListExportApiView.as_view(), name=Name.VIDEO_EXPORT),
    url(r"^videos/(?P<pk>[\w-]+)/$", VideoRetrieveUpdateApiView.as_view(), name=Name.VIDEO),
    url(r"^video_set/$", VideoSetApiView.as_view(), name="video_set"),
]
