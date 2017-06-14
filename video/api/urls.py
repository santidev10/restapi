"""
Video api urls module
"""
from django.conf.urls import url

from video.api.views import VideoListApiView
from video.api.views import VideoListFiltersApiView
from video.api.views import VideoRetrieveUpdateApiView
from video.api.views import VideoSetApiView

urlpatterns = [
    url(r'^videos/$', VideoListApiView.as_view(), name="video_list"),
    url(r'^videos/filters/$', VideoListFiltersApiView.as_view(), name='video_filters'),
    url(r'^videos/(?P<pk>[\w-]+)/$', VideoRetrieveUpdateApiView.as_view(), name='video'),
    url(r'^video_set/$', VideoSetApiView.as_view(), name="video_set"),
]
