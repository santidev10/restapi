"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.views import SegmentListCreateApiView, \
    SegmentChannelListApiView

urlpatterns = [
    url(r'^segments/$', SegmentListCreateApiView.as_view(),
        name="segment_list"),
    url(r'^segments/(?P<pk>\d+)/channels/$',
        SegmentChannelListApiView.as_view(), name="segment_list"),
]
