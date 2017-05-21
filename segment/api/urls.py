"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.views import SegmentListCreateApiView,\
    SegmentRetrieveUpdateDeleteApiView

urlpatterns = [
    url(r'^segments/$', SegmentListCreateApiView.as_view(),
        name="segment_list"),
    url(r'^segments/(?P<pk>\d+)/$',
        SegmentRetrieveUpdateDeleteApiView.as_view(),
        name="segment_list"),
]
