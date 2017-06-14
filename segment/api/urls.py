"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.views import SegmentListCreateApiView,\
    SegmentRetrieveUpdateDeleteApiView, SegmentDuplicateApiView

urlpatterns = [
    url(r'^segments/$', SegmentListCreateApiView.as_view(),
        name="segment_list"),
    url(r'^segments/(?P<pk>\d+)/$',
        SegmentRetrieveUpdateDeleteApiView.as_view(),
        name="segment_details"),
    url(r'^segments/(?P<pk>\d+)/duplicate/$',
        SegmentDuplicateApiView.as_view(),
        name="segment_duplicate"),
]
