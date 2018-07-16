"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.urls.names import Name
from segment.api.views import SegmentDuplicateApiView
from segment.api.views import SegmentListCreateApiView
from segment.api.views import SegmentRetrieveUpdateDeleteApiView
from segment.api.views import SegmentShareApiView
from segment.api.views import SegmentSuggestedChannelApiView
from segment.utils import SEGMENT_TYPES

segment_types = '|'.join(SEGMENT_TYPES.fget())

urlpatterns = [
    url(r'^segments/(?P<segment_type>{})/$'.format(segment_types),
        SegmentListCreateApiView.as_view(),
        name=Name.SEGMENT_LIST),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        SegmentRetrieveUpdateDeleteApiView.as_view(),
        name="segment_details"),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/share/$'.format(
        segment_types),
        SegmentShareApiView.as_view(),
        name="segment_share"),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/duplicate/$'.format(
        segment_types),
        SegmentDuplicateApiView.as_view(),
        name="segment_duplicate"),
    url(
        r'^segments/(?P<segment_type>{})/suggested_channels/(?P<pk>\d+)/$'.format(
            segment_types),
        SegmentSuggestedChannelApiView.as_view(),
        name="suggested_channels"),
]
