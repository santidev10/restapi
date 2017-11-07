"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.views import SegmentDuplicateApiView
from segment.api.views import SegmentListCreateApiView
from segment.api.views import SegmentRetrieveUpdateDeleteApiView
from segment.api.views import SegmentSuggestedChannelApiView
from segment.models import SEGMENT_TYPES


segment_types = '|'.join(SEGMENT_TYPES.fget())

urlpatterns = [
    url(r'^segments/(?P<segment_type>{})/$'.format(segment_types),
        SegmentListCreateApiView.as_view(),
        name="segment_list"),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        SegmentRetrieveUpdateDeleteApiView.as_view(),
        name="segment_details"),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/duplicate/$'.format(segment_types),
        SegmentDuplicateApiView.as_view(),
        name="segment_duplicate"),
    url(r'^segments/(?P<segment_type>{})/suggested_channels/(?P<pk>\d+)/$'.format(segment_types),
        SegmentSuggestedChannelApiView.as_view(),
        name="segment_duplicate"),
]
