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
from segment.api.views import PersistentMasterSegmentsListApiView
from segment.api.views import PersistentSegmentExportApiView
from segment.api.views import PersistentSegmentListApiView
from segment.api.views import PersistentSegmentRetrieveApiView
from segment.api.views import PersistentSegmentPreviewAPIView
from segment.utils import SEGMENT_TYPES
from segment.utils import PERSISTENT_SEGMENT_TYPES

segment_types = '|'.join(SEGMENT_TYPES.fget())
persistent_segment_types = '|'.join(PERSISTENT_SEGMENT_TYPES.fget())

urlpatterns = [
    # segments
    url(r'^segments/(?P<segment_type>{})/$'.format(segment_types),
        SegmentListCreateApiView.as_view(),
        name=Name.SEGMENT_LIST),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        SegmentRetrieveUpdateDeleteApiView.as_view(),
        name="segment_details"),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/share/$'.format(segment_types),
        SegmentShareApiView.as_view(),
        name=Name.SEGMENT_SHARE),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/duplicate/$'.format(segment_types),
        SegmentDuplicateApiView.as_view(),
        name=Name.SEGMENT_DUPLICATE),
    url(r'^segments/(?P<segment_type>{})/suggested_channels/(?P<pk>\d+)/$'.format(segment_types),
        SegmentSuggestedChannelApiView.as_view(),
        name="suggested_channels"),

    # persistent_segments
    url(r'^persistent_segments/(?P<segment_type>{})/$'.format(persistent_segment_types),
        PersistentSegmentListApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_LIST),
    url(r'^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(persistent_segment_types),
        PersistentSegmentRetrieveApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_DETAILS),
    url(r'^persistent_segments_export/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(persistent_segment_types),
        PersistentSegmentExportApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_EXPORT),
    url(r'^persistent_master_segments/$', PersistentMasterSegmentsListApiView.as_view(),
        name=Name.PERSISTENT_MASTER_SEGMENTS_LIST),
    url(r'^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/preview$'.format(persistent_segment_types),
        PersistentSegmentPreviewAPIView.as_view(),
        name=Name.PERSISTENT_SEGMENT_PREVIEW),
]
