"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.urls.names import Name
from segment.api.views import SegmentDeleteApiViewV2
from segment.api.views import SegmentExport
from segment.api.views import SegmentListCreateApiViewV2
from segment.api.views import SegmentCreationOptionsApiView
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
    url(r'^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/preview/$'.format(persistent_segment_types),
        PersistentSegmentPreviewAPIView.as_view(),
        name=Name.PERSISTENT_SEGMENT_PREVIEW),
]

urlpatterns_v2 = [
    url(r'^segments/(?P<segment_type>{})/$'.format(segment_types),
        SegmentListCreateApiViewV2.as_view(),
        name=Name.SEGMENT_LIST),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        SegmentDeleteApiViewV2.as_view(),
        name=Name.SEGMENT_LIST),
    url(r'^segments/options/(?P<segment_type>{})/$'.format(segment_types),
        SegmentCreationOptionsApiView.as_view(),
        name=Name.SEGMENT_CREATION_OPTIONS),
    url(r'^segments/export/(?P<pk>\d+)/$',
        SegmentExport.as_view(),
        name=Name.SEGMENT_LIST),
]
