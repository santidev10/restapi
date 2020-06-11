"""
Segment api urls endpoint
"""
from django.conf.urls import url
from segment.api.urls.names import Name
from segment.api.views import CustomSegmentListApiView
from segment.api.views import PersistentMasterSegmentsListApiView
from segment.api.views import PersistentSegmentExportApiView
from segment.api.views import PersistentSegmentPreviewAPIView
from segment.api.views import PersistentSegmentRetrieveApiView
from segment.api.views import SegmentCreateApiViewV3
from segment.api.views import SegmentCreationOptionsApiView
from segment.api.views import SegmentDeleteApiViewV2
from segment.api.views import SegmentExport
from segment.api.views import SegmentListCreateApiViewV2
from segment.api.views import SegmentPreviewAPIView
from segment.models.persistent.constants import PersistentSegmentType


segment_types = f"{PersistentSegmentType.CHANNEL}|{PersistentSegmentType.VIDEO}"

urlpatterns = [
    # persistent_segments
    url(r'^persistent_segments/(?P<segment_type>{})/$'.format(segment_types),
        CustomSegmentListApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_LIST),
    url(r'^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        PersistentSegmentRetrieveApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_DETAILS),
    url(r'^persistent_segments_export/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        PersistentSegmentExportApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_EXPORT),
    url(r'^persistent_master_segments/$', PersistentMasterSegmentsListApiView.as_view(),
        name=Name.PERSISTENT_MASTER_SEGMENTS_LIST),
    url(r'^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/preview/$'.format(segment_types),
        PersistentSegmentPreviewAPIView.as_view(),
        name=Name.PERSISTENT_SEGMENT_PREVIEW),
]

urlpatterns_v2 = [
    url(r'^segments/(?P<segment_type>{})/$'.format(segment_types),
        SegmentListCreateApiViewV2.as_view(),
        name=Name.SEGMENT_LIST),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>\d+)/$'.format(segment_types),
        SegmentDeleteApiViewV2.as_view(),
        name=Name.SEGMENT_DELETE),
    url(r'^segments/options/(?P<segment_type>{})/$'.format(segment_types),
        SegmentCreationOptionsApiView.as_view(),
        name=Name.SEGMENT_CREATION_OPTIONS),
    url(r'^segments/export/(?P<pk>\d+)/$',
        SegmentExport.as_view(),
        name=Name.SEGMENT_EXPORT),
    url(r'^segments/(?P<segment_type>{})/(?P<pk>.+)/preview/$'.format(segment_types),
        SegmentPreviewAPIView.as_view(),
        name=Name.SEGMENT_PREVIEW),
]

urlpatterns_v3 = [
    url(r'^segments/$'.format(segment_types),
        SegmentCreateApiViewV3.as_view(),
        name=Name.SEGMENT_CREATE),
    url(r'^segments/options/$',
        SegmentCreationOptionsApiView.as_view(),
        name=Name.SEGMENT_CREATION_OPTIONS),
]
