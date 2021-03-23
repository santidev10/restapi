"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api import views
from segment.api.urls.names import Name
from segment.models.persistent.constants import PersistentSegmentType


segment_types = f"{PersistentSegmentType.CHANNEL}|{PersistentSegmentType.VIDEO}"

urlpatterns = [
    # persistent_segments
    url(r"^persistent_segments/(?P<segment_type>{})/$".format(segment_types),
        views.CustomSegmentListApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_LIST),
    url(r"^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/$".format(segment_types),
        views.PersistentSegmentRetrieveApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_DETAILS),
    url(r"^persistent_segments_export/(?P<segment_type>{})/(?P<pk>\d+)/$".format(segment_types),
        views.PersistentSegmentExportApiView.as_view(),
        name=Name.PERSISTENT_SEGMENT_EXPORT),
    url(r"^persistent_segments/(?P<segment_type>{})/(?P<pk>\d+)/preview/$".format(segment_types),
        views.SegmentPreviewAPIView.as_view(),
        name=Name.PERSISTENT_SEGMENT_PREVIEW),
]

urlpatterns_v2 = [
    url(r"segments/(?P<pk>\d+)/$",
        views.CustomSegmentUpdateApiView.as_view(),
        name=Name.CUSTOM_SEGMENT_UPDATE),

    url(r"^segments/options/$",
        views.SegmentCreateOptionsApiView.as_view(),
        name=Name.SEGMENT_CREATION_OPTIONS),

    url(r"^segments/create/$",
        views.SegmentCreateUpdateApiView.as_view(),
        name=Name.SEGMENT_CREATE),

    url(r"^segments/list/$",
        views.SegmentListApiView.as_view(),
        name=Name.SEGMENT_LIST),

    url(r"^segments/(?P<segment_type>{})/(?P<pk>\d+)/$".format(segment_types),
        views.SegmentDeleteApiView.as_view(),
        name=Name.SEGMENT_DELETE),

    url(r"^segments/export/(?P<pk>\d+)/$",
        views.SegmentExport.as_view(),
        name=Name.SEGMENT_EXPORT),

    url(r"^segments/(?P<segment_type>{})/(?P<pk>.+)/preview/$".format(segment_types),
        views.SegmentPreviewAPIView.as_view(),
        name=Name.SEGMENT_PREVIEW),

    url(r"^segments/sync/(?P<pk>.+)/$".format(segment_types),
        views.SegmentSyncAPIView.as_view(),
        name=Name.SEGMENT_SYNC),

    url(r"^segments/oauth/$".format(segment_types),
        views.SegmentOAuthAPIView.as_view(),
        name=Name.SEGMENT_OAUTH),
]

