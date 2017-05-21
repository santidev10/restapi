"""
Segment api urls endpoint
"""
from django.conf.urls import url

from segment.api.views import SegmentListCreateApiView

urlpatterns = [
    url(r'^segments/$', SegmentListCreateApiView.as_view(),
        name="segment_list"),
    # TODO add segment retrieve/update/destroy endpoint
]
