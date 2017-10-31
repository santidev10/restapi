from django.conf.urls import url

from singledb.api.views import CountryListApiView
from singledb.api.views.augmentation import AugmentationChannelListApiView
from singledb.api.views.augmentation import AugmentationChannelSegmentListApiView
from singledb.api.views.highlights import HighlightChannelsListApiView
from singledb.api.views.highlights import HighlightVideosListApiView

urlpatterns = [
    url(r'^countries/$', CountryListApiView.as_view(), name="countries_list"),
    url(r'^augmentation_channels/$', AugmentationChannelListApiView.as_view(), name="augmentation_channels"),
    url(r'^augmentation_segments/$', AugmentationChannelSegmentListApiView.as_view(), name="augmentation_segments"),
    url(r'^highlights/channels/$', HighlightChannelsListApiView.as_view(), name="highlights_channels"),
    url(r'^highlights/videos/$', HighlightVideosListApiView.as_view(), name="highlights_videos"),
]
