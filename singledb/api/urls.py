from django.conf.urls import url

from singledb.settings import SLAVE_MODE
from singledb.api.views import ChannelListApiView

if SLAVE_MODE:
    from singledb.api.slave_views import ChannelListApiView
    from singledb.api.slave_views import ChannelListFiltersApiView
    from singledb.api.slave_views import ChannelRetrieveUpdateDeleteApiView

    from singledb.api.slave_views import VideoListApiView
    from singledb.api.slave_views import VideoListFiltersApiView
    from singledb.api.slave_views import VideoRetrieveUpdateDeleteApiView
else:
    from singledb.api.views import ChannelListApiView
    from singledb.api.views import ChannelListFiltersApiView
    from singledb.api.views import ChannelRetrieveUpdateDeleteApiView

    from singledb.api.views import VideoListApiView
    from singledb.api.views import VideoListFiltersApiView
    from singledb.api.views import VideoRetrieveUpdateDeleteApiView


urlpatterns = [
    url(r'^channels/$', ChannelListApiView.as_view(), name="channels_list"),
    url(r'^channels/filters/$', ChannelListFiltersApiView.as_view(), name='channel_filters'),
    url(r'^channels/(?P<pk>[\w-]+)/$', ChannelRetrieveUpdateDeleteApiView.as_view(), name='channel'),

    url(r'^videos/$', VideoListApiView.as_view(), name="videos_list"),
    url(r'^videos/filters/$', VideoListFiltersApiView.as_view(), name="video_filters"),
    url(r'^videos/(?P<pk>[\w-]+)/$', VideoRetrieveUpdateDeleteApiView.as_view(), name='video'),
]
