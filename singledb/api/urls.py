from django.conf.urls import url

from ..settings import SLAVE_MODE
from .views import ChannelListApiView

if SLAVE_MODE:
    from .slave_views import ChannelListApiView
    from .slave_views import ChannelRetrieveUpdateDeleteApiView
    from .slave_views import VideoListApiView
    from .slave_views import VideoRetrieveUpdateDeleteApiView
else:
    from .views import ChannelRetrieveUpdateDeleteApiView
    from .views import VideoListApiView
    from .views import VideoRetrieveUpdateDeleteApiView


urlpatterns = [
    url(r'^channels/$', ChannelListApiView.as_view(), name="channels_list"),
    url(r'^channels/(?P<pk>[\w-]+)/$', ChannelRetrieveUpdateDeleteApiView.as_view(), name='channel'),

    url(r'^videos/$', VideoListApiView.as_view(), name="videos_list"),
    url(r'^videos/(?P<pk>[\w-]+)/$', VideoRetrieveUpdateDeleteApiView.as_view(), name='video'),
]
