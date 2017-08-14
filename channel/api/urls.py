"""
Chanel api urls module
"""
from django.conf.urls import url

from channel.api.views import ChannelListApiView
from channel.api.views import ChannelListFiltersApiView
from channel.api.views import ChannelRetrieveUpdateApiView
from channel.api.views import ChannelSetApiView
from channel.api.views import ChannelsVideosByKeywords

urlpatterns = [
    url(r'^channels/$', ChannelListApiView.as_view(), name="channel_list"),
    url(r'^channels/filters/$', ChannelListFiltersApiView.as_view(), name='channel_filters'),
    url(r'^channels/(?P<pk>[\w-]+)/$', ChannelRetrieveUpdateApiView.as_view(), name='channel'),
    url(r'^channel_set/$', ChannelSetApiView.as_view(), name="channel_set"),
    url(r'^channels/video_by_keyword/(?P<keyword>[\w-]+)/$', ChannelsVideosByKeywords.as_view(), name='channel_keyword_videos'),
]
