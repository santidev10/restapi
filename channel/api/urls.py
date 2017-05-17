"""
Chanel api urls module
"""
from django.conf.urls import url

from channel.api.views import ChannelListApiView

urlpatterns = [
    url(r'^channels/$', ChannelListApiView.as_view(), name="channel_list"),
]
