"""
Chanel api urls module
"""
from django.conf.urls import url

from channel.api.country_view import CountryListApiView
from channel.api.views import ChannelAuthenticationApiView
from channel.api.views import ChannelListApiView
from channel.api.views import ChannelListFiltersApiView
from channel.api.views import ChannelRetrieveUpdateApiView
from channel.api.views import ChannelSetApiView

urlpatterns = [
    url(r'^countries/$', CountryListApiView.as_view(), name="countries_list"),
    url(r'^channels/authentication/$', ChannelAuthenticationApiView.as_view(),
        name='channel_authentication'),
    url(r'^channels/$', ChannelListApiView.as_view(), name="channel_list"),
    url(r'^channels/filters/$', ChannelListFiltersApiView.as_view(),
        name='channel_filters'),
    url(r'^channels/(?P<pk>[\w-]+)/$', ChannelRetrieveUpdateApiView.as_view(),
        name='channel'),
    url(r'^channel_set/$', ChannelSetApiView.as_view(), name="channel_set"),
]
