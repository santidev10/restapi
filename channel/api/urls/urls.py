"""
Chanel api urls module
"""
from django.conf.urls import url

from channel.api.country_view import CountryListApiView
from channel.api.views import ChannelAuthenticationApiView
from channel.api.views import ChannelListApiView
from channel.api.views import ChannelListFiltersApiView
from channel.api.views import ChannelRetrieveUpdateDeleteApiView
from channel.api.views import ChannelSetApiView
from .names import ChannelPathName

urlpatterns = [
    url(r"^countries/$", CountryListApiView.as_view(), name=ChannelPathName.COUNTRIES_LIST),
    url(r"^channels/authentication/$", ChannelAuthenticationApiView.as_view(),
        name=ChannelPathName.CHANNEL_AUTHENTICATION),
    url(r"^channels/$", ChannelListApiView.as_view(), name=ChannelPathName.CHANNEL_LIST),
    url(r"^channels/filters/$", ChannelListFiltersApiView.as_view(), name=ChannelPathName.CHANNEL_FILTERS),
    url(r"^channels/(?P<pk>[\w-]+)/$", ChannelRetrieveUpdateDeleteApiView.as_view(), name=ChannelPathName.CHANNEL),
    url(r"^channel_set/$", ChannelSetApiView.as_view(), name=ChannelPathName.CHANNEL_SET),
]
