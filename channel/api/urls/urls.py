"""
Channel api urls module
"""
from django.conf.urls import url

from channel.api.country_view import CountryListApiView
from channel.api.views import ChannelAuthenticationApiView
from channel.api.views import ChannelListApiView
from channel.api.views import ChannelListExportApiView
from channel.api.views import ChannelRetrieveUpdateDeleteApiView
from channel.api.views import ChannelSetApiView
from channel.api.views import ChannelTrackApiView
from .names import ChannelPathName

urlpatterns = [
    url(r"^countries/$", CountryListApiView.as_view(), name=ChannelPathName.COUNTRIES_LIST),
    url(r"^channels/authentication/$", ChannelAuthenticationApiView.as_view(),
        name=ChannelPathName.CHANNEL_AUTHENTICATION),
    url(r"^channels/$", ChannelListApiView.as_view(), name=ChannelPathName.CHANNEL_LIST),
    url(r"^channels/export/$", ChannelListExportApiView.as_view(), name=ChannelPathName.CHANNEL_LIST_PREPARE_EXPORT),
    url(r"^channels/export/(?P<export_name>.+)/$", ChannelListExportApiView.as_view(), name=ChannelPathName.CHANNEL_LIST_EXPORT),
    url(r"^channels/(?P<pk>[\w-]+)/$", ChannelRetrieveUpdateDeleteApiView.as_view(), name=ChannelPathName.CHANNEL),
    url(r"^channel_set/$", ChannelSetApiView.as_view(), name=ChannelPathName.CHANNEL_SET),
    url(r"^channel_track/$", ChannelTrackApiView.as_view(), name=ChannelPathName.CHANNEL_TRACK)
]
