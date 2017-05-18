"""
Channel api views module
"""
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class ChannelListApiView(SingledbApiView):
    connector_get = Connector().get_channel_list


class ChannelListFiltersApiView(SingledbApiView):
    connector_get = Connector().get_channel_filters_list


class ChannelRetrieveUpdateApiView(SingledbApiView):
    connector_get = Connector().get_channel
    connector_put = Connector().put_channel


class ChannelSetApiView(SingledbApiView):
    connector_delete = Connector().delete_channels
