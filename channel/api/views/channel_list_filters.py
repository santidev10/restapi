from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class ChannelListFiltersApiView(SingledbApiView):
    permission_required = ('userprofile.channel_filter',)
    connector_get = Connector().get_channel_filters_list