from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class HighlightChannelsListApiView(SingledbApiView):
    connector_get = Connector().get_highlights_channels
