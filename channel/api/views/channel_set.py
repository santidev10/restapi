from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete


class ChannelSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    connector_delete = Connector().delete_channels