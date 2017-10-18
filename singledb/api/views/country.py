from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class CountryListApiView(SingledbApiView):
    permission_required = tuple()
    connector_get = Connector().get_country_list
