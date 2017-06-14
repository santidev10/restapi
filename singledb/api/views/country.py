from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class CountryListApiView(SingledbApiView):
    connector_get = Connector().get_country_list
