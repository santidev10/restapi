from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class CountryListApiView(SingledbApiView):
    connector = Connector().get_country_list
