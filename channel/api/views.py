"""
Chanel api views module
"""
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from utils.single_database_connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException


class ChannelListApiView(APIView):
    """
    Proxy view for channel list
    """
    def get(self, request):
        """
        Get procedure
        """
        connector = SingleDatabaseApiConnector()
        try:
            response_data = connector.get_channel_list(request.query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)
