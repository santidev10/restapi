from rest_framework.response import Response
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from singledb.connector import SingleDatabaseApiConnectorException


class SingledbApiView(APIView):
    def delete(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_delete'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
        return self._connect(request, self.connector_delete, data=request.data, **kwargs)

    def get(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_get'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
        return self._connect(request, self.connector_get, **kwargs)

    def post(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_post'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
        return self._connect(request, self.connector_post, data=request.data, **kwargs)

    def put(self, request, *args, **kwargs):
        if not hasattr(self, 'connector_put'):
            return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
        return self._connect(request, self.connector_put, data=request.data, **kwargs)

    def _connect(self, request, connector, **kwargs):
        try:
            response_data = connector(request.query_params, **kwargs)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)
