"""
CSV export mechanism module for objects list
"""
from urllib.parse import unquote

from django.http import StreamingHttpResponse
from django.http import FileResponse
from django.utils import timezone
from utils.api.file_list_api_view import FileListApiView

from singledb.connector import SingleDatabaseApiConnector as Connector


class CassandraExportMixinApiView(object):
    """
    Export mixin for cassandra data
    """

    @property
    def filename(self):
        return "{title}_export_report {date}.csv".format(
            title=self.export_file_title,
            date=timezone.now().strftime("%d-%m-%Y.%H:%M%p")
        )

    def data_generator(self, data):
        for item in data:
            yield item

    def _data_simple(self, request):
        # max export size limit
        max_export_size = 10000

        ids = request.data.pop("ids", None)
        if ids:
            ids = ids.split(",")
            if self.export_file_title == "keyword":
                ids = [unquote(i) for i in ids]
            connector = Connector()
            request.data["ids_hash"] = connector.store_ids(ids)

        request.query_params._mutable = True
        request.query_params["size"] = max_export_size
        request.query_params["fields"] = ",".join(self.renderer.header)
        request.query_params.update(request.data)
        # prepare api call
        request.query_params._mutable = True
        response = self.get(request)
        if response.status_code > 300:
            raise SDBError(request)

        return response.data.get("items")

    def _data_filtered(self, filters):
        return self._data_filtered_batch_generator(filters)

    def post(self, request):
        """
        Export mechanism
        """
        filters = self.request.data.get("filters")
        try:
            if filters is not None:
                export_data = self._data_filtered(filters)
            else:
                export_data = self._data_simple(request)
        except SDBError as er:
            return er.sdb_response

        data_generator = self.renderer().render(data=self.data_generator(export_data))
        response = FileResponse(data_generator, content_type='text/csv')
        response["Content-Disposition"] = "attachment; filename=\"{}\"".format(self.filename)
        return response


class SDBError(Exception):
    def __init__(self, sdb_response):
        self.sdb_response = sdb_response
