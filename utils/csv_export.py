"""
CSV export mechanism module for objects list
"""
import csv
from io import StringIO
from urllib.parse import unquote

from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.lang import flatten_generator


class BaseCSVStreamResponseGenerator(object):
    def __init__(self, columns, data_generator, headers_map):
        self.columns = columns
        self.data_generator = data_generator
        self.headers_map = headers_map

    def _map_row(self, row):
        return [row.get(column) for column in self.columns]

    def get_filename(self):
        return "report.csv"

    def prepare_csv_file_response(self):
        """
        Prepare streaming response obj
        :return: file response
        """
        response = StreamingHttpResponse(
            self.stream_generator(),
            content_type="text/csv",
            status=HTTP_200_OK,
        )
        filename = self.get_filename()
        response["Content-Disposition"] = "attachment; filename='{}'".format(filename)
        return response

    def export_generator(self):
        """
        Export data generator
        """
        yield self._map_row(self.headers_map)
        for row in self.data_generator:
            yield self._map_row(row)

    def stream_generator(self):
        for row in self.export_generator():
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(row)
            yield output.getvalue()


class CSVExport(BaseCSVStreamResponseGenerator):
    """
    Class for csv export
    """

    def __init__(self, fields, data, file_title):
        """
        Set up params
        """
        super(CSVExport, self).__init__(fields, data, {f: f for f in fields})
        self.file_title = file_title

    def get_filename(self):
        return "{title}_export_report {date}.csv".format(
            title=self.file_title,
            date=timezone.now().strftime("%d-%m-%Y.%H:%M%p")
        )


class CassandraExportMixin(object):
    """
    Export mixin for cassandra data
    """

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
        request.query_params["fields"] = ",".join(self.fields_to_export)
        request.query_params.update(request.data)
        # prepare api call
        request.query_params._mutable = True
        response = self.get(request)
        if response.status_code > 300:
            raise SDBError(request)

        return response.data.get("items")

    def _data_filtered(self, filters):
        return flatten_generator(self._data_filtered_batch_generator(filters))

    def post(self, request):
        """
        Export mechanism
        """
        assert self.fields_to_export and self.export_file_title
        filters = request.data.get("filters")
        try:
            if filters is not None:
                export_data = self._data_filtered(filters)
            else:
                export_data = self._data_simple(request)
        except SDBError as er:
            return er.sdb_response

        # generate csv file
        csv_generator = CSVExport(
            fields=self.fields_to_export,
            data=export_data,
            file_title=self.export_file_title)
        response = csv_generator.prepare_csv_file_response()
        return response


class SDBError(Exception):
    def __init__(self, sdb_response):
        self.sdb_response = sdb_response
