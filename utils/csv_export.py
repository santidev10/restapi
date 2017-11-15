"""
CSV export mechanism module for objects list
"""
import csv
from io import StringIO

from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.status import HTTP_201_CREATED

from singledb.connector import SingleDatabaseApiConnector as Connector


class CSVExportException(Exception):
    """
    Exception class from csv export
    """
    pass


class CSVExport(object):
    """
    Class for csv export
    """
    def __init__(self, fields, data, file_title):
        """
        Set up params
        """
        self.fields = fields
        self.data = data
        self.file_title = file_title

    def export_generator(self):
        """
        Export data generator
        """
        yield self.fields
        for obj in self.data:
            row = []
            for field in self.fields:
                row.append(obj.get(field))
            yield row

    def prepare_csv_file_response(self):
        """
        Prepare streaming response obj
        :return: file response
        """
        def stream_generator():
            for row in self.export_generator():
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(row)
                yield output.getvalue()

        response = StreamingHttpResponse(
            stream_generator(),
            content_type="text/csv",
            status=HTTP_201_CREATED
        )
        filename = "{title}_export_report {date}.csv".format(
            title=self.file_title,
            date=timezone.now().strftime("%d-%m-%Y.%H:%M%p")
        )
        response["Content-Disposition"] = "attachment; filename='{}'".format(
            filename)
        return response


class CassandraExportMixin(object):
    """
    Export mixin for cassandra data
    """
    def post(self, request):
        """
        Export mechanism
        """
        assert self.fields_to_export and self.export_file_title
        # max export size limit
        max_export_size = 10000
        ids = request.data.pop("ids", None)
        if ids:
            connector = Connector()
            request.data["ids_hash"] = connector.store_ids(ids.split(","))
        request.query_params._mutable = True
        request.query_params["size"] = max_export_size
        request.query_params["fields"] = ",".join(self.fields_to_export)
        request.query_params.update(request.data)
        # prepare api call
        request.query_params._mutable = True
        response = self.get(request)
        if response.status_code > 300:
            return response
        # generate csv file
        csv_generator = CSVExport(
            fields=self.fields_to_export,
            data=response.data.get("items"),
            file_title=self.export_file_title)
        response = csv_generator.prepare_csv_file_response()
        return response
