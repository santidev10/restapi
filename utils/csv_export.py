"""
CSV export mechanism module for objects list
"""
import csv
from io import StringIO

from django.http import StreamingHttpResponse
from django.utils import timezone


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
            stream_generator(), content_type='text/csv')
        filename = "{title}_export_report {date}.csv".format(
            title=self.file_title,
            date=timezone.now().strftime("%d-%m-%Y.%H:%M%p")
        )
        response['Content-Disposition'] = 'attachment; filename="{}"'.format(
            filename)
        return response


def list_export(method):
    """
    Decorator function for objects list export
    """
    def wrapper_get(self, request, *args, **kwargs):
        """
        Extended get method
        """
        is_export = request.query_params.get("export")
        if is_export == "1":
            # check required export options
            # assert self.fields_to_export and self.export_file_title
            # prepare api call
            request.query_params._mutable = True
            request.query_params["flat"] = "1"
            response = method(
                self=self, request=request, *args, **kwargs)
            if response.status_code > 300:
                return response
            # generate csv file
            csv_generator = CSVExport(
                fields=self.fields_to_export,
                data=response.data, file_title=self.export_file_title)
            response = csv_generator.prepare_csv_file_response()
            return response
        response = method(
            self=self, request=request, *args, **kwargs)
        return response
    return wrapper_get
