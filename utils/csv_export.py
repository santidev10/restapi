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
