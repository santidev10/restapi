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
    def __init__(self, fields, data, obj_type, countable_fields=None):
        """
        Set up params
        """
        self.allowed_countable_fields = {
            "youtube_link"
        }
        if countable_fields is not None:
            if not countable_fields.issubset(self.allowed_countable_fields):
                raise CSVExportException(
                    "Not allowed countable fields were sent")
            self.countable_fields = countable_fields
        else:
            self.countable_fields = set()
        self.allowed_obj_types = {
            "channel",
            "video",
            "keyword"
        }
        if obj_type not in self.allowed_obj_types:
            raise CSVExportException("Not valid obj_type")
        self.obj_type = obj_type
        self.fields = fields
        self.data = data

    def prepare_youtube_link_field(self, obj_data):
        """
        Generate obj youtube_link
        """
        if self.obj_type == "channel":
            return "https://youtube.com/channek/{}/".format(obj_data.get("id"))

    def export_generator(self):
        """
        Export data generator
        """
        yield self.fields
        for obj in self.data:
            row = []
            for field in self.fields:
                if field in self.countable_fields:
                    row.append(
                        getattr(self, "prepare_{}_field".format(field))(obj))
                    continue
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
        filename = "channels_export_report {date}.csv".format(
            date=timezone.now().strftime("%d-%m-%Y.%H:%M%p")
        )
        response['Content-Disposition'] = 'attachment; filename="{}"'.format(
            filename)
        return response

