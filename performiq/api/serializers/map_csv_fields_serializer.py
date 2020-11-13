import csv

from io import StringIO

from django.core.files.uploadedfile import InMemoryUploadedFile
from rest_framework import serializers
from rest_framework.serializers import ValidationError


class CSVFileField(serializers.FileField):

    def run_validation(self, data: InMemoryUploadedFile):
        super().run_validation(data=data)
        if not data.name.endswith("csv"):
            raise ValidationError("The file extension must be '.csv'")
        file = data.read().decode("utf-8-sig", errors="ignore")
        io_string = StringIO(file)
        try:
            reader = csv.reader(io_string, delimiter=",", quotechar="\"")
            header_row = next(reader)
            self._validate_header_row(header_row)
        except csv.Error:
            raise ValidationError("Unable to parse the CSV file")
        if self.is_header_row(header_row):
            try:
                next(reader)
            except StopIteration:
                raise ValidationError("CSV Invalid, one row besides the header row must be present.")
        data.seek(0)
        return data

    def _validate_header_row(self, header_row):
        nulls = [value for value in header_row if not value]
        if nulls:
            raise ValidationError(f"Header row invalid. Reason: no values detected")

    @staticmethod
    def is_header_row(row):
        # check for obvious numerics
        numerics = [value for value in row
                    if type(value) in [int, float, complex]
                    or isinstance(value, str) and value.isnumeric()]
        if numerics:
            return False
        # check for obvious urls
        for value in row:
            if isinstance(value, str):
                for substr in ["www", "http", ".com"]:
                    if substr in value:
                        return False
        return True


class MapCSVFieldsSerializer(serializers.Serializer):
    csv_file = CSVFileField(required=True, allow_empty_file=False)