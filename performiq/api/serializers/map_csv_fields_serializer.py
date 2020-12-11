import csv
from io import StringIO

from typing import Type
from django.core.files.uploadedfile import UploadedFile
from rest_framework import serializers
from rest_framework.serializers import ValidationError

from performiq.utils.map_csv_fields import decode_to_string
from performiq.utils.map_csv_fields import get_reader
from performiq.utils.map_csv_fields import is_header_row


class CSVFileField(serializers.FileField):

    def run_validation(self, data: Type[UploadedFile]):
        super().run_validation(data=data)
        # check file extension
        if not data.name.endswith("csv"):
            raise ValidationError("The file extension must be '.csv'")
        # check content type
        expected_content_type = "text/csv"
        if data.content_type != expected_content_type:
            msg = f"The file's content type ({data.content_type}) is not the expected: '{expected_content_type}'"
            raise ValidationError(msg)
        # reset file position and grab the first chunk to validate on
        data.seek(0)
        chunk = next(data.chunks())
        decoded = decode_to_string(chunk)
        io_string = StringIO(decoded)
        try:
            reader = get_reader(io_string)
            header_row = next(reader)
            self._validate_header_row(header_row)
        except csv.Error:
            raise ValidationError("Unable to parse the CSV file")
        # validate that there is one row besides the header row, if a header row is present
        if is_header_row(header_row):
            try:
                next(reader)
            except StopIteration:
                raise ValidationError("CSV Invalid, one row besides the header row must be present.")

        return data

    def _validate_header_row(self, header_row) -> None:
        nulls = [value for value in header_row if not value]
        if nulls:
            raise ValidationError(f"Header row invalid. Reason: no values detected")


class MapCSVFieldsSerializer(serializers.Serializer):
    csv_file = CSVFileField(required=True, allow_empty_file=False)
