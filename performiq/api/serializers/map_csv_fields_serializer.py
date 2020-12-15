import csv

from typing import Type
from django.core.files.uploadedfile import UploadedFile
from rest_framework import serializers
from rest_framework.serializers import ValidationError

from performiq.utils.map_csv_fields import CSVHeaderUtil


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
        try:
            csv_header_util = CSVHeaderUtil(data)
        except csv.Error:
            raise ValidationError("Unable to parse the CSV file")
        if not csv_header_util.is_valid():
            raise ValidationError("The CSV did not have the required format!")

        return data


class MapCSVFieldsSerializer(serializers.Serializer):
    csv_file = CSVFileField(required=True, allow_empty_file=False)
