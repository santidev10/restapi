import csv

from typing import Type
from django.core.files.uploadedfile import UploadedFile
from rest_framework import serializers
from rest_framework.serializers import ValidationError

from performiq.utils.map_csv_fields import CSVHeaderUtil


EXPECTED_CONTENT_TYPES = [
    "text/csv",
    "text/comma-separated-values",
    "application/csv",
    "application/excel",
    "application/vnd.msexcel",
    "application/vnd.ms-excel",
]


class CSVFileField(serializers.FileField):

    def run_validation(self, data: Type[UploadedFile]):
        """
        NOTE: Although application/vnd.ms-excel (an excel content type) is allowed, it should only be allowed with a
        .csv file extension. The extension should always be checked first!
        :param data:
        :return:
        """
        super().run_validation(data=data)
        # check file extension
        if not data.name.endswith("csv"):
            raise ValidationError("The file extension must be '.csv'")
        # check content type
        if data.content_type not in EXPECTED_CONTENT_TYPES:
            expected_types_str = ", ".join(EXPECTED_CONTENT_TYPES)
            msg = f"The file's content type ({data.content_type}) is not of the expected: '{expected_types_str}'"
            raise ValidationError(msg)
        # reset file position and grab the first chunk to validate on
        data.seek(0)
        try:
            csv_header_util = CSVHeaderUtil(data)
        except csv.Error:
            raise ValidationError("Unable to parse the CSV file")
        if not csv_header_util.is_valid():
            raise ValidationError(csv_header_util.validation_errors)

        return data


class MapCSVFieldsSerializer(serializers.Serializer):
    csv_file = CSVFileField(required=True, allow_empty_file=False)
