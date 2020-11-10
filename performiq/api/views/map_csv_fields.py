import csv
import string
from io import StringIO

from django.core.validators import URLValidator

from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView
from django.core.exceptions import ValidationError

from performiq.api.serializers.map_csv_fields_serializer import MapCSVFieldsSerializer
from performiq.utils.constants import CSVFieldTypeEnum


# TODO if this is not good enough for detection, we can pull in a package to get all iso 4217 codes and symbols
CURRENCY_STRINGS = ["USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "NZD", "SEK", "KRW"]
CURRENCY_SYMBOLS = ["$", "€", "¥", "£", "A$", "C$", "元", "HK$", "NZ$", "kr", "₩"]


def is_currency_validator(value):
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        if value.isnumeric():
            return value
        try:
            float(value)
            return value
        except ValueError:
            pass
        for currency_string in CURRENCY_STRINGS:
            if currency_string in value:
                return value
        for symbol in CURRENCY_SYMBOLS:
            if symbol in value:
                return value
    raise ValidationError("Not a valid currency")


def is_integer_validator(value):
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValidationError("value is a non-integer float instance")
        return value
    if isinstance(value, str):
        split = value.split(".")
        if len(split) > 2:
            raise ValidationError("greater than two decimals detected. Invalid integer")
        try:
            float_value = float(value)
            if not float_value.is_integer():
                raise ValidationError("string float values must be an integer")
        except ValueError:
            raise ValidationError("value not castable to float")
        return value
    raise ValidationError("value is not an integer")


def is_cpm_validator(value):
    is_currency_validator(value)

    if 1 <= float(value) <= 10:
        return value
    raise ValidationError("value likely not cpm")


def is_cpv_validator(value):
    is_currency_validator(value)

    if float(value) <= 0.1:
        return value
    raise ValidationError("value likely not cpv")


def is_rate_validator(value):
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            raise ValidationError("value not castable to float")
    if isinstance(value, float) or isinstance(value, int):
        if value > 100:
            raise ValidationError("value must be less than 100")
        if value < 0:
            raise ValidationError("value must be zero or positive")
        return value
    raise ValidationError("value is likely not a percentage")


def is_url_validator(value):
    validator = URLValidator()
    validator(value)
    return value


class PerformIQMapCSVFieldsAPIView(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    parser_classes = [MultiPartParser]

    serializer_class = MapCSVFieldsSerializer

    header_guess_names = {
        CSVFieldTypeEnum.URL.value: ["url", "link", "www", "http"],
        CSVFieldTypeEnum.IMPRESSIONS.value: ["impression", "impres", "imp"],
        CSVFieldTypeEnum.VIEWS.value: ["views",],
        CSVFieldTypeEnum.COST.value: CURRENCY_STRINGS + ["$", "cost"],
        CSVFieldTypeEnum.AVERAGE_CPV.value: ["cpv", "avg", "average"],
        CSVFieldTypeEnum.AVERAGE_CPM.value: ["cpm", "avg", "average"],
        CSVFieldTypeEnum.VIDEO_PLAYED_VIEW_RATE.value: ["rate"],
    }

    data_guess_functions = {
        CSVFieldTypeEnum.URL.value: is_url_validator,
        CSVFieldTypeEnum.IMPRESSIONS.value: is_integer_validator,
        CSVFieldTypeEnum.VIEWS.value: is_integer_validator,
        CSVFieldTypeEnum.COST.value: is_currency_validator,
        CSVFieldTypeEnum.AVERAGE_CPV.value: is_cpv_validator,
        CSVFieldTypeEnum.AVERAGE_CPM.value: is_cpm_validator,
        CSVFieldTypeEnum.VIDEO_PLAYED_VIEW_RATE.value: is_rate_validator,
    }

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        csv_file = validated_data.get("csv_file")
        fields_map = self._map_csv_fields(csv_file)
        default_map = self._get_default_map()
        data = {
            "mapping": fields_map,
            "column_options": default_map
        }
        return Response(status=HTTP_200_OK, data=data)

    def _get_default_map(self) -> dict:
        """
        Get the default column: name ordered dict
        :return:
        """
        default_headers = [header.value for header in CSVFieldTypeEnum]
        letters = list(string.ascii_uppercase)[:len(default_headers)]
        return dict(zip(letters, default_headers))

    def _map_csv_fields(self, csv_file) -> dict:
        """
        map csv columns to a best guess for a header
        :param csv_file:
        :return:
        """
        file = csv_file.read().decode("utf-8-sig", errors="ignore")
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=",", quotechar="\"")

        header_row = next(reader)
        data_row = next(reader)
        header_data_map = dict(zip(header_row, data_row))
        header_map = {header.value: None for header in CSVFieldTypeEnum}
        available_headers = [header.value for header in CSVFieldTypeEnum]
        column_letters = list(string.ascii_uppercase)
        for header, data in header_data_map.items():
            column_letter = column_letters.pop(0)
            header_guess = self._get_header_guess(header)
            if header_guess and header_guess in available_headers:
                header_map[header_guess] = column_letter
                available_headers.remove(header_guess)
                continue
            data_guess = self._get_data_guess(data)
            if data_guess and data_guess in available_headers:
                header_map[data_guess] = column_letter
                available_headers.remove(data_guess)
                continue

        return header_map

    def _get_header_guess(self, value: str) -> str:
        """
        attempt to guess the header for a given header value
        :param value:
        :return:
        """
        value = value.lower()
        for header_name, alt_names in self.header_guess_names.items():
            for alt_name in alt_names:
                if alt_name in value:
                    return header_name

    def _get_data_guess(self, value) -> str:
        """
        attempt to guess the header for a given column data value
        :param value:
        :return:
        """
        for header_name, func in self.data_guess_functions.items():
            try:
                func(value)
            except ValidationError:
                continue
            return header_name
