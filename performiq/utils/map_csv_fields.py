import csv
import string
from io import StringIO

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.core.validators import URLValidator

from performiq.api.serializers.map_csv_fields_serializer import CSVFileField
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
    if isinstance(value, str):
        for substr in ["www", "http", ".com"]:
            if substr in value:
                return value
    validator = URLValidator()
    validator(value)
    return value


class CSVColumnMapper:

    alt_header_names = {
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

    # listed highest certainty first
    headers_by_certainty = [
        CSVFieldTypeEnum.URL.value,
        CSVFieldTypeEnum.AVERAGE_CPV.value,
        CSVFieldTypeEnum.AVERAGE_CPM.value,
        CSVFieldTypeEnum.VIDEO_PLAYED_VIEW_RATE.value,
        # very uncertain guesses below
        CSVFieldTypeEnum.COST.value,
        CSVFieldTypeEnum.IMPRESSIONS.value,
        CSVFieldTypeEnum.VIEWS.value,
    ]


    def __init__(self, csv_file: InMemoryUploadedFile):
        self.csv_file = csv_file
        self._init_csv_data()
        self._map_csv_fields()

    def get_mapping(self) -> dict:
        """
        public method for getting the mapping after initializing with csv file
        :return: dict
        """
        return self.header_map

    def get_column_options(self) -> dict:
        """
        public method for geting the default column_letter_key:label dict.
        If no headers are present use "column A", etc. if headers are
        present, use the declared header values
        :return: dict
        """
        letters = list(string.ascii_uppercase)[:len(self.header_row)]
        labels = self.header_row if self.csv_has_header_row else [f"column {letter}" for letter in letters]
        return dict(zip(letters, labels))


    def _init_csv_data(self):
        """
        initialize data needed from csv for rest of script
        :param csv_file:
        :return:
        """
        file = self.csv_file.read().decode("utf-8-sig", errors="ignore")
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=",", quotechar="\"")

        self.header_row = next(reader)
        self.csv_has_header_row = True if CSVFileField.is_header_row(self.header_row) else False
        self.data_row = next(reader) if self.csv_has_header_row else self.header_row
        self.header_map = {header.value: None for header in CSVFieldTypeEnum}
        self.available_headers = [header.value for header in CSVFieldTypeEnum]
        column_letters = list(string.ascii_uppercase)[:len(self.available_headers)]

        # initialize header/data guess maps
        self.header_guess_map = {letter: [] for letter in column_letters}
        self.data_guess_map = {letter: [] for letter in column_letters}

        header_data_map = dict(zip(self.header_row, self.data_row))
        for header, data in header_data_map.items():
            column_letter = column_letters.pop(0)
            if self.csv_has_header_row:
                header_guess = self._get_header_guess(header)
                self.header_guess_map[column_letter] = header_guess
            data_guesses = self._get_data_guesses(data)
            self.data_guess_map[column_letter] = data_guesses

    def _map_csv_fields(self):
        """
        map csv columns to a best guess for a header. Map using csv header names, if present
        :param csv_file:
        :return: None
        """

        # do header name based guess assignment
        if self.csv_has_header_row:
            self._assign_header_guesses_by_header_name()
        # do data value based guess assignment
        self._assign_unique_guess_headers()
        self.data_guess_map = self._remove_unavailable_headers(self.data_guess_map)
        self._assign_headers_by_certainty(self.data_guess_map)

    def _assign_header_guesses_by_header_name(self):
        """
        assign header guesses based on header name
        :return:
        """
        for letter, header_guess in self.header_guess_map.items():
            if not header_guess:
                continue
            if header_guess not in self.available_headers:
                continue
            self.header_map[header_guess] = letter
            self.available_headers.remove(header_guess)

    def _assign_headers_by_certainty(self, guess_map):
        """
        assign guess headers by certainty of the guess
        :return:
        """
        for header in self.headers_by_certainty:
            if header not in self.available_headers:
                continue
            self._assign_header_by_certainty(header, guess_map)
            guess_map = self._remove_unavailable_headers(guess_map)

    def _assign_header_by_certainty(self, header, guess_map):
        """
        assign a guess header by certainty
        assign if
        :param header:
        :param guess_map:
        :return:
        """
        if header not in self.available_headers:
            return
        member_map = {letter: guesses for letter, guesses in guess_map.items() if header in guesses}
        # assign header if it was not guessed for any other column
        if len(member_map) == 1:
            letter = next(iter(member_map))
            self.header_map[header] = letter
            self.available_headers.remove(header)
            return

        # narrow possible columns by reducing to columns with the lowest number of guesses
        by_count = {letter: len(guesses) for letter, guesses in member_map.items()}
        counts = by_count.values()
        lowest_count = sorted(counts)[0]
        reduced_map = {letter: guesses for letter, guesses in member_map.items() if len(guesses) == lowest_count}

        # set first item in reduced group
        letter = next(iter(reduced_map))
        self.header_map[header] = letter
        self.available_headers.remove(header)

    def _assign_unique_guess_headers(self):
        """
        assign guess headers if it's the only guess in the whole guess map
        :return:
        """
        for letter, guesses in self.data_guess_map.items():
            if len(guesses) != 1 \
                    or guesses[0] not in self.available_headers \
                    or self._in_other_guesses(letter, guesses[0], self.data_guess_map):
                continue
            guess = guesses[0]
            self.header_map[guess] = letter
            self.available_headers.remove(guess)

    def _remove_unavailable_headers(self, guess_map: dict):
        """
        for a given guess map, remove letter column keys that have already been mapped
        :param guess_map: expects a map that's an instance attr
        :return:
        """
        new_map = {}
        # mapped to letter
        inverse_header_map = {value: key for key, value in self.header_map.items()}
        for letter, guesses in guess_map.items():
            letter_assigned = inverse_header_map.get(letter, None)
            if letter_assigned:
                continue
            intersection = list(set(guesses) & set(self.available_headers))
            new_map[letter] = intersection
        return new_map

    def _in_other_guesses(self, member_letter, member, guesses_map):
        """
        check if guess is in other column guesses for a given guess map
        :param member_letter:
        :param member:
        :param guesses_map:
        :return:
        """
        new_map = guesses_map.copy()
        new_map.pop(member_letter, None)
        for letter, guesses in new_map.items():
            if member in guesses:
                return True
        return False

    def _get_header_guess(self, value: str) -> str:
        """
        attempt to guess the header for a given header value
        :param value:
        :return:
        """
        value = value.lower()
        for header, alt_names in self.alt_header_names.items():
            for alt_name in alt_names:
                if alt_name in value:
                    return header

    def _get_data_guesses(self, value) -> list:
        """
        attempt to guess the header for a given column data value
        :param value:
        :return:
        """
        guesses = []
        for header_name, func in self.data_guess_functions.items():
            try:
                func(value)
            except ValidationError:
                continue
            guesses.append(header_name)
        return guesses
