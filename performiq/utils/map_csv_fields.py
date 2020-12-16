import csv
import operator
import string
from io import StringIO
from typing import Type
from abc import ABC
from abc import abstractmethod

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import UploadedFile
from django.core.validators import URLValidator

from performiq.utils.constants import CSVFieldTypeEnum


# TODO if this is not good enough for detection, we can pull in a package to get all iso 4217 codes and symbols
CURRENCY_STRINGS = ["USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "NZD", "SEK", "KRW"]
CURRENCY_SYMBOLS = ["$", "€", "¥", "£", "A$", "C$", "元", "HK$", "NZ$", "kr", "₩"]


def is_float_validator(value):
    try:
        float(value)
    except ValueError:
        raise ValidationError("Value not castable to float")

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
    is_float_validator(value)

    if 1 <= float(value) <= 10:
        return value
    raise ValidationError("value likely not cpm")


def is_cpv_validator(value):
    is_currency_validator(value)
    is_float_validator(value)

    try:
        float(value)
    except ValueError:
        raise ValidationError("Value not castable to float")
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


def is_header_row(row: list) -> bool:
    """
    check for signs that a row is a header row
    :param row:
    :return: Bool
    """
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


def decode_to_string(data: bytes) -> str:
    """
    decode a bytes object into a string, try multiple character encodings
    NOTE: Google's reports use utf-16!
    :param data:
    :return: str
    """
    for encoding in ["utf-8", "utf-8-sig", "utf-16", "utf-32"]:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValidationError("Could not find the right character encoding!")


class AbstractCSVType(ABC):
    """
    Abstract class for validation with CSVHeaderUtil
    """

    def __init__(self, rows: list):
        self.rows = rows

    @abstractmethod
    def is_valid(self):
        """
        raise ValidationErrors if the `rows` list passed in __init__ are invalid for the concrete type
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def get_type_string():
        """
        return a type string in snake case, e.g.: csv_type_asdf
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def get_first_data_row_index():
        """
        return the index of the first data row
        :return:
        """
        pass


class CSVWithOnlyData(AbstractCSVType):

    def is_valid(self):
        if not len(self.rows):
            raise ValidationError("CSV must have at least one row")
        if not len(self.rows[0]):
            raise ValidationError("CSV must have at least one column")
        if is_header_row(self.rows[0]):
            raise ValidationError("CSV cannot have a header row")
        return True

    @staticmethod
    def get_type_string():
        return "csv_with_only_data"

    @staticmethod
    def get_first_data_row_index():
        return 0


class CSVWithHeader(AbstractCSVType):

    def is_valid(self):
        if len(self.rows) < 2:
            raise ValidationError("CSV must have at least two rows")
        if not len(self.rows[0]):
            raise ValidationError("CSV must have at least one column")
        if not is_header_row(self.rows[0]):
            raise ValidationError("First row must be a header row")
        nulls = [value for value in self.rows[0] if not value]
        if nulls:
            raise ValidationError("Header row invalid. Reason: no values detected")
        if is_header_row(self.rows[1]):
            raise ValidationError("Second row must be a data row")
        return True

    @staticmethod
    def get_type_string():
        return "csv_with_headers"

    @staticmethod
    def get_first_data_row_index():
        return 1


class ManagedPlacementsReport(AbstractCSVType):

    def is_valid(self):
        if len(self.rows) < 4:
            raise ValidationError("Managed placements reports must have at least 4 rows")
        if not len(self.rows[0]):
            raise ValidationError("CSV must have at least one column")
        if not len(self.rows[0]) or self.rows[0][0] != "Managed placements report":
            raise ValidationError("First row must be 'Managed placements report'")
        if not len(self.rows[1]) or self.rows[1][0] != "All time":
            raise ValidationError("Second must be 'All time'")
        if not is_header_row(self.rows[2]):
            raise ValidationError("Third row must be a header row")
        return True

    @staticmethod
    def get_type_string():
        return "managed_placements_report"

    @staticmethod
    def get_first_data_row_index():
        return 3


class CSVHeaderUtil:

    csv_header_types = [
        CSVWithOnlyData,
        CSVWithHeader,
        ManagedPlacementsReport,
    ]

    def __init__(self, csv_file: Type[UploadedFile] = None, reader: csv.reader = None, rows: list = None,
                 row_depth: int = 4):
        """
        takes either an UploadedFile, csv.reader, or list of rows, and an optional row_depth to validate or determine
        the index of the first data row for the given csv representation
        :param csv_file: UploadedFile. Encoding, delimiters will be determined by the util
        :param reader: csv.reader instance.
        :param rows: list of rows
        :param row_depth: integer. default 4. max depth to scan to determine csv "type". Should be set to the lowest
        required depth. Should be the max of all csv_header_types' data row index
        """
        if all(item is None for item in [csv_file, reader, rows]):
            raise ValueError("Either csv_file, reader or rows must be passed!")

        self.row_depth = row_depth
        self.validation_errors = {}
        self.valid_types = {}

        if rows is not None:
            self.rows = rows[:self.row_depth]
        elif reader is not None:
            self.reader = reader
            self._init_rows_from_reader()
        elif csv_file is not None:
            csv_file.seek(0)
            chunk = next(csv_file.chunks())
            decoded = decode_to_string(chunk)
            io_string = StringIO(decoded)
            self.reader = get_reader(io_string)
            self._init_rows_from_reader()

        self._run_validation()

        if csv_file:
            csv_file.seek(0)

    def _init_rows_from_reader(self):
        """
        take row samples from self.reader up to self.row_depth limit
        :return:
        """
        self.rows = []
        for i in range(self.row_depth):
            try:
                row = next(self.reader)
            except StopIteration:
                break
            self.rows.append(row)

    def _run_validation(self):
        """
        run through all registered validators, store type-mapped validation errors
        :return:
        """
        for csv_type in self.csv_header_types:
            csv_type_string = csv_type.get_type_string()

            instance = csv_type(self.rows)
            try:
                instance.is_valid()
            except ValidationError as e:
                type_errors = self.validation_errors.get(csv_type_string, [])
                type_errors.append(e.message)
                self.validation_errors[csv_type_string] = type_errors
                continue

            self.valid_types[csv_type_string] = instance

    def is_valid(self) -> bool:
        """
        True if there's at least one valid type in the valid_types dict
        :return:
        """
        return True if self.valid_types else False

    def get_first_data_row_index(self) -> int:
        """
        get the index of the first data row
        :return: int
        """
        if not self.valid_types:
            raise ValidationError(self.validation_errors)

        indices = [instance.get_first_data_row_index() for valid_type, instance in self.valid_types.items()]

        return max(indices)


def get_reader(io_string: StringIO, row_depth: int = 5) -> csv.reader:
    """
    get a reader with the the most likely delimiter value
    :param io_string:
    :param row_depth:
    :return csv.reader:
    """
    delimiter_map = {}
    for delimiter in [",", "\t"]:
        io_string.seek(0)
        reader = csv.reader(io_string, delimiter=delimiter, quotechar="\"")
        column_counts = []
        for _ in range(row_depth):
            try:
                row = next(reader)
            except StopIteration:
                break
            column_counts.append(len(row))

        delimiter_map[delimiter] = max(column_counts)

    if not delimiter_map:
        raise ValidationError("No rows detected!")

    delimiter_with_greatest_column_count = max(delimiter_map.items(), key=operator.itemgetter(1))[0]
    io_string.seek(0)
    return csv.reader(io_string, delimiter=delimiter_with_greatest_column_count, quotechar="\"")


class CSVColumnMapper:

    # NOTE: ordering matters here! Higher items have more priority for now
    alt_header_names = {
        CSVFieldTypeEnum.URL.value: ["url", "link", "www", "http"],
        CSVFieldTypeEnum.IMPRESSIONS.value: ["impression", "impres", "imp"],
        CSVFieldTypeEnum.VIEWS.value: ["views"],
        CSVFieldTypeEnum.COST.value: CURRENCY_STRINGS + ["$", "cost"],
        CSVFieldTypeEnum.CTR.value: ["ctr", "through rate", "through_rate", "click through", "click_through",
                                     "clickthrough"],
        CSVFieldTypeEnum.VIEW_RATE.value: ["view rate", "view_rate"],
        CSVFieldTypeEnum.VIDEO_PLAYED_TO_100_RATE.value: ["complet", "100", "play"],
        CSVFieldTypeEnum.AVERAGE_CPV.value: ["cpv", "cost per view", "cost_per_view"],
        CSVFieldTypeEnum.AVERAGE_CPM.value: ["cpm", "mille"],
    }

    data_guess_functions = {
        CSVFieldTypeEnum.URL.value: is_url_validator,
        CSVFieldTypeEnum.IMPRESSIONS.value: is_integer_validator,
        CSVFieldTypeEnum.VIEWS.value: is_integer_validator,
        CSVFieldTypeEnum.COST.value: is_currency_validator,
        CSVFieldTypeEnum.AVERAGE_CPV.value: is_cpv_validator,
        CSVFieldTypeEnum.AVERAGE_CPM.value: is_cpm_validator,
        CSVFieldTypeEnum.VIEW_RATE.value: is_rate_validator,
        CSVFieldTypeEnum.VIDEO_PLAYED_TO_100_RATE.value: is_rate_validator,
        CSVFieldTypeEnum.CTR.value: is_rate_validator,
    }

    # listed highest certainty first
    headers_by_certainty = [
        CSVFieldTypeEnum.URL.value,
        CSVFieldTypeEnum.AVERAGE_CPV.value,
        CSVFieldTypeEnum.AVERAGE_CPM.value,
        CSVFieldTypeEnum.VIEW_RATE.value,
        CSVFieldTypeEnum.VIDEO_PLAYED_TO_100_RATE.value,
        CSVFieldTypeEnum.CTR.value,
        # very uncertain guesses below
        CSVFieldTypeEnum.COST.value,
        CSVFieldTypeEnum.IMPRESSIONS.value,
        CSVFieldTypeEnum.VIEWS.value,
    ]


    def __init__(self, csv_file: Type[UploadedFile]):
        csv_file.seek(0)
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
        column_letters = self._get_column_letters(self.header_row)
        labels = self.header_row if self.csv_has_header_row else [f"column {letter}" for letter in column_letters]
        return dict(zip(column_letters, labels))

    def _get_column_letters(self, longest_row) -> list:
        """
        Given a header row, which presumes the max number of columns, get a list of the column letters/names
        :param longest_row:
        :return:
        """
        column_letters = list(string.ascii_uppercase)
        if len(longest_row) <= len(column_letters):
            return column_letters

        position = len(column_letters)
        non_alphabet_count = len(longest_row) - len(column_letters)
        for _ in range(non_alphabet_count):
            position += 1
            column_letters.append(self._get_non_alphabet_excel_column_name(position))

        return column_letters

    def _get_non_alphabet_excel_column_name(self, position):
        """
        get the column name for excel columns past alphabet columns, e.g. AA, AB...
        :param position:
        :return:
        """
        name = ""
        while position > 0:
            position, remainder = divmod(position - 1, 26)
            name = chr(65 + remainder) + name
        return name

    def _init_csv_data(self):
        """
        initialize data needed from csv for rest of script
        :param csv_file:
        :return:
        """
        # reset file position, grab first chunk to make header guess from
        self.csv_file.seek(0)
        chunk = next(self.csv_file.chunks())
        decoded = decode_to_string(chunk)
        io_string = StringIO(decoded)
        reader = get_reader(io_string)

        # skip to the header row index, if it's not in row 0
        util = CSVHeaderUtil(csv_file=self.csv_file)
        data_row_index = util.get_first_data_row_index()
        # ignores 0 and 1
        if data_row_index:
            for i in range(data_row_index - 1):
                next(reader)

        self.header_row = next(reader)
        self.csv_has_header_row = True if is_header_row(self.header_row) else False
        self.data_row = next(reader) if self.csv_has_header_row else self.header_row
        self.header_map = {header.value: None for header in CSVFieldTypeEnum}
        self.available_headers = [header.value for header in CSVFieldTypeEnum]
        column_letters = self._get_column_letters(self.header_row)

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
        if not by_count:
            return
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
