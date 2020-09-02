import datetime

from django.http import HttpResponse
from rest_framework.exceptions import APIException
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_404_NOT_FOUND

XLSX_CONTENT_TYPE = "application/" \
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def xlsx_response(title, xlsx):
    response = HttpResponse(
        xlsx,
        content_type=XLSX_CONTENT_TYPE
    )
    response["Content-Disposition"] = "attachment; filename=\"{}.xlsx\"".format(
        title)
    return response


def get_object(model, message=None, code=HTTP_404_NOT_FOUND, should_raise=True, **kwargs):
    """
    Utility function to retrieve model objects
    Raises DRF APIException with message if not found
    :param model: Django model
    :param message: str
    :param code: Http code
    :param should_raise: bool
    :param kwargs:
    :return: model obj
    """
    message = message if message else f"Item not found with params: {kwargs}"
    obj = None
    try:
        obj = model.objects.get(**kwargs)
    except model.DoesNotExist:
        if should_raise:
            raise CustomAPIException(code, message)
    return obj


def validate_fields(expected, received, should_raise=True, message="Missing fields"):
    """
    Utility function to validate fields the same as expected
    Raises DRF ValidationError with message of
    :param expected: iter [str...]
    :param received: iter [str...]
    :param should_raise: bool
    :param message: str
    :return: bool
    """
    validated = False
    remains = set(expected) - set(received)
    if remains:
        if should_raise:
            raise ValidationError(f"{message}: {list(remains)}")
    else:
        validated = True
    return validated


def validate_date(date_str, date_format="%Y-%m-%d", message="Accepted format: YYYY-MM-DD", should_raise=True):
    validated = None
    try:
        datetime.datetime.strptime(date_str, date_format)
        validated = date_str
    except ValueError:
        if should_raise:
            raise ValidationError(message)
    return validated


def validate_max_page(max_size, size, page, should_raise=True):
    max_page = max_size / size
    if page > max_page and should_raise:
        raise ValidationError(f"Max page allowed: {max_page}")


class CustomAPIException(APIException):
    def __init__(self, status_code, detail):
        super().__init__()
        self.status_code = status_code
        self.detail = detail
