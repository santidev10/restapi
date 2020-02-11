from django.http import HttpResponse
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


def get_object(model, message, code=HTTP_404_NOT_FOUND, should_raise=True, **kwargs):
    """
    Utility function to retrieve model objects
    Raises DRF ValidationError with message if not found
    :param model: Django model
    :param message: str
    :param code: Http code
    :param should_raise: bool
    :param kwargs:
    :return: model obj
    """
    obj = None
    try:
        obj = model.objects.get(**kwargs)
    except model.DoesNotExist:
        if should_raise:
            raise ValidationError(message, code=code)
    return obj


def validate_fields(expected, received, should_raise=True):
    """
    Utility function to validate recieved fields the same as expected
    Raises DRF ValidationError with message of
    :param expected: iter [str...]
    :param received: iter [str...]
    :param should_raise: bool
    :return: bool
    """
    validated = False
    remains = set(expected) - set(received)
    if remains:
        if should_raise:
            raise ValidationError(f"Missing fields: {remains}")
    else:
        validated = True
    return validated
