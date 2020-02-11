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


def get_object(model, message, code=HTTP_404_NOT_FOUND, **kwargs):
    """
    Utility function to retrieve model objects
    Raises DRF ValidationError with message if not found
    :param model: Django model
    :param message: str
    :param code: Http code
    :param kwargs:
    :return: model obj
    """
    try:
        obj = model.objects.get(**kwargs)
    except model.DoesNotExist:
        raise ValidationError(message, code=code)
    return obj
