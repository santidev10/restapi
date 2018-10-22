from django.utils.encoding import force_text
from django.utils.translation import ugettext_lazy as _
from rest_framework import status
from rest_framework.exceptions import APIException


class PayloadTooLarge(APIException):
    status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
    default_detail = _('Payload is too large {payload_size} bytes ({limit_size} bytes limited).')
    default_code = 'payload_too_large'

    def __init__(self, limit_size, payload_size, detail=None, code=None):
        if detail is None:
            detail = force_text(self.default_detail).format(payload_size=payload_size, limit_size=limit_size)
        super(PayloadTooLarge, self).__init__(detail, code)


class PermissionsError(APIException):
    status_code = status.HTTP_403_FORBIDDEN
