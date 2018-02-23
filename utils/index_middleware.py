from re import sub
from rest_framework.authtoken.models import Token


class IndexMiddleware(object):
    def process_view(self, request, view_func, view_args, view_kwargs):
        header_token = request.META.get('HTTP_AUTHORIZATION', None)
        if header_token is None:
            return

        is_staff = False
        try:
            token = sub('Token', '', request.META.get('HTTP_AUTHORIZATION', None))
            token_obj = Token.objects.get(key=token.strip())
            user = token_obj.user
            is_staff = user.is_staff
        except Token.DoesNotExist:
            pass

        if is_staff:
            from singledb.connector import SingleDatabaseApiConnector
            actual_index = request.META.get("HTTP_CHF_ACTUAL_INDEX")
            SingleDatabaseApiConnector.index_info = True
            SingleDatabaseApiConnector.actual_index = actual_index
