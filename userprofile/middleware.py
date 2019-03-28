from re import sub

from django.conf import settings
from rest_framework.authtoken.models import Token
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from userprofile.utils import is_apex_user
from userprofile.utils import is_correct_apex_domain


class ApexUserCheck:

    def process_request(self, request):
        header_token = request.META.get("HTTP_AUTHORIZATION", None)
        if header_token is None:
            return

        try:
            token = sub("Token", "", request.META.get("HTTP_AUTHORIZATION", None))
            token_obj = Token.objects.get(key=token.strip())
            user = token_obj.user
        except Token.DoesNotExist:
            return

        user_email = user.email
        request_origin = request.META.get("HTTP_ORIGIN") or request.META.get("HTTP_REFERER")

        if not request_origin:
            return

        if is_apex_user(user_email) and not is_correct_apex_domain(request_origin):

            response = Response(
                data={
                    "error": ["Unable to authenticate APEX user"
                              " on this site. Please go to {}".format(settings.APEX_HOST)]
                },
                status=HTTP_400_BAD_REQUEST
            )

            response.accepted_renderer = JSONRenderer()
            response.accepted_media_type = "application/json"
            response.renderer_context = {}
            response.render()

            return response
