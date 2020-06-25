from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.models import AccountCreation


class AwCreationCodeRetrieveAPIView(GenericAPIView):
    permission_classes = tuple()

    @staticmethod
    def get(request, account_id, **_):
        try:
            account_management = AccountCreation.objects.get(
                account_id=account_id,
                is_managed=True,
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        with open("aw_creation/scripts/aws_functions.js") as f:
            functions = f.read()
        code = functions + "\n" + account_management.get_aws_code(request)
        return Response(data={"code": code})
