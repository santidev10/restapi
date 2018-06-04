from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.views.performance_account_details import \
    PerformanceAccountDetailsApiView
from aw_creation.models import AccountCreation
from aw_reporting.demo.decorators import demo_view_decorator
from userprofile.models import UserSettingsKey


@demo_view_decorator
class AccountCreationDetailsApiView(RetrieveAPIView):
    def get(self, request, *args, **kwargs):
        pk = kwargs.get("pk")
        show_conversions = self.request.user.aw_settings.get(
            UserSettingsKey.SHOW_CONVERSIONS)
        queryset = AccountCreation.objects.filter(owner=self.request.user)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        data = PerformanceAccountDetailsApiView.get_details_data(item,
                                                                 show_conversions)
        return Response(data=data)
