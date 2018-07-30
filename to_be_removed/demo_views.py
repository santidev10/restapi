from django.shortcuts import get_object_or_404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from to_be_removed.demo_account import DEMO_ACCOUNT_ID
from to_be_removed.demo_account import DemoAccountDeprecated


def show_demo_data(request, pk):
    return not request.user.aw_connections.count() or \
           get_object_or_404(AccountCreation, pk=pk).status == AccountCreation.STATUS_PENDING


class PerformanceAccountDetailsApiViewOLD:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if request.data.get("is_chf") == 1 and pk != DEMO_ACCOUNT_ID:
                return original_method(view, request, pk=pk, **kwargs)
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
                filters = view.get_filters()

                account = DemoAccountDeprecated()
                data = account.header_data
                data['details'] = account.details

                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                data['overview'] = account.overview
                if pk != DEMO_ACCOUNT_ID:
                    original_data = original_method(view, request, pk=pk, **kwargs).data
                    for k in ('id', 'name', 'status', 'thumbnail', 'is_changed'):
                        data[k] = original_data[k]
                return Response(status=HTTP_200_OK, data=data)
            return original_method(view, request, pk=pk, **kwargs)

        return method
